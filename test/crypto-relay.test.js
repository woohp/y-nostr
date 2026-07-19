import assert from "node:assert/strict";
import test from "node:test";

const {
    createEvent,
    createIdentity,
    createPeerTopic,
    createRoomTopic,
    decrypt,
    deriveEncryptionKey,
    encrypt,
    verifyEvent,
} = await import("../dist/index.js");
const { NostrRelay, getEventTopic, selectDefaultRelayUrls } =
    await import("../dist/nostr-relay.js");

const privateKey = (value) => {
    const key = new Uint8Array(32);
    key[31] = value;
    return key;
};

test("selects a stable, room-distributed subset of default relays", () => {
    const selected = selectDefaultRelayUrls("room-a", 5);
    assert.equal(selected.length, 5);
    assert.deepEqual(selected, selectDefaultRelayUrls("room-a", 5));
    assert.notDeepEqual(selected, selectDefaultRelayUrls("room-b", 5));
});

test("creates and verifies canonical Nostr events", async () => {
    const identity = createIdentity(privateKey(1));
    const event = await createEvent("topic", "content", identity, 1234567890);

    assert.match(event.id, /^[0-9a-f]{64}$/);
    assert.match(event.pubkey, /^[0-9a-f]{64}$/);
    assert.match(event.sig, /^[0-9a-f]{128}$/);
    assert.deepEqual(event.tags, [
        ["x", "topic"],
        ["expiration", "1234568010"],
    ]);
    assert.equal(await verifyEvent(event), true);
    assert.equal(await verifyEvent({ ...event, content: "tampered" }), false);
    assert.equal(getEventTopic(event), "topic");
    assert.equal(getEventTopic({ ...event, tags: [...event.tags, ["x", "other"]] }), null);
});

test("derives stable room and peer topics and encrypts signaling", async () => {
    const roomTopic = await createRoomTopic("room", "password");
    assert.equal(roomTopic, await createRoomTopic("room", "password"));
    assert.notEqual(roomTopic, await createRoomTopic("room", "wrong"));
    assert.notEqual(await createRoomTopic("b:c", "a"), await createRoomTopic("c", "a:b"));
    assert.equal((await createPeerTopic(roomTopic, "peer")).length, 64);

    const key = await deriveEncryptionKey("room", "password");
    const encrypted = await encrypt(key, "secret SDP");
    assert.notEqual(encrypted, "secret SDP");
    assert.equal(await decrypt(key, encrypted), "secret SDP");
    await assert.rejects(decrypt(await deriveEncryptionKey("room", "wrong"), encrypted));
});

class MockWebSocket {
    static OPEN = 1;
    static sockets = [];
    readyState = 0;
    listeners = new Map();
    sent = [];

    constructor(url) {
        this.url = url;
        MockWebSocket.sockets.push(this);
    }

    addEventListener(type, listener) {
        const listeners = this.listeners.get(type) ?? [];
        listeners.push(listener);
        this.listeners.set(type, listeners);
    }

    dispatch(type, event = {}) {
        for (const listener of this.listeners.get(type) ?? []) listener(event);
    }

    send(data) {
        this.sent.push(JSON.parse(data));
    }

    close() {
        this.readyState = 3;
        this.dispatch("close");
    }

    open() {
        this.readyState = MockWebSocket.OPEN;
        this.dispatch("open");
    }
}

test("subscribes, verifies relay events, and closes cleanly", async () => {
    MockWebSocket.sockets.length = 0;
    const received = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: (event) => received.push(event),
        WebSocket: MockWebSocket,
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();

    assert.equal(socket.sent[0][0], "REQ");
    assert.deepEqual(socket.sent[0][2]["#x"], ["topic"]);

    const event = await createEvent("topic", "payload", createIdentity(privateKey(2)));
    socket.dispatch("message", {
        data: JSON.stringify(["EVENT", relay.subscriptionId, event]),
    });
    while (received.length === 0) {
        await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.deepEqual(received, [event]);

    socket.dispatch("message", {
        data: JSON.stringify(["EVENT", relay.subscriptionId, { ...event, content: "bad" }]),
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(received.length, 1);

    relay.destroy();
    assert.equal(socket.sent.at(-1)[0], "CLOSE");
    assert.equal(relay.reconnectTimer, undefined);
});
test("ignores malformed tags and events completed after destruction", async () => {
    MockWebSocket.sockets.length = 0;
    const received = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: (event) => received.push(event),
        WebSocket: MockWebSocket,
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();

    socket.dispatch("message", {
        data: JSON.stringify([
            "EVENT",
            relay.subscriptionId,
            {
                id: "x",
                pubkey: "x",
                created_at: 1,
                kind: 20000,
                tags: [null],
                content: "x",
                sig: "x",
            },
        ]),
    });
    const event = await createEvent("topic", "payload", createIdentity(privateKey(3)));
    socket.dispatch("message", {
        data: JSON.stringify(["EVENT", relay.subscriptionId, event]),
    });
    relay.destroy();
    await new Promise((resolve) => setTimeout(resolve, 10));

    assert.deepEqual(received, []);
});
test("retries after a synchronous WebSocket reconnect failure", (context) => {
    class ReconnectWebSocket extends MockWebSocket {
        static attempts = 0;

        constructor(url) {
            ReconnectWebSocket.attempts++;
            if (ReconnectWebSocket.attempts > 1) throw new Error("constructor failed");
            super(url);
        }
    }

    MockWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: () => {},
        onError: (error) => errors.push(error),
        WebSocket: ReconnectWebSocket,
    });
    const socket = MockWebSocket.sockets[0];
    socket.open();
    socket.dispatch("close");

    context.mock.timers.tick(750);

    assert.match(String(errors[0]), /constructor failed/);
    assert.ok(relay.reconnectTimer);
    relay.destroy();
});
test("times out a WebSocket that never opens and schedules reconnect", (context) => {
    MockWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: () => {},
        onError: (error) => errors.push(error),
        connectTimeout: 100,
        WebSocket: MockWebSocket,
    });

    context.mock.timers.tick(100);

    assert.match(String(errors[0]), /connection timed out/);
    assert.equal(relay.socket, null);
    assert.ok(relay.reconnectTimer);
    relay.destroy();
});

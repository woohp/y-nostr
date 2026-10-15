import assert from "node:assert/strict";
import test from "node:test";
import { waitFor } from "../test-support/wait.js";
import { createWebSocketHarness } from "../test-support/websocket.js";

const { createIdentity, createPeerTopic, decrypt, deriveRoomSecrets, encrypt } =
    await import("../dist/crypto.js");
const { createEvent, getEventTopic, verifyEvent } = await import("../dist/nostr-event.js");
const { NostrRelay, selectDefaultRelayUrls } = await import("../dist/nostr-relay.js");

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
    const secrets = await deriveRoomSecrets("room", "password");
    const wrong = await deriveRoomSecrets("room", "wrong");
    assert.equal(secrets.roomTopic, (await deriveRoomSecrets("room", "password")).roomTopic);
    assert.notEqual(secrets.roomTopic, wrong.roomTopic);
    assert.notEqual(
        (await deriveRoomSecrets("b:c", "a")).roomTopic,
        (await deriveRoomSecrets("c", "a:b")).roomTopic,
    );
    assert.equal((await createPeerTopic(secrets.roomTopic, "peer")).length, 64);

    const encrypted = await encrypt(secrets.encryptionKey, "secret SDP");
    assert.notEqual(encrypted, "secret SDP");
    assert.equal(await decrypt(secrets.encryptionKey, encrypted), "secret SDP");
    await assert.rejects(decrypt(wrong.encryptionKey, encrypted));
});

test("subscribes, verifies relay events, and closes cleanly", async (context) => {
    const { WebSocket, sockets } = createWebSocketHarness();
    const received = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: (event) => received.push(event),
        WebSocket,
    });
    context.after(() => relay.destroy());
    const socket = sockets[0];
    socket.open();

    assert.equal(socket.sent[0][0], "REQ");
    assert.deepEqual(socket.sent[0][2]["#x"], ["topic"]);

    const event = await createEvent("topic", "payload", createIdentity(privateKey(2)));
    socket.dispatch("message", {
        data: JSON.stringify(["EVENT", relay.subscriptionId, event]),
    });
    await waitFor(() => received.length > 0);
    assert.deepEqual(received, [event]);

    socket.dispatch("message", {
        data: JSON.stringify(["EVENT", relay.subscriptionId, { ...event, content: "bad" }]),
    });
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(received.length, 1);

    assert.equal(relay.connected, true);
    relay.destroy();
    assert.equal(socket.sent.at(-1)[0], "CLOSE");
    assert.equal(relay.connected, false);
});
test("ignores malformed tags and events completed after destruction", async (context) => {
    const { WebSocket, sockets } = createWebSocketHarness();
    const received = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: (event) => received.push(event),
        WebSocket,
    });
    context.after(() => relay.destroy());
    const socket = sockets[0];
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
    const { WebSocket, sockets } = createWebSocketHarness();
    class ReconnectWebSocket extends WebSocket {
        static attempts = 0;

        constructor(url) {
            ReconnectWebSocket.attempts++;
            if (ReconnectWebSocket.attempts > 1) throw new Error("constructor failed");
            super(url);
        }
    }

    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: () => {},
        onError: (error) => errors.push(error),
        WebSocket: ReconnectWebSocket,
    });
    context.after(() => relay.destroy());
    const socket = sockets[0];
    socket.open();
    socket.dispatch("close");

    context.mock.timers.tick(750);

    assert.match(String(errors[0]), /constructor failed/);
    context.mock.timers.tick(1500);
    assert.equal(ReconnectWebSocket.attempts, 3);
});
test("times out a WebSocket that never opens and schedules reconnect", (context) => {
    const { WebSocket } = createWebSocketHarness();
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const errors = [];
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: () => {},
        onError: (error) => errors.push(error),
        connectTimeout: 100,
        WebSocket,
    });
    context.after(() => relay.destroy());

    context.mock.timers.tick(100);

    assert.match(String(errors[0]), /connection timed out/);
    assert.equal(relay.socket, null);
    assert.ok(relay.reconnectTimer);
});
test("reconnects when the relay subscription send fails", (context) => {
    const { WebSocket, sockets } = createWebSocketHarness();
    class SubscriptionFailureWebSocket extends WebSocket {
        send(data) {
            if (JSON.parse(data)[0] === "REQ") throw new Error("subscription failed");
            super.send(data);
        }
    }

    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let opened = false;
    const relay = new NostrRelay("wss://relay.test", {
        topics: ["topic"],
        onEvent: () => {},
        onOpen: () => {
            opened = true;
        },
        WebSocket: SubscriptionFailureWebSocket,
    });
    context.after(() => relay.destroy());
    const socket = sockets[0];
    socket.readyState = WebSocket.OPEN;

    socket.dispatch("open");

    assert.equal(opened, false);
    assert.ok(relay.reconnectTimer);
});

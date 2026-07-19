import assert from "node:assert/strict";
import test from "node:test";
import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";

class FakeDataChannel {
    binaryType = "blob";
    readyState = "connecting";
    listeners = new Map();

    addEventListener(event, listener) {
        this.listeners.set(event, listener);
    }

    send() {}
    close() {}

    markOpen() {
        this.readyState = "open";
    }

    dispatchOpen() {
        assert.equal(this.readyState, "open");
        this.listeners.get("open")?.();
    }

    receive(data) {
        this.listeners.get("message")?.({ data });
    }
}

class FakePeerConnection {
    connectionState = "connected";
    iceConnectionState = "connected";
    iceGatheringState = "complete";
    localDescription = null;
    remoteDescription = null;
    channel = new FakeDataChannel();

    addEventListener() {}
    createDataChannel() {
        return this.channel;
    }
    async createOffer() {
        return { type: "offer", sdp: "fake-offer" };
    }
    async createAnswer() {
        return { type: "answer", sdp: "fake-answer" };
    }
    async setLocalDescription(description) {
        this.localDescription = { ...description, toJSON: () => ({ ...description }) };
    }
    async setRemoteDescription(description) {
        this.remoteDescription = description;
    }
    close() {}
}

class SelectiveWebSocket {
    static OPEN = 1;
    readyState = 0;

    constructor(url) {
        if (url.includes("invalid")) throw new Error("Invalid relay URL");
    }

    addEventListener() {}
    send() {}
    close() {}
}

class RecoveryWebSocket {
    static OPEN = 1;
    static sockets = [];
    readyState = 0;
    listeners = new Map();
    sent = [];

    constructor(url) {
        this.url = url;
        RecoveryWebSocket.sockets.push(this);
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

    open() {
        this.readyState = RecoveryWebSocket.OPEN;
        this.dispatch("open");
    }

    close() {
        this.readyState = 3;
        this.dispatch("close");
    }
}

class ThrowingPublishWebSocket extends RecoveryWebSocket {
    send(data) {
        if (JSON.parse(data)[0] === "EVENT") throw new Error("publish failed");
        super.send(data);
    }
}

class TransientPublishWebSocket extends RecoveryWebSocket {
    eventAttempts = 0;

    send(data) {
        if (JSON.parse(data)[0] === "EVENT" && ++this.eventAttempts === 1) {
            throw new Error("transient publish failure");
        }
        super.send(data);
    }
}

globalThis.RTCPeerConnection = FakePeerConnection;

const { NostrProvider, createEvent, createIdentity, defaultRtcConfig } =
    await import("../dist/index.js");

const privateKey = (value) => {
    const key = new Uint8Array(32);
    key[31] = value;
    return key;
};

function createProvider(keyValue) {
    return new NostrProvider("test-room", new Y.Doc(), {
        privateKey: privateKey(keyValue),
        relays: [],
    });
}

test("uses public STUN servers by default and allows rtcConfig overrides", async () => {
    const provider = createProvider(1);
    await provider.ready;
    assert.equal(provider.rtcConfig, defaultRtcConfig);
    assert.deepEqual(provider.rtcConfig.iceServers, [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun.cloudflare.com:3478" },
    ]);

    const rtcConfig = { iceServers: [] };
    const overridden = new NostrProvider("test-room", new Y.Doc(), {
        privateKey: privateKey(2),
        relays: [],
        rtcConfig,
    });
    await overridden.ready;
    assert.equal(overridden.rtcConfig, rtcConfig);

    provider.destroy();
    overridden.destroy();
});

test("sendToPeer sends a directed binary message to only the selected peer", async () => {
    const sender = createProvider(3);
    await sender.ready;
    const sent = [];
    sender.peers.set("recipient", {
        connected: true,
        send: (data) => {
            sent.push(data);
            return true;
        },
        destroy: () => {},
    });
    sender.peers.set("other", {
        connected: true,
        send: () => {
            assert.fail("message was sent to an unintended peer");
        },
        destroy: () => {},
    });

    assert.equal(sender.sendToPeer("recipient", new Uint8Array([1, 2, 3])), true);
    assert.equal(sent.length, 1);

    const recipient = createProvider(4);
    await recipient.ready;
    const peer = recipient.createPeer(sender.peerId, true);
    const received = new Promise((resolve) => {
        recipient.on("direct-message", (peerId, payload) => resolve({ peerId, payload }));
    });
    peer.channel.receive(sent[0].buffer);

    assert.deepEqual(await received, {
        peerId: sender.peerId,
        payload: new Uint8Array([1, 2, 3]),
    });

    sender.destroy();
    recipient.destroy();
});

test("only the deterministically selected peer creates an offer", async () => {
    const first = createProvider(6);
    const second = createProvider(7);
    await Promise.all([first.ready, second.ready]);
    const [lower, higher] = [first, second].sort((a, b) => a.peerId.localeCompare(b.peerId));

    const relay = { url: "wss://relay.test", isOpen: () => true, publish: () => true };
    await lower.receiveAnnounce(higher.peerId, relay);
    await higher.receiveAnnounce(lower.peerId, relay);

    assert.equal(lower.pendingPeers.get(higher.peerId)?.role, "offering");
    assert.equal(higher.pendingPeers.has(lower.peerId), false);

    first.destroy();
    second.destroy();
});

test("completes event-driven encrypted offer and answer signaling", async () => {
    const first = createProvider(8);
    const second = createProvider(9);
    await Promise.all([first.ready, second.ready]);
    const [lower, higher] = [first, second].sort((a, b) => a.peerId.localeCompare(b.peerId));
    const participants = [lower, higher];
    const relay = {
        url: "wss://relay.test",
        isOpen: () => true,
        publish: (event) => {
            for (const provider of participants) {
                queueMicrotask(() => void provider.receiveEvent(event, relay));
            }
            return true;
        },
    };

    await lower.announce(relay);
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(lower.pendingPeers.get(higher.peerId)?.role, "offering");
    assert.equal(higher.pendingPeers.get(lower.peerId)?.role, "answering");
    assert.equal(lower.pendingPeers.get(higher.peerId)?.peer.pc.remoteDescription.type, "answer");
    assert.equal(higher.pendingPeers.get(lower.peerId)?.peer.pc.remoteDescription.type, "offer");

    first.destroy();
    second.destroy();
});

test("sendToPeer returns false for missing and disconnected peers", async () => {
    const provider = createProvider(5);
    await provider.ready;
    provider.peers.set("disconnected", {
        connected: false,
        send: () => {},
        destroy: () => {},
    });

    assert.equal(provider.sendToPeer("missing", new Uint8Array()), false);
    assert.equal(provider.sendToPeer("disconnected", new Uint8Array()), false);

    provider.destroy();
});
test("keeps valid relays when another relay constructor throws", async () => {
    const provider = new NostrProvider("test-room", new Y.Doc(), {
        privateKey: privateKey(17),
        relays: ["invalid://relay", "wss://valid.test"],
        WebSocket: SelectiveWebSocket,
    });
    const error = new Promise((resolve) => provider.on("connection-error", resolve));

    await provider.ready;

    assert.match(String((await error).error), /Invalid relay URL/);
    assert.equal(provider.relayConnections.length, 1);
    assert.equal(provider.relayConnections[0].url, "wss://valid.test");
    provider.destroy();
});

test("rejects signaling ciphertext re-signed by another identity or moved to another topic", async () => {
    const sender = createProvider(10);
    const receiver = createProvider(11);
    await Promise.all([sender.ready, receiver.ready]);
    const relay = { url: "wss://relay.test", isOpen: () => true, publish: () => true };
    let announcements = 0;
    receiver.receiveAnnounce = async () => {
        announcements++;
    };

    const original = await sender.createPayloadEvent(sender.roomTopic, {
        v: 2,
        type: "announce",
    });
    await receiver.receiveEvent(original, relay);
    assert.equal(announcements, 1);

    const attacker = createIdentity(privateKey(12));
    const resigned = await createEvent(receiver.roomTopic, original.content, attacker);
    await receiver.receiveEvent(resigned, relay);
    const moved = await createEvent("different-topic", original.content, sender.identity);
    await receiver.receiveEvent(moved, relay);
    const misrouted = await sender.createPayloadEvent(sender.roomTopic, {
        v: 2,
        type: "announce-response",
        to: receiver.peerId,
    });
    await receiver.receiveEvent(misrouted, relay);
    assert.equal(announcements, 1);

    sender.destroy();
    receiver.destroy();
});

test("reports malformed peer frames and destroys the peer", async () => {
    const provider = createProvider(13);
    await provider.ready;
    const peer = provider.createPeer("remote", true);
    const error = new Promise((resolve) => provider.on("peer-error", resolve));

    provider.onPeerMessage(peer, "remote", new ArrayBuffer(0));

    assert.ok((await error) instanceof Error);
    assert.equal(peer.closed, true);
    provider.destroy();
});

test("sets synced only after Sync Step 2 and synchronizes documents", async () => {
    const first = createProvider(14);
    const second = createProvider(15);
    await Promise.all([first.ready, second.ready]);
    first.doc.getText("text").insert(0, "hello");

    const firstPeer = first.createPeer(second.peerId, true);
    const secondPeer = second.createPeer(first.peerId, true);
    firstPeer.channel.send = (data) => secondPeer.channel.receive(data.slice().buffer);
    secondPeer.channel.send = (data) => firstPeer.channel.receive(data.slice().buffer);

    assert.equal(first.synced, false);
    assert.equal(second.synced, false);
    firstPeer.channel.markOpen();
    secondPeer.channel.markOpen();
    firstPeer.channel.dispatchOpen();
    assert.equal(first.synced, true);
    assert.equal(second.synced, false);
    secondPeer.channel.dispatchOpen();
    assert.equal(second.synced, true);
    assert.equal(second.doc.getText("text").toString(), "hello");

    firstPeer.destroy();
    assert.equal(first.synced, false);
    first.destroy();
    second.destroy();
});

test("connect shares initialization and is reusable while destroy is final", async () => {
    const provider = createProvider(16);
    assert.equal(provider.connect(), provider.ready);
    await provider.ready;
    provider.disconnect();
    assert.equal(provider.shouldConnect, false);

    await provider.connect();
    assert.equal(provider.shouldConnect, true);
    provider.destroy();
    await provider.connect();
    assert.equal(provider.shouldConnect, false);
});
test("connect can retry after key derivation fails", async (context) => {
    const originalCrypto = globalThis.crypto;
    context.after(() => {
        Object.defineProperty(globalThis, "crypto", {
            configurable: true,
            value: originalCrypto,
        });
    });
    let rejectImport = true;
    const subtle = new Proxy(originalCrypto.subtle, {
        get(target, property) {
            if (property === "importKey" && rejectImport) {
                return () => {
                    rejectImport = false;
                    return Promise.reject(new Error("KDF failed"));
                };
            }
            const value = Reflect.get(target, property);
            return typeof value === "function" ? value.bind(target) : value;
        },
    });
    Object.defineProperty(globalThis, "crypto", {
        configurable: true,
        value: {
            subtle,
            getRandomValues: originalCrypto.getRandomValues.bind(originalCrypto),
        },
    });

    const provider = createProvider(18);
    await assert.rejects(provider.ready, /KDF failed/);
    Object.defineProperty(globalThis, "crypto", { configurable: true, value: originalCrypto });

    await provider.connect();
    assert.equal(provider.shouldConnect, true);
    provider.destroy();
});

test("announcement recovery is bounded, non-overlapping, and cancellable", async (context) => {
    const provider = createProvider(19);
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let announcements = 0;
    provider.announce = async () => {
        announcements++;
    };

    provider.scheduleAnnouncementRetry();
    provider.scheduleAnnouncementRetry();
    for (const delay of [375, 750, 1500]) {
        context.mock.timers.tick(delay);
        await Promise.resolve();
        await Promise.resolve();
    }
    assert.equal(announcements, 3);
    assert.equal(provider.announcementRetry.attempt, 3);

    provider.scheduleAnnouncementRetry();
    await provider.receiveAnnounce("f".repeat(64), {
        url: "wss://relay.test",
        isOpen: () => true,
        publish: () => true,
    });
    context.mock.timers.tick(375);
    assert.equal(announcements, 3);
    assert.equal(provider.announcementRetry, null);

    provider.pendingPeers.clear();
    provider.destroy();
});
test("recovery resumes when a pending peer disappears during offer creation", async (context) => {
    const provider = createProvider(21);
    context.after(() => provider.destroy());
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let resolveOffer;
    const offer = new Promise((resolve) => {
        resolveOffer = resolve;
    });
    provider.createPeer = () => ({
        createOffer: () => offer,
        destroy: () => {},
    });
    provider.scheduleAnnouncementRetry();

    const peerId = "f".repeat(64);
    const receiving = provider.receiveAnnounce(peerId, {
        url: "wss://relay.test",
        isOpen: () => true,
        publish: () => true,
    });
    await Promise.resolve();
    assert.equal(provider.announcementRetry.paused, true);
    provider.pendingPeers.delete(peerId);
    resolveOffer({ type: "offer", sdp: "late offer" });
    await receiving;

    assert.equal(provider.announcementRetry.paused, false);
    assert.ok(provider.announcementRetry.timer);
});
test("retries a dropped initial announcement after the first relay opens", async (context) => {
    RecoveryWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const provider = new NostrProvider("recovery-room", new Y.Doc(), {
        privateKey: privateKey(22),
        relays: ["wss://relay.test"],
        WebSocket: RecoveryWebSocket,
    });
    context.after(() => provider.destroy());
    await provider.ready;
    const socket = RecoveryWebSocket.sockets[0];

    socket.open();
    const eventCount = () => socket.sent.filter((message) => message[0] === "EVENT").length;
    while (eventCount() < 1) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(eventCount(), 1);

    context.mock.timers.tick(375);
    while (eventCount() < 2) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(eventCount(), 2);
});

test("protocol send failures destroy only the failed peer", async () => {
    const provider = createProvider(23);
    await provider.ready;
    let initialSends = 0;
    let initialDestroyed = 0;
    const initialPeer = {
        send: () => {
            initialSends++;
            return false;
        },
        destroy: () => {
            initialDestroyed++;
            provider.removePeer(initialPeer, "initial-peer");
        },
    };

    provider.onPeerOpen(initialPeer, "initial-peer", true);
    assert.equal(initialSends, 1);
    assert.equal(initialDestroyed, 1);
    assert.equal(provider.peers.has("initial-peer"), false);

    let failedDestroyed = 0;
    let healthySends = 0;
    const failedPeer = {
        connected: true,
        send: () => {
            throw new Error("incremental send failed");
        },
        destroy: () => {
            failedDestroyed++;
            provider.removePeer(failedPeer, "failed-peer");
        },
    };
    const healthyPeer = {
        connected: true,
        send: () => {
            healthySends++;
            return true;
        },
        destroy: () => provider.removePeer(healthyPeer, "healthy-peer"),
    };
    provider.peers.set("failed-peer", failedPeer);
    provider.peers.set("healthy-peer", healthyPeer);

    provider.doc.getText("updates").insert(0, "repair me");

    assert.equal(failedDestroyed, 1);
    assert.equal(provider.peers.has("failed-peer"), false);
    assert.equal(healthySends, 1);
    assert.equal(provider.peers.has("healthy-peer"), true);
    provider.destroy();
});

test("repeated offer failures preserve the global retry budget", async (context) => {
    const provider = createProvider(20);
    context.after(() => provider.destroy());
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let announcements = 0;
    provider.announce = async () => {
        announcements++;
    };
    provider.createPeer = (peerId) => ({
        createOffer: async () => {
            throw new Error("offer failed");
        },
        destroy: () => provider.pendingPeers.delete(peerId),
    });
    const relay = { url: "wss://relay.test", isOpen: () => true, publish: () => true };

    provider.scheduleAnnouncementRetry();
    for (const delay of [375, 750, 1500]) {
        context.mock.timers.tick(delay);
        await Promise.resolve();
        await Promise.resolve();
        await provider.receiveAnnounce("f".repeat(64), relay);
    }
    context.mock.timers.tick(10000);

    assert.equal(announcements, 3);
    assert.equal(provider.announcementRetry.attempt, 3);
    provider.destroy();
});
test("protocol send failure starts bounded peer-loss recovery", async (context) => {
    const provider = createProvider(24);
    context.after(() => provider.destroy());
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let announcements = 0;
    const relay = {
        url: "wss://relay.test",
        isOpen: () => true,
        publish: () => {
            announcements++;
            return true;
        },
        destroy: () => {},
    };
    provider.relayConnections = [relay];
    provider.openRelays.add(relay);
    const peer = {
        connected: true,
        send: () => false,
        destroy: () => provider.removePeer(peer, "failed-peer"),
    };
    provider.peers.set("failed-peer", peer);

    provider.doc.getText("updates").insert(0, "trigger recovery");
    assert.equal(provider.peers.has("failed-peer"), false);

    context.mock.timers.tick(375);
    while (announcements < 1) await new Promise((resolve) => setImmediate(resolve));
    context.mock.timers.tick(750);
    while (announcements < 2) await new Promise((resolve) => setImmediate(resolve));

    assert.equal(announcements, 2);
    assert.equal(provider.announcementRetry.attempt, 2);
});
test("failed pending connection starts recovery before signal timeout", async (context) => {
    const provider = createProvider(25);
    context.after(() => provider.destroy());
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const relay = {
        url: "wss://relay.test",
        isOpen: () => true,
        publish: () => true,
        destroy: () => {},
    };
    provider.relayConnections = [relay];
    provider.openRelays.add(relay);
    const peerId = "pending-peer";
    const peer = {
        acceptAnswer: async () => {
            throw new Error("ICE failed");
        },
        destroy: () => provider.removePeer(peer, peerId),
    };
    provider.pendingPeers.set(peerId, {
        peer,
        relay,
        role: "offering",
        offerId: "offer-id",
        awaitingConnection: true,
    });

    await provider.receiveAnswer(peerId, "offer-id", { type: "answer", sdp: "failed" });

    assert.equal(provider.pendingPeers.has(peerId), false);
    assert.ok(provider.announcementRetry?.timer);
});

test("owned awareness removal is sent before provider teardown", async () => {
    const provider = createProvider(26);
    await provider.ready;
    provider.awareness.setLocalState({ user: "local" });
    const sent = [];
    const peer = {
        connected: true,
        send: (message) => {
            sent.push(message);
            return true;
        },
        destroy: () => {},
    };
    provider.peers.set("presence-peer", peer);

    provider.destroy();

    assert.equal(sent.length, 1);
    assert.equal(provider.awareness.getLocalState(), null);
});

test("passes send safeguards to the WebRTC transport", async () => {
    const provider = new NostrProvider("limits-room", new Y.Doc(), {
        privateKey: privateKey(27),
        relays: [],
        fallbackMaxMessageSize: 123,
        maxBufferedAmount: 456,
    });
    await provider.ready;
    const peer = provider.createPeer("transport-peer", true);

    assert.equal(peer.fallbackMaxMessageSize, 123);
    assert.equal(peer.maxBufferedAmount, 456);
    peer.destroy();
    provider.destroy();
});

test("disconnect broadcasts owned awareness removal and restores local state", async () => {
    const provider = createProvider(29);
    await provider.ready;
    const localState = { user: "local" };
    provider.awareness.setLocalState(localState);
    const sent = [];
    provider.peers.set("presence-peer", {
        connected: true,
        send: (message) => {
            sent.push(message);
            return true;
        },
        destroy: () => {},
    });

    provider.disconnect();

    assert.equal(sent.length, 1);
    assert.deepEqual(provider.awareness.getLocalState(), localState);
    provider.destroy();
});

test("signaling publication failures emit connection-error, not peer-error", async () => {
    const provider = createProvider(30);
    await provider.ready;
    const connectionErrors = [];
    const peerErrors = [];
    provider.on("connection-error", (event) => connectionErrors.push(event));
    provider.on("peer-error", (error) => peerErrors.push(error));

    await provider.receiveAnnounce("f".repeat(64), {
        url: "wss://relay.test",
        isOpen: () => false,
        publish: () => false,
    });

    assert.equal(connectionErrors.length, 1);
    assert.equal(peerErrors.length, 0);
    provider.destroy();
});
test("disconnect and destroy leave caller-owned awareness untouched", async () => {
    const doc = new Y.Doc();
    const awareness = new Awareness(doc);
    const localState = { user: "external" };
    awareness.setLocalState(localState);
    const provider = new NostrProvider("external-awareness", doc, {
        privateKey: privateKey(31),
        relays: [],
        awareness,
    });
    await provider.ready;

    provider.disconnect();
    assert.deepEqual(awareness.getLocalState(), localState);
    provider.destroy();
    assert.deepEqual(awareness.getLocalState(), localState);

    const updatedState = { user: "still usable" };
    awareness.setLocalState(updatedState);
    assert.deepEqual(awareness.getLocalState(), updatedState);
    awareness.destroy();
});
test("relay send exceptions emit one connection error", async (context) => {
    RecoveryWebSocket.sockets.length = 0;
    const provider = new NostrProvider("publish-error", new Y.Doc(), {
        privateKey: privateKey(32),
        relays: ["wss://relay.test"],
        WebSocket: ThrowingPublishWebSocket,
    });
    context.after(() => provider.destroy());
    const errors = [];
    provider.on("connection-error", (event) => errors.push(event));
    await provider.ready;

    RecoveryWebSocket.sockets[0].open();
    while (errors.length === 0) await new Promise((resolve) => setImmediate(resolve));
    assert.equal(errors.length, 1);

    errors.length = 0;
    await provider.announce(provider.relayConnections[0]);
    assert.equal(errors.length, 1);
    provider.destroy();
});
test("transient initial publication failure starts bounded recovery", async (context) => {
    RecoveryWebSocket.sockets.length = 0;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const provider = new NostrProvider("transient-publication", new Y.Doc(), {
        privateKey: privateKey(33),
        relays: ["wss://relay.test"],
        WebSocket: TransientPublishWebSocket,
    });
    context.after(() => provider.destroy());
    await provider.ready;
    const socket = RecoveryWebSocket.sockets[0];

    socket.open();
    while (!provider.announcementRetry?.timer) {
        await new Promise((resolve) => setImmediate(resolve));
    }
    context.mock.timers.tick(375);
    while (socket.eventAttempts < 2) await new Promise((resolve) => setImmediate(resolve));

    assert.equal(socket.readyState, TransientPublishWebSocket.OPEN);
    assert.equal(socket.eventAttempts, 2);
});

test("signaling requires the protocol expiration tag", async () => {
    const sender = createProvider(34);
    const receiver = createProvider(35);
    await Promise.all([sender.ready, receiver.ready]);
    const relay = { url: "wss://relay.test", isOpen: () => true, publish: () => true };
    let announcements = 0;
    receiver.receiveAnnounce = async () => {
        announcements++;
    };
    const event = await sender.createPayloadEvent(sender.roomTopic, {
        v: 2,
        type: "announce",
    });
    const expiration = event.tags.find((tag) => tag[0] === "expiration");
    const withExpiration = (value) =>
        event.tags.map((tag) => (tag[0] === "expiration" ? ["expiration", value] : tag));
    const invalidTags = [
        event.tags.filter((tag) => tag[0] !== "expiration"),
        [...event.tags, expiration],
        withExpiration("invalid"),
        withExpiration(String(event.created_at + 121)),
        withExpiration(`0${event.created_at + 120}`),
        withExpiration(`+${event.created_at + 120}`),
        withExpiration("9007199254740992"),
    ];

    for (const tags of invalidTags) await receiver.receiveEvent({ ...event, tags }, relay);
    assert.equal(announcements, 0);
    await receiver.receiveEvent(event, relay);
    assert.equal(announcements, 1);
    sender.destroy();
    receiver.destroy();
});

test("relay event callback reports asynchronous failures", async (context) => {
    RecoveryWebSocket.sockets.length = 0;
    const provider = new NostrProvider("incoming-failure", new Y.Doc(), {
        privateKey: privateKey(36),
        relays: ["wss://relay.test"],
        WebSocket: RecoveryWebSocket,
    });
    context.after(() => provider.destroy());
    await provider.ready;
    const errors = [];
    provider.on("connection-error", (event) => errors.push(event));
    provider.receiveEvent = async () => {
        throw new Error("incoming failure");
    };

    provider.relayConnections[0].onEvent({}, provider.relayConnections[0]);
    while (errors.length === 0) await new Promise((resolve) => setImmediate(resolve));

    assert.match(String(errors[0].error), /incoming failure/);
    assert.equal(errors[0].relay, "wss://relay.test");
});

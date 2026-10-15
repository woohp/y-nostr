import assert from "node:assert/strict";
import test from "node:test";
import * as Y from "yjs";
import { Awareness } from "y-protocols/awareness";
import * as syncProtocol from "y-protocols/sync";
import * as encoding from "lib0/encoding";
import { waitFor } from "../test-support/wait.js";
import { createWebSocketHarness } from "../test-support/websocket.js";

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

    constructor(config) {
        this.config = config;
    }

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

globalThis.RTCPeerConnection = FakePeerConnection;

const { NostrProvider, defaultRtcConfig } = await import("../dist/index.js");
const { createIdentity } = await import("../dist/crypto.js");
const { createEvent } = await import("../dist/nostr-event.js");

const privateKey = (value) => {
    const key = new Uint8Array(32);
    key[31] = value;
    return key;
};

// These providers have no relays, so walk the scheduler down the path a real relay takes:
// report one open, then start the request that queued while nothing could carry it.
const armRetry = (provider) => {
    provider.announcements.relayOpened({ url: "wss://relay.test" });
    provider.announcements.startQueuedRequest();
};

const fakeRelay = (publish = () => true) => ({
    url: "wss://relay.test",
    isOpen: () => true,
    publish,
    destroy: () => {},
});

function createProvider(keyValue) {
    return new NostrProvider("test-room", new Y.Doc(), {
        privateKey: privateKey(keyValue),
        relays: [],
    });
}

test("rejects self-signaling before peer negotiation", async (context) => {
    const provider = createProvider(40);
    context.after(() => provider.destroy());
    await provider.ready;
    let publications = 0;
    const relay = fakeRelay(() => {
        publications++;
        return true;
    });

    await provider.receiveAnnounce(provider.peerId, relay);
    await provider.receiveOffer(
        provider.peerId,
        "self-offer",
        { type: "offer", sdp: "self" },
        relay,
    );

    assert.equal(provider.pendingPeers.size, 0);
    assert.equal(publications, 0);
});

test("sendToPeer sends a directed binary message to only the selected peer", async (context) => {
    const sender = createProvider(3);
    context.after(() => sender.destroy());
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
    context.after(() => recipient.destroy());
    await recipient.ready;
    const peer = recipient.createPeer(sender.peerId, true);
    peer.channel.markOpen();
    recipient.peers.set(sender.peerId, peer);
    const received = new Promise((resolve) => {
        recipient.on("direct-message", (peerId, payload) => resolve({ peerId, payload }));
    });
    peer.channel.receive(sent[0].buffer);

    assert.deepEqual(await received, {
        peerId: sender.peerId,
        payload: new Uint8Array([1, 2, 3]),
    });
});

test("only the deterministically selected peer creates an offer", async (context) => {
    const first = createProvider(6);
    const second = createProvider(7);
    context.after(() => {
        first.destroy();
        second.destroy();
    });
    await Promise.all([first.ready, second.ready]);
    const [lower, higher] = [first, second].sort((a, b) => a.peerId.localeCompare(b.peerId));

    const relay = fakeRelay();
    await lower.receiveAnnounce(higher.peerId, relay);
    await higher.receiveAnnounce(lower.peerId, relay);

    assert.equal(lower.pendingPeers.get(higher.peerId)?.role, "offering");
    assert.equal(higher.pendingPeers.has(lower.peerId), false);
});

test("completes event-driven encrypted offer and answer signaling", async (context) => {
    const first = createProvider(8);
    const second = createProvider(9);
    context.after(() => {
        first.destroy();
        second.destroy();
    });
    await Promise.all([first.ready, second.ready]);
    const [lower, higher] = [first, second].sort((a, b) => a.peerId.localeCompare(b.peerId));
    const participants = [lower, higher];
    const relay = fakeRelay((event) => {
        for (const provider of participants) {
            queueMicrotask(() => void provider.receiveEvent(event, relay));
        }
        return true;
    });

    await lower.announce(relay);
    await new Promise((resolve) => setTimeout(resolve, 50));

    assert.equal(lower.pendingPeers.get(higher.peerId)?.role, "offering");
    assert.equal(higher.pendingPeers.get(lower.peerId)?.role, "answering");
    assert.equal(lower.pendingPeers.get(higher.peerId)?.peer.pc.remoteDescription.type, "answer");
    assert.equal(higher.pendingPeers.get(lower.peerId)?.peer.pc.remoteDescription.type, "offer");
});

test("sendToPeer returns false for missing and disconnected peers", async (context) => {
    const provider = createProvider(5);
    context.after(() => provider.destroy());
    await provider.ready;
    provider.peers.set("disconnected", {
        connected: false,
        send: () => {},
        destroy: () => {},
    });

    assert.equal(provider.sendToPeer("missing", new Uint8Array()), false);
    assert.equal(provider.sendToPeer("disconnected", new Uint8Array()), false);
});
test("keeps valid relays when another relay constructor throws", async (context) => {
    const { WebSocket } = createWebSocketHarness();
    const provider = new NostrProvider("test-room", new Y.Doc(), {
        privateKey: privateKey(17),
        relays: ["invalid://relay", "wss://valid.test"],
        WebSocket,
    });
    context.after(() => provider.destroy());
    const error = new Promise((resolve) => provider.on("connection-error", resolve));

    await provider.ready;

    assert.match(String((await error).error), /Invalid relay URL/);
    assert.equal(provider.relayConnections.length, 1);
    assert.equal(provider.relayConnections[0].url, "wss://valid.test");
});

test("rejects signaling ciphertext re-signed by another identity or moved to another topic", async (context) => {
    const sender = createProvider(10);
    const receiver = createProvider(11);
    context.after(() => {
        sender.destroy();
        receiver.destroy();
    });
    await Promise.all([sender.ready, receiver.ready]);
    const relay = fakeRelay();
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
});

test("reports malformed peer frames and destroys the peer", async (context) => {
    const provider = createProvider(13);
    context.after(() => provider.destroy());
    await provider.ready;
    const peer = provider.createPeer("remote", true);
    peer.channel.markOpen();
    provider.peers.set("remote", peer);
    const error = new Promise((resolve) => provider.on("peer-error", resolve));

    provider.onPeerMessage(peer, "remote", new ArrayBuffer(0));

    assert.ok((await error) instanceof Error);
    assert.equal(peer.closed, true);
});

test("sets synced only after Sync Step 2 and synchronizes documents", async (context) => {
    const first = createProvider(14);
    const second = createProvider(15);
    context.after(() => {
        first.destroy();
        second.destroy();
    });
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
    first.peers.set(second.peerId, firstPeer);
    second.peers.set(first.peerId, secondPeer);
    firstPeer.channel.dispatchOpen();
    assert.equal(first.synced, true);
    assert.equal(second.synced, false);
    secondPeer.channel.dispatchOpen();
    assert.equal(second.synced, true);
    assert.equal(second.doc.getText("text").toString(), "hello");

    firstPeer.destroy();
    assert.equal(first.synced, false);
});

test("connect shares initialization and is reusable while destroy is final", async (context) => {
    const provider = createProvider(16);
    context.after(() => provider.destroy());
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
    context.mock.method(crypto.subtle, "importKey").mock.mockImplementationOnce(async () => {
        throw new Error("KDF failed");
    });

    const provider = createProvider(18);
    context.after(() => provider.destroy());
    await assert.rejects(provider.ready, /KDF failed/);

    await provider.connect();
    assert.equal(provider.shouldConnect, true);
});

test("a published offer stops announcement retries", async (context) => {
    const provider = createProvider(19);
    context.after(() => provider.destroy());
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let announcements = 0;
    provider.announce = async () => {
        announcements++;
    };

    armRetry(provider);
    await provider.receiveAnnounce("f".repeat(64), fakeRelay());
    assert.equal(provider.pendingPeers.get("f".repeat(64))?.signalPublished, true);

    context.mock.timers.tick(10_000);
    assert.equal(announcements, 0);
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
    let announcements = 0;
    provider.announce = async () => {
        announcements++;
    };
    armRetry(provider);

    const peerId = "f".repeat(64);
    const receiving = provider.receiveAnnounce(peerId, fakeRelay());
    context.mock.timers.tick(375);
    assert.equal(announcements, 0, "the handshake pauses retries");
    provider.pendingPeers.delete(peerId);
    resolveOffer({ type: "offer", sdp: "late offer" });
    await receiving;

    context.mock.timers.tick(375);
    assert.equal(announcements, 1);
});
test("retries a dropped initial announcement after the first relay opens", async (context) => {
    const { WebSocket, sockets } = createWebSocketHarness();
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const provider = new NostrProvider("recovery-room", new Y.Doc(), {
        privateKey: privateKey(22),
        relays: ["wss://relay.test"],
        WebSocket,
    });
    context.after(() => provider.destroy());
    await provider.ready;
    const socket = sockets[0];

    socket.open();
    const eventCount = () => socket.sent.filter((message) => message[0] === "EVENT").length;
    await waitFor(() => eventCount() >= 1);
    assert.equal(eventCount(), 1);

    context.mock.timers.tick(375);
    await waitFor(() => eventCount() >= 2);
    assert.equal(eventCount(), 2);
});

test("protocol send failures destroy only the failed peer", async (context) => {
    const provider = createProvider(23);
    context.after(() => provider.destroy());
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
    const relay = fakeRelay();

    armRetry(provider);
    for (const delay of [375, 750, 1500]) {
        context.mock.timers.tick(delay);
        await Promise.resolve();
        await Promise.resolve();
        await provider.receiveAnnounce("f".repeat(64), relay);
    }
    context.mock.timers.tick(10000);

    assert.equal(announcements, 3);
});
test("protocol send failure starts peer-loss recovery", async (context) => {
    const provider = createProvider(24);
    context.after(() => provider.destroy());
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let announcements = 0;
    const relay = fakeRelay(() => {
        announcements++;
        return true;
    });
    provider.relayConnections = [relay];
    provider.announcements.relayOpened(relay);
    const peer = {
        connected: true,
        send: () => false,
        destroy: () => provider.removePeer(peer, "failed-peer"),
    };
    provider.peers.set("failed-peer", peer);

    provider.doc.getText("updates").insert(0, "trigger recovery");
    assert.equal(provider.peers.has("failed-peer"), false);

    context.mock.timers.tick(375);
    await waitFor(() => announcements >= 1);
    assert.equal(announcements, 1);
});
test("failed pending connection starts recovery before signal timeout", async (context) => {
    const provider = createProvider(25);
    context.after(() => provider.destroy());
    await provider.ready;
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    let announcements = 0;
    provider.announce = async () => {
        announcements++;
    };
    const relay = fakeRelay();
    provider.announcements.relayOpened(relay);
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
        signalPublished: true,
    });

    await provider.receiveAnswer(peerId, "offer-id", { type: "answer", sdp: "failed" });

    assert.equal(provider.pendingPeers.has(peerId), false);
    context.mock.timers.tick(375);
    assert.equal(announcements, 1);
});

test("owned awareness removal is sent before provider teardown", async (context) => {
    const provider = createProvider(26);
    context.after(() => provider.destroy());
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

test("passes transport options to the WebRTC peer", async (context) => {
    const defaults = createProvider(28);
    context.after(() => defaults.destroy());
    await defaults.ready;
    const defaultPeer = defaults.createPeer("transport-peer", true);
    context.after(() => defaultPeer.destroy());
    assert.equal(defaultPeer.pc.config, defaultRtcConfig);

    const rtcConfig = { iceServers: [] };
    const provider = new NostrProvider("limits-room", new Y.Doc(), {
        privateKey: privateKey(27),
        relays: [],
        rtcConfig,
        fallbackMaxMessageSize: 123,
        maxBufferedAmount: 456,
    });
    context.after(() => provider.destroy());
    await provider.ready;
    const peer = provider.createPeer("transport-peer", true);
    context.after(() => peer.destroy());

    assert.equal(peer.pc.config, rtcConfig);
    assert.equal(peer.fallbackMaxMessageSize, 123);
    assert.equal(peer.maxBufferedAmount, 456);
});

test("disconnect broadcasts owned awareness removal and restores local state", async (context) => {
    const provider = createProvider(29);
    context.after(() => provider.destroy());
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
});

test("signaling publication failures emit connection-error, not peer-error", async (context) => {
    const provider = createProvider(30);
    context.after(() => provider.destroy());
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
});
test("disconnect and destroy leave caller-owned awareness untouched", async (context) => {
    const doc = new Y.Doc();
    const awareness = new Awareness(doc);
    context.after(() => awareness.destroy());
    const localState = { user: "external" };
    awareness.setLocalState(localState);
    const provider = new NostrProvider("external-awareness", doc, {
        privateKey: privateKey(31),
        relays: [],
        awareness,
    });
    context.after(() => provider.destroy());
    await provider.ready;

    provider.disconnect();
    assert.deepEqual(awareness.getLocalState(), localState);
    provider.destroy();
    assert.deepEqual(awareness.getLocalState(), localState);

    const updatedState = { user: "still usable" };
    awareness.setLocalState(updatedState);
    assert.deepEqual(awareness.getLocalState(), updatedState);
});
test("relay send exceptions emit one connection error", async (context) => {
    const { WebSocket, sockets } = createWebSocketHarness();
    class ThrowingPublishWebSocket extends WebSocket {
        send(data) {
            if (JSON.parse(data)[0] === "EVENT") throw new Error("publish failed");
            super.send(data);
        }
    }
    const provider = new NostrProvider("publish-error", new Y.Doc(), {
        privateKey: privateKey(32),
        relays: ["wss://relay.test"],
        WebSocket: ThrowingPublishWebSocket,
    });
    context.after(() => provider.destroy());
    const errors = [];
    provider.on("connection-error", (event) => errors.push(event));
    await provider.ready;

    sockets[0].open();
    await waitFor(() => errors.length > 0);
    assert.equal(errors.length, 1);

    errors.length = 0;
    await provider.announce(provider.relayConnections[0]);
    assert.equal(errors.length, 1);
});
test("transient initial publication failure starts bounded recovery", async (context) => {
    const { WebSocket, sockets } = createWebSocketHarness();
    class TransientPublishWebSocket extends WebSocket {
        eventAttempts = 0;

        send(data) {
            if (JSON.parse(data)[0] === "EVENT" && ++this.eventAttempts === 1) {
                throw new Error("transient publish failure");
            }
            super.send(data);
        }
    }
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const provider = new NostrProvider("transient-publication", new Y.Doc(), {
        privateKey: privateKey(33),
        relays: ["wss://relay.test"],
        WebSocket: TransientPublishWebSocket,
    });
    context.after(() => provider.destroy());
    await provider.ready;
    const socket = sockets[0];

    socket.open();
    await waitFor(() => socket.eventAttempts >= 1);
    await new Promise((resolve) => setImmediate(resolve));
    context.mock.timers.tick(375);
    await waitFor(() => socket.eventAttempts >= 2);

    assert.equal(socket.readyState, TransientPublishWebSocket.OPEN);
    assert.equal(socket.eventAttempts, 2);
});

test("signaling requires the protocol expiration tag", async (context) => {
    const sender = createProvider(34);
    const receiver = createProvider(35);
    context.after(() => {
        sender.destroy();
        receiver.destroy();
    });
    await Promise.all([sender.ready, receiver.ready]);
    const relay = fakeRelay();
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
});

test("relay event callback reports asynchronous failures", async (context) => {
    const { WebSocket } = createWebSocketHarness();
    const provider = new NostrProvider("incoming-failure", new Y.Doc(), {
        privateKey: privateKey(36),
        relays: ["wss://relay.test"],
        WebSocket,
    });
    context.after(() => provider.destroy());
    await provider.ready;
    const errors = [];
    provider.on("connection-error", (event) => errors.push(event));
    provider.receiveEvent = async () => {
        throw new Error("incoming failure");
    };

    const [relay] = provider.relayConnections;
    relay.opts.onEvent({}, relay);
    await waitFor(() => errors.length > 0);

    assert.match(String(errors[0].error), /incoming failure/);
    assert.equal(errors[0].relay, "wss://relay.test");
});

test("disconnect cancels active ICE negotiation without peer errors", async (context) => {
    class GatheringPeerConnection extends FakePeerConnection {
        iceGatheringState = "gathering";
    }
    const relay = fakeRelay();
    const cases = [
        (provider) => provider.receiveAnnounce("f".repeat(64), relay),
        (provider) =>
            provider.receiveOffer(
                "0".repeat(64),
                "incoming-offer",
                { type: "offer", sdp: "incoming" },
                relay,
            ),
    ];

    for (const [index, startNegotiation] of cases.entries()) {
        const provider = new NostrProvider(`cancel-ice-${index}`, new Y.Doc(), {
            privateKey: privateKey(38 + index),
            relays: [],
            iceGatheringTimeout: 1000,
            RTCPeerConnection: GatheringPeerConnection,
        });
        context.after(() => provider.destroy());
        await provider.ready;
        const errors = [];
        provider.on("peer-error", (error) => errors.push(error));
        const negotiating = startNegotiation(provider);
        await waitFor(() => provider.pendingPeers.size > 0);

        provider.disconnect();
        await negotiating;

        assert.deepEqual(errors, []);
        assert.equal(provider.pendingPeers.size, 0);
    }
});
test("late Sync Step 2 from a removed peer cannot restore synced", async (context) => {
    const provider = createProvider(37);
    context.after(() => provider.destroy());
    await provider.ready;
    const peerId = "removed-peer";
    const peer = { connected: true, send: () => true, destroy: () => {} };
    provider.peers.set(peerId, peer);
    provider.removePeer(peer, peerId);
    const encoder = encoding.createEncoder();
    encoding.writeVarUint(encoder, 0);
    syncProtocol.writeSyncStep2(encoder, provider.doc);

    provider.onPeerMessage(peer, peerId, encoding.toUint8Array(encoder).buffer);

    assert.equal(provider.synced, false);
    assert.equal(provider.syncedPeers.size, 0);
});

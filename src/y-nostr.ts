import type * as Y from "yjs";
import * as syncProtocol from "y-protocols/sync";
import * as awarenessProtocol from "y-protocols/awareness";
import type { Awareness } from "y-protocols/awareness";
import * as encoding from "lib0/encoding";
import * as decoding from "lib0/decoding";
import { ObservableV2 } from "lib0/observable";
import {
    createEvent,
    createIdentity,
    createPeerTopic,
    decrypt,
    deriveRoomSecrets,
    encrypt,
    signalExpirationSeconds,
    type NostrEvent,
    type NostrIdentity,
} from "./crypto.js";
import {
    NostrRelay,
    defaultRelayUrls,
    getEventTopic,
    selectDefaultRelayUrls,
} from "./nostr-relay.js";
import { WebrtcDataPeer } from "webrtc-data-peer";

export { defaultRelayUrls };

export const defaultRtcConfig: RTCConfiguration = {
    iceServers: [
        { urls: "stun:stun.l.google.com:19302" },
        { urls: "stun:stun.cloudflare.com:3478" },
    ],
};

const messageSync = 0;
const messageAwareness = 1;
const messageQueryAwareness = 3;
const messageDirect = 4;
const protocolVersion = 2;
const seenEventLimit = 1000;

type PeerId = string;
type DocUpdateHandler = (update: Uint8Array, origin: unknown) => void;
type AwarenessUpdate = { added: number[]; updated: number[]; removed: number[] };
type AwarenessUpdateHandler = (update: AwarenessUpdate, origin: unknown) => void;
type PeerRole = "offering" | "answering";
type NegotiationRole = PeerRole | "self";
type PeerMessageResult = "handled" | "sync-step-2";
interface SignalBase {
    v: number;
    from: PeerId;
    topic: string;
}

interface AnnouncePayload extends SignalBase {
    type: "announce";
}

interface DirectedPayloadBase extends SignalBase {
    to: PeerId;
}

interface AnnounceResponsePayload extends DirectedPayloadBase {
    type: "announce-response";
}

interface SessionSignalPayload extends DirectedPayloadBase {
    type: "offer" | "answer";
    offerId: string;
    signal: RTCSessionDescriptionInit;
}

type SignalPayload = AnnouncePayload | AnnounceResponsePayload | SessionSignalPayload;
type OutgoingSignalPayload =
    | Omit<AnnouncePayload, "from" | "topic">
    | Omit<AnnounceResponsePayload, "from" | "topic">
    | Omit<SessionSignalPayload, "from" | "topic">;

interface PendingPeer {
    peer: WebrtcDataPeer;
    relay: NostrRelay;
    role: PeerRole;
    offerId: string;
    signalPublished: boolean;
    timer?: ReturnType<typeof setTimeout>;
}

export interface NostrProviderOptions {
    relays?: readonly string[];
    relayRedundancy?: number;
    password?: string;
    awareness?: Awareness;
    maxConns?: number;
    offerTimeout?: number;
    signalTimeout?: number;
    relayConnectTimeout?: number;
    fallbackMaxMessageSize?: number;
    maxBufferedAmount?: number;
    rtcConfig?: RTCConfiguration | undefined;
    channelName?: string;
    privateKey?: Uint8Array;
    WebSocket?: typeof WebSocket;
    RTCPeerConnection?: typeof RTCPeerConnection;
    debug?: boolean;
}

export interface DebugEvent {
    type: string;
    [key: string]: unknown;
}

export interface NostrProviderEvents {
    status: (event: { status: "connected" | "disconnected"; relay: string }) => void;
    peers: (peerIds: readonly string[]) => void;
    synced: (synced: boolean) => void;
    "connection-error": (event: { error: unknown; relay?: string }) => void;
    "peer-error": (error: unknown) => void;
    "direct-message": (peerId: string, message: Uint8Array) => void;
    debug: (event: DebugEvent) => void;
}

export class NostrProvider extends ObservableV2<NostrProviderEvents> {
    readonly roomName: string;
    readonly doc: Y.Doc;
    private readonly relays: readonly string[];
    private readonly password: string;
    private readonly maxConns: number;
    private readonly offerTimeout: number;
    private readonly signalTimeout: number;
    private readonly relayConnectTimeout: number;
    private readonly fallbackMaxMessageSize: number;
    private readonly maxBufferedAmount: number;
    private readonly rtcConfig: RTCConfiguration;
    private readonly channelName: string;
    private readonly identity: NostrIdentity;
    readonly peerId: PeerId;
    private readonly debug: boolean;
    readonly awareness: Awareness;
    readonly ready: Promise<void>;
    private shouldConnect = true;
    private _synced = false;
    private roomTopic: string | null = null;
    private peerTopic: string | null = null;
    private relayConnections: NostrRelay[] = [];
    private readonly peers: Map<PeerId, WebrtcDataPeer> = new Map();
    private readonly pendingPeers: Map<PeerId, PendingPeer> = new Map();
    private encryptionKey: CryptoKey | null = null;
    private readonly _WebSocket: typeof WebSocket | undefined;
    private readonly _RTCPeerConnection: typeof RTCPeerConnection | undefined;
    private readonly _ownsAwareness: boolean;
    private readonly _docUpdateHandler: DocUpdateHandler;
    private readonly _awarenessUpdateHandler: AwarenessUpdateHandler;
    private readonly seenEvents = new Set<string>();
    private readonly seenEventQueue: string[] = [];
    private readonly syncedPeers = new Set<WebrtcDataPeer>();
    private readonly openRelays = new Set<NostrRelay>();
    private recoveryStartPending = false;
    private announcementRetry: {
        attempt: number;
        generation: number;
        paused: boolean;
        timer?: ReturnType<typeof setTimeout>;
    } | null = null;
    private connectionGeneration = 0;
    private connectionActive = false;
    private connectionPromise: Promise<void> | null = null;
    private destroyed = false;

    constructor(roomName: string, doc: Y.Doc, opts: NostrProviderOptions = {}) {
        super();
        this.roomName = roomName;
        this.doc = doc;
        this.relays = opts.relays ?? selectDefaultRelayUrls(roomName, opts.relayRedundancy);
        this.password = opts.password ?? "";
        this.maxConns = opts.maxConns ?? 20;
        this.offerTimeout = opts.offerTimeout ?? 5000;
        this.signalTimeout = opts.signalTimeout ?? 15000;
        this.relayConnectTimeout = opts.relayConnectTimeout ?? 10000;
        this.fallbackMaxMessageSize = opts.fallbackMaxMessageSize ?? 256 * 1024;
        this.maxBufferedAmount = opts.maxBufferedAmount ?? 4 * 1024 * 1024;
        this.rtcConfig = opts.rtcConfig ?? defaultRtcConfig;
        this.channelName = opts.channelName ?? "y-nostr";
        this.identity = createIdentity(opts.privateKey);
        this.peerId = this.identity.publicKey;
        this.debug = !!opts.debug;
        this._WebSocket = opts.WebSocket;
        this._RTCPeerConnection = opts.RTCPeerConnection;
        this._ownsAwareness = !opts.awareness;
        this.awareness = opts.awareness ?? new awarenessProtocol.Awareness(doc);

        this._docUpdateHandler = (update, origin) => {
            if (origin !== this) this.broadcastSyncUpdate(update);
        };
        this._awarenessUpdateHandler = ({ added, updated, removed }) => {
            const changedClients = added.concat(updated, removed);
            const update = awarenessProtocol.encodeAwarenessUpdate(this.awareness, changedClients);
            this.broadcastAwareness(update);
        };
        this.doc.on("update", this._docUpdateHandler);
        this.awareness.on("update", this._awarenessUpdateHandler);

        this.ready = this.connect();
    }

    get synced(): boolean {
        return this._synced;
    }

    get peerIds(): readonly PeerId[] {
        return Array.from(this.peers.keys());
    }

    sendToPeer(peerId: PeerId, message: Uint8Array<ArrayBuffer>): boolean {
        const peer = this.peers.get(peerId);
        if (!peer?.connected) return false;
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageDirect);
        encoding.writeVarUint8Array(encoder, message);
        const encoded = encoding.toUint8Array(encoder);
        try {
            return peer.send(encoded);
        } catch {
            return false;
        }
    }

    connect(): Promise<void> {
        if (this.destroyed) return Promise.resolve();
        if (this.connectionPromise) return this.connectionPromise;
        if (this.connectionActive) return Promise.resolve();

        this.shouldConnect = true;
        this.connectionActive = true;
        const generation = ++this.connectionGeneration;
        const attempt = this.initializeConnection(generation);
        const tracked = attempt
            .catch((error: unknown) => {
                if (generation === this.connectionGeneration) this.connectionActive = false;
                throw error;
            })
            .finally(() => {
                if (this.connectionPromise === tracked) this.connectionPromise = null;
            });
        this.connectionPromise = tracked;
        return tracked;
    }

    private async initializeConnection(generation: number): Promise<void> {
        const { roomTopic, encryptionKey } = await deriveRoomSecrets(this.roomName, this.password);
        const peerTopic = await createPeerTopic(roomTopic, this.peerId);
        if (!this.isActive() || !this.isCurrentGeneration(generation)) return;
        this.roomTopic = roomTopic;
        this.peerTopic = peerTopic;
        this.encryptionKey = encryptionKey;

        const initialAnnouncement = this.createPayloadEvent(roomTopic, {
            v: protocolVersion,
            type: "announce",
        });
        void initialAnnouncement.catch((error: unknown) =>
            this.emit("connection-error", [{ error }]),
        );

        for (const url of this.relays) {
            if (!this.isActive() || !this.isCurrentGeneration(generation)) break;
            try {
                const relay = new NostrRelay(url, {
                    topics: [roomTopic, peerTopic],
                    onEvent: (event, relay) => {
                        void this.receiveEvent(event, relay, generation).catch((error: unknown) => {
                            if (this.isActive() && this.isCurrentGeneration(generation)) {
                                this.emit("connection-error", [{ error, relay: relay.url }]);
                            }
                        });
                    },
                    onOpen: (relay, reconnected) => {
                        if (!this.isActive() || !this.isCurrentGeneration(generation)) return;
                        const publication = reconnected
                            ? this.announce(relay)
                            : initialAnnouncement.then((event) => {
                                  if (!this.isActive() || !this.isCurrentGeneration(generation)) {
                                      return false;
                                  }
                                  if (Math.floor(Date.now() / 1000) - event.created_at > 90) {
                                      return this.announce(relay);
                                  }
                                  return relay.publish(event);
                              });
                        void publication
                            .catch((error: unknown) => {
                                if (reconnected) {
                                    this.emit("connection-error", [{ error, relay: relay.url }]);
                                }
                            })
                            .finally(() => {
                                if (
                                    this.isActive() &&
                                    this.recoveryStartPending &&
                                    this.openRelays.size > 0 &&
                                    this.isCurrentGeneration(generation)
                                ) {
                                    this.recoveryStartPending = false;
                                    this.startAnnouncementRecoveryEpisode();
                                }
                            });
                    },
                    onStatus: (status, relay) => {
                        if (!this.isActive() || !this.isCurrentGeneration(generation)) return;
                        if (status === "connected") {
                            if (this.openRelays.size === 0) this.recoveryStartPending = true;
                            this.openRelays.add(relay);
                        } else {
                            this.openRelays.delete(relay);
                            if (this.openRelays.size === 0) {
                                this.pauseAnnouncementRetry();
                                this.recoveryStartPending = true;
                            }
                        }
                        this.emit("status", [{ status, relay: relay.url }]);
                    },
                    onError: (error, relay) => {
                        if (this.isActive() && this.isCurrentGeneration(generation)) {
                            this.emit("connection-error", [{ error, relay: relay.url }]);
                        }
                    },
                    connectTimeout: this.relayConnectTimeout,
                    ...(this._WebSocket ? { WebSocket: this._WebSocket } : {}),
                });
                this.relayConnections.push(relay);
            } catch (error) {
                this.emit("connection-error", [{ error, relay: url }]);
            }
        }
    }

    private async announce(relay?: NostrRelay): Promise<boolean> {
        if (!this.isActive() || !this.roomTopic) return false;
        return this.publishPayload(
            this.roomTopic,
            { v: protocolVersion, type: "announce" },
            relay ? [relay] : this.relayConnections,
        );
    }

    private async receiveEvent(
        event: NostrEvent,
        relay: NostrRelay,
        generation: number = this.connectionGeneration,
    ): Promise<void> {
        if (!this.isActive() || !this.isCurrentGeneration(generation)) return;
        if (event.pubkey === this.peerId || this.hasSeenEvent(event.id)) return;
        if (Math.abs(Math.floor(Date.now() / 1000) - event.created_at) > 120) return;
        if (!hasValidSignalExpiration(event)) return;

        const eventTopic = getEventTopic(event);
        if (!eventTopic) return;
        this.rememberEvent(event.id);
        let payload: SignalPayload;
        try {
            if (!this.encryptionKey) return;
            payload = JSON.parse(await decrypt(this.encryptionKey, event.content)) as SignalPayload;
        } catch {
            this.emitDebug({ type: "signal-decrypt-failed", relay: relay.url });
            return;
        }
        if (!this.isActive() || !this.isCurrentGeneration(generation)) return;
        if (!isSignalPayload(payload) || payload.v !== protocolVersion) return;
        if (payload.from !== event.pubkey || payload.topic !== eventTopic) return;
        if (!this.hasValidRouting(payload, eventTopic)) return;

        this.emitDebug({ type: "signal-received", signalType: payload.type, peerId: event.pubkey });
        if (payload.type === "announce" || payload.type === "announce-response") {
            await this.receiveAnnounce(event.pubkey, relay, payload.type === "announce-response");
        } else if (payload.type === "offer") {
            await this.receiveOffer(event.pubkey, payload.offerId, payload.signal, relay);
        } else {
            await this.receiveAnswer(event.pubkey, payload.offerId, payload.signal);
        }
    }

    private async receiveAnnounce(
        peerId: PeerId,
        relay: NostrRelay,
        isResponse: boolean = false,
    ): Promise<void> {
        if (!this.isActive()) return;
        const role = this.negotiationRole(peerId);
        if (
            role === "self" ||
            this.peers.has(peerId) ||
            this.pendingPeers.has(peerId) ||
            !this.hasCapacity()
        ) {
            return;
        }

        if (role === "answering") {
            if (!isResponse) {
                const topic = await createPeerTopic(this.roomTopic!, peerId);
                await this.publishPayload(
                    topic,
                    { v: protocolVersion, type: "announce-response", to: peerId },
                    [relay],
                );
            }
            return;
        }

        const offerId = createOfferId();
        await this.negotiatePeer(peerId, relay, role, offerId, (peer) => peer.createOffer());
    }

    private async receiveOffer(
        peerId: PeerId,
        offerId: string,
        signal: RTCSessionDescriptionInit,
        relay: NostrRelay,
    ): Promise<void> {
        if (!this.isActive()) return;
        const role = this.negotiationRole(peerId);
        if (
            role !== "answering" ||
            this.peers.has(peerId) ||
            this.pendingPeers.has(peerId) ||
            !this.hasCapacity()
        ) {
            return;
        }

        await this.negotiatePeer(peerId, relay, role, offerId, (peer) => peer.acceptOffer(signal));
    }

    private async negotiatePeer(
        peerId: PeerId,
        relay: NostrRelay,
        role: PeerRole,
        offerId: string,
        createSignal: (peer: WebrtcDataPeer) => Promise<RTCSessionDescriptionInit>,
    ): Promise<void> {
        const generation = this.connectionGeneration;
        this.pauseAnnouncementRetry();
        let peer: WebrtcDataPeer;
        try {
            peer = this.createPeer(peerId, role === "offering");
        } catch (error) {
            this.emit("peer-error", [error]);
            this.ensureAnnouncementRecovery();
            return;
        }
        const pending: PendingPeer = {
            peer,
            relay,
            role,
            offerId,
            signalPublished: false,
        };
        this.pendingPeers.set(peerId, pending);

        try {
            const signal = await createSignal(peer);
            if (this.pendingPeers.get(peerId) !== pending) {
                this.ensureAnnouncementRecovery();
                return;
            }
            const topic = await createPeerTopic(this.roomTopic!, peerId);
            const signalType = role === "offering" ? "offer" : "answer";
            const published = await this.publishPayload(
                topic,
                {
                    v: protocolVersion,
                    type: signalType,
                    to: peerId,
                    offerId,
                    signal,
                },
                [relay],
            );
            if (!published) {
                peer.destroy();
                this.ensureAnnouncementRecovery();
                return;
            }
            if (this.pendingPeers.get(peerId) !== pending) {
                this.startAnnouncementRecoveryEpisode();
                return;
            }
            this.cancelAnnouncementRetry();
            pending.signalPublished = true;
            this.startSignalTimeout(peerId, pending);
            this.emitDebug({
                type: role === "offering" ? "offer-sent" : "answer-sent",
                peerId,
                offerId,
                relay: relay.url,
            });
        } catch (error) {
            if (
                !this.isActive() ||
                !this.isCurrentGeneration(generation) ||
                this.pendingPeers.get(peerId) !== pending
            ) {
                return;
            }
            this.emit("peer-error", [error]);
            peer.destroy();
            this.ensureAnnouncementRecovery();
        }
    }

    private async receiveAnswer(
        peerId: PeerId,
        offerId: string,
        signal: RTCSessionDescriptionInit,
    ): Promise<void> {
        if (!this.isActive()) return;
        const generation = this.connectionGeneration;
        const pending = this.pendingPeers.get(peerId);
        if (!pending || pending.role !== "offering" || pending.offerId !== offerId) return;
        try {
            await pending.peer.acceptAnswer(signal);
        } catch (error) {
            if (
                !this.isActive() ||
                !this.isCurrentGeneration(generation) ||
                this.pendingPeers.get(peerId) !== pending
            ) {
                return;
            }
            this.emit("peer-error", [error]);
            pending.peer.destroy();
        }
    }

    private createPeer(remotePeerId: PeerId, initiator: boolean): WebrtcDataPeer {
        const generation = this.connectionGeneration;
        const peer = new WebrtcDataPeer({
            initiator,
            rtcConfig: this.rtcConfig,
            channelLabel: this.channelName,
            iceGatheringTimeout: this.offerTimeout,
            fallbackMaxMessageSize: this.fallbackMaxMessageSize,
            maxBufferedAmount: this.maxBufferedAmount,
            ...(this._RTCPeerConnection ? { RTCPeerConnection: this._RTCPeerConnection } : {}),
            onOpen: (peer) => this.onPeerOpen(peer, remotePeerId, initiator, generation),
            onMessage: (peer, data) => this.onPeerMessage(peer, remotePeerId, data, generation),
            onClose: (peer) => this.removePeer(peer, remotePeerId, generation),
            onError: (_peer, error) => {
                if (this.isActive() && this.isCurrentGeneration(generation)) {
                    this.emit("peer-error", [error]);
                }
            },
            onDebug: (event) => this.emitDebug({ type: String(event["type"]), ...event }),
        });
        this.emitDebug({ type: "peer-created", initiator, peerId: remotePeerId });
        return peer;
    }

    private onPeerOpen(
        peer: WebrtcDataPeer,
        peerId: PeerId,
        initiator: boolean,
        generation: number = this.connectionGeneration,
    ): void {
        if (!this.isActive() || !this.isCurrentGeneration(generation)) {
            peer.destroy();
            return;
        }
        const existing = this.peers.get(peerId);
        if (existing && existing !== peer) {
            peer.destroy();
            return;
        }
        const pending = this.pendingPeers.get(peerId);
        if (pending?.peer === peer) {
            clearTimeout(pending.timer);
            this.pendingPeers.delete(peerId);
        }
        this.peers.set(peerId, peer);
        this.cancelAnnouncementRetry();
        this.emitDebug({ type: "peer-connect", peerId, initiator });
        if (!this.sendSyncStep1(peer) || !this.sendAwareness(peer)) return;
        this.emit("peers", [this.peerIds]);
    }

    private onPeerMessage(
        peer: WebrtcDataPeer,
        peerId: PeerId,
        data: ArrayBuffer,
        generation: number = this.connectionGeneration,
    ): void {
        if (!this.isActive() || !this.isCurrentGeneration(generation)) return;
        if (this.peers.get(peerId) !== peer || !peer.connected) return;
        try {
            if (this.readPeerMessage(peer, peerId, data) === "sync-step-2") {
                this.syncedPeers.add(peer);
                this.updateSyncedState();
            }
        } catch (error) {
            this.emit("peer-error", [error]);
            peer.destroy();
        }
    }

    private readPeerMessage(
        peer: WebrtcDataPeer,
        peerId: PeerId,
        data: ArrayBuffer,
    ): PeerMessageResult {
        const decoder = decoding.createDecoder(new Uint8Array(data));
        const encoder = encoding.createEncoder();
        const messageType = decoding.readVarUint(decoder);

        if (messageType === messageSync) {
            encoding.writeVarUint(encoder, messageSync);
            const syncType = syncProtocol.readSyncMessage(decoder, encoder, this.doc, peer);
            if (
                encoding.length(encoder) > 1 &&
                !this.sendProtocolMessage(peer, encoding.toUint8Array(encoder))
            ) {
                return "handled";
            }
            return syncType === syncProtocol.messageYjsSyncStep2 ? "sync-step-2" : "handled";
        }
        if (messageType === messageAwareness) {
            awarenessProtocol.applyAwarenessUpdate(
                this.awareness,
                decoding.readVarUint8Array(decoder),
                peer,
            );
        } else if (messageType === messageQueryAwareness) {
            this.sendAwareness(peer);
        } else if (messageType === messageDirect) {
            this.emit("direct-message", [peerId, decoding.readVarUint8Array(decoder)]);
        }
        return "handled";
    }

    private removePeer(
        peer: WebrtcDataPeer,
        peerId: PeerId,
        generation: number = this.connectionGeneration,
    ): void {
        if (!this.isCurrentGeneration(generation)) return;
        let changed = false;
        let shouldRecover = false;
        this.syncedPeers.delete(peer);
        this.updateSyncedState();
        if (peerId && this.peers.get(peerId) === peer) {
            this.peers.delete(peerId);
            changed = true;
        }
        if (peerId) {
            const pending = this.pendingPeers.get(peerId);
            if (pending?.peer === peer) {
                clearTimeout(pending.timer);
                this.pendingPeers.delete(peerId);
                shouldRecover = pending.signalPublished;
            }
        }
        if (changed) this.emit("peers", [this.peerIds]);
        if (changed || shouldRecover) this.startAnnouncementRecoveryEpisode();
    }

    private sendProtocolMessage(peer: WebrtcDataPeer, message: Uint8Array<ArrayBuffer>): boolean {
        try {
            if (peer.send(message)) return true;
        } catch (error) {
            this.emit("peer-error", [error]);
        }
        peer.destroy();
        return false;
    }

    private sendSyncStep1(peer: WebrtcDataPeer): boolean {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.writeSyncStep1(encoder, this.doc);
        return this.sendProtocolMessage(peer, encoding.toUint8Array(encoder));
    }

    private sendAwareness(peer: WebrtcDataPeer): boolean {
        const states = Array.from(this.awareness.getStates().keys());
        if (states.length === 0) return true;
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageAwareness);
        encoding.writeVarUint8Array(
            encoder,
            awarenessProtocol.encodeAwarenessUpdate(this.awareness, states),
        );
        return this.sendProtocolMessage(peer, encoding.toUint8Array(encoder));
    }

    private broadcastSyncUpdate(update: Uint8Array): void {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageSync);
        syncProtocol.writeUpdate(encoder, update);
        this.broadcast(encoding.toUint8Array(encoder));
    }

    private broadcastAwareness(update: Uint8Array): void {
        const encoder = encoding.createEncoder();
        encoding.writeVarUint(encoder, messageAwareness);
        encoding.writeVarUint8Array(encoder, update);
        this.broadcast(encoding.toUint8Array(encoder));
    }

    private broadcast(message: Uint8Array<ArrayBuffer>): void {
        for (const peer of this.peers.values()) {
            if (peer.connected) this.sendProtocolMessage(peer, message);
        }
    }

    private emitDebug(event: DebugEvent): void {
        if (this.debug) this.emit("debug", [event]);
    }

    disconnect(): void {
        const localAwarenessState =
            this._ownsAwareness && !this.destroyed ? this.awareness.getLocalState() : null;
        if (localAwarenessState !== null) this.awareness.setLocalState(null);
        this.shouldConnect = false;
        this.connectionActive = false;
        this.connectionPromise = null;
        this.connectionGeneration++;
        this.cancelAnnouncementRetry();
        this.openRelays.clear();
        this.recoveryStartPending = false;
        for (const relay of this.relayConnections) relay.destroy();
        this.relayConnections = [];
        for (const peer of this.peers.values()) peer.destroy();
        for (const pending of this.pendingPeers.values()) {
            clearTimeout(pending.timer);
            pending.peer.destroy();
        }
        this.peers.clear();
        this.pendingPeers.clear();
        this.syncedPeers.clear();
        if (this._synced) {
            this._synced = false;
            this.emit("synced", [false]);
        }
        if (localAwarenessState !== null) this.awareness.setLocalState(localAwarenessState);
    }

    override destroy(): void {
        if (this.destroyed) return;
        this.destroyed = true;
        if (this._ownsAwareness && this.awareness.getLocalState() !== null) {
            this.awareness.setLocalState(null);
        }
        this.disconnect();
        this.doc.off("update", this._docUpdateHandler);
        this.awareness.off("update", this._awarenessUpdateHandler);
        if (this._ownsAwareness) this.awareness.destroy();
        super.destroy();
    }

    private isActive(): boolean {
        return !this.destroyed && this.shouldConnect;
    }

    private isCurrentGeneration(generation: number): boolean {
        return generation === this.connectionGeneration;
    }

    private negotiationRole(peerId: PeerId): NegotiationRole {
        if (peerId === this.peerId) return "self";
        return this.peerId < peerId ? "offering" : "answering";
    }

    private hasValidRouting(payload: SignalPayload, eventTopic: string): boolean {
        if (payload.type === "announce") {
            return payload.topic === this.roomTopic && eventTopic === this.roomTopic;
        }
        return (
            payload.to === this.peerId &&
            payload.topic === this.peerTopic &&
            eventTopic === this.peerTopic
        );
    }

    private updateSyncedState(): void {
        const synced = this.syncedPeers.size > 0;
        if (synced === this._synced) return;
        this._synced = synced;
        this.emit("synced", [synced]);
    }

    private startAnnouncementRecoveryEpisode(): void {
        if (this.openRelays.size === 0) {
            this.recoveryStartPending = true;
            return;
        }
        this.cancelAnnouncementRetry();
        this.scheduleAnnouncementRetry();
    }

    private scheduleAnnouncementRetry(): void {
        if (!this.isActive() || this.announcementRetry) return;
        this.announcementRetry = {
            attempt: 0,
            generation: this.connectionGeneration,
            paused: false,
        };
        this.scheduleNextAnnouncementRetry(this.announcementRetry);
    }

    private scheduleNextAnnouncementRetry(
        state: NonNullable<NostrProvider["announcementRetry"]>,
    ): void {
        if (
            this.announcementRetry !== state ||
            !this.isActive() ||
            !this.isCurrentGeneration(state.generation) ||
            state.paused ||
            state.timer ||
            state.attempt >= 3
        ) {
            return;
        }
        const delay = 500 * 2 ** state.attempt * (0.75 + Math.random() * 0.5);
        state.timer = setTimeout(() => {
            delete state.timer;
            state.attempt++;
            if (
                this.announcementRetry !== state ||
                !this.isActive() ||
                !this.isCurrentGeneration(state.generation)
            ) {
                return;
            }
            void this.announce().finally(() => this.scheduleNextAnnouncementRetry(state));
        }, delay);
    }

    private pauseAnnouncementRetry(): void {
        if (!this.announcementRetry) return;
        this.announcementRetry.paused = true;
        clearTimeout(this.announcementRetry.timer);
        delete this.announcementRetry.timer;
    }

    private ensureAnnouncementRecovery(): void {
        if (!this.announcementRetry) {
            this.startAnnouncementRecoveryEpisode();
            return;
        }
        this.announcementRetry.paused = false;
        this.scheduleNextAnnouncementRetry(this.announcementRetry);
    }

    private cancelAnnouncementRetry(): void {
        if (!this.announcementRetry) return;
        clearTimeout(this.announcementRetry.timer);
        this.announcementRetry = null;
    }

    private hasCapacity(): boolean {
        return this.peers.size + this.pendingPeers.size < this.maxConns;
    }

    private startSignalTimeout(peerId: PeerId, pending: PendingPeer): void {
        clearTimeout(pending.timer);
        pending.timer = setTimeout(() => {
            if (this.pendingPeers.get(peerId) !== pending) return;
            pending.peer.destroy();
        }, this.signalTimeout);
    }

    private async createPayloadEvent(
        topic: string,
        payload: OutgoingSignalPayload,
    ): Promise<NostrEvent> {
        if (!this.encryptionKey) throw new Error("Provider encryption is not initialized");
        const authenticatedPayload: SignalPayload = {
            ...payload,
            from: this.peerId,
            topic,
        } as SignalPayload;
        const content = await encrypt(this.encryptionKey, JSON.stringify(authenticatedPayload));
        return createEvent(topic, content, this.identity);
    }

    private async publishPayload(
        topic: string,
        payload: OutgoingSignalPayload,
        relays: readonly NostrRelay[] = this.relayConnections,
    ): Promise<boolean> {
        if (!this.isActive()) return false;
        const generation = this.connectionGeneration;
        try {
            const event = await this.createPayloadEvent(topic, payload);
            if (!this.isActive() || !this.isCurrentGeneration(generation)) return false;
            const hadOpenRelay = relays.some((relay) => relay.isOpen());
            let published = false;
            for (const relay of relays) published = relay.publish(event) || published;
            if (!published) {
                if (!hadOpenRelay) {
                    this.emit("connection-error", [
                        { error: new Error(`No open relay accepted ${payload.type} signaling`) },
                    ]);
                }
                return false;
            }
            this.emitDebug({
                type: "signal-sent",
                signalType: payload.type,
                relays: relays.map((relay) => relay.url),
            });
            return true;
        } catch (error) {
            this.emit("connection-error", [{ error }]);
            return false;
        }
    }

    private hasSeenEvent(id: string): boolean {
        return this.seenEvents.has(id);
    }

    private rememberEvent(id: string): void {
        this.seenEvents.add(id);
        this.seenEventQueue.push(id);
        if (this.seenEventQueue.length > seenEventLimit) {
            const removed = this.seenEventQueue.shift();
            if (removed) this.seenEvents.delete(removed);
        }
    }
}

const createOfferId = (): string => {
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    let result = "";
    for (const byte of bytes) result += byte.toString(16).padStart(2, "0");
    return result;
};

const hasValidSignalExpiration = (event: NostrEvent): boolean => {
    const tags = event.tags.filter((tag) => tag[0] === "expiration");
    if (tags.length !== 1 || tags[0]!.length !== 2) return false;
    const value = tags[0]![1]!;
    if (!/^(0|[1-9]\d*)$/.test(value)) return false;
    const expiration = Number(value);
    return (
        Number.isSafeInteger(expiration) &&
        expiration === event.created_at + signalExpirationSeconds
    );
};

const isSignalPayload = (value: unknown): value is SignalPayload => {
    if (!value || typeof value !== "object") return false;
    const payload = value as Record<string, unknown>;
    if (
        typeof payload["v"] !== "number" ||
        typeof payload["from"] !== "string" ||
        typeof payload["topic"] !== "string"
    ) {
        return false;
    }
    if (payload["type"] === "announce") {
        return (
            payload["to"] === undefined &&
            payload["offerId"] === undefined &&
            payload["signal"] === undefined
        );
    }
    if (payload["type"] === "announce-response") {
        return (
            typeof payload["to"] === "string" &&
            payload["offerId"] === undefined &&
            payload["signal"] === undefined
        );
    }
    if (payload["type"] === "offer" || payload["type"] === "answer") {
        return (
            typeof payload["to"] === "string" &&
            typeof payload["offerId"] === "string" &&
            !!payload["signal"] &&
            typeof payload["signal"] === "object"
        );
    }
    return false;
};

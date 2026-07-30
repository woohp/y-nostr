import { randomHex } from "./crypto.js";
import {
    getEventTopic,
    isNostrEvent,
    nostrEventKind,
    nostrTopicTag,
    verifyEvent,
    type NostrEvent,
} from "./nostr-event.js";

export const defaultRelayUrls = [
    "wss://basspistol.org",
    "wss://bucket.coracle.social",
    "wss://chorus.almostmachines.dev",
    "wss://chorus.pjv.me",
    "wss://communities.nos.social",
    "wss://hol.is",
    "wss://koru.bitcointxoko.org",
    "wss://nos.lol",
    "wss://nostr-relay.corb.net",
    "wss://nostr.data.haus",
    "wss://nostr.islandarea.net",
    "wss://nostr.self-determined.de",
    "wss://purplerelay.com",
    "wss://relay.agorist.space",
    "wss://relay.damus.io",
    "wss://relay.froth.zone",
    "wss://relay.libernet.app",
    "wss://relay.mostr.pub",
    "wss://relay.nostr.place",
    "wss://relay.sigit.io",
    "wss://schnorr.me",
    "wss://slick.mjex.me",
    "wss://strfry.openhoofd.nl",
    "wss://yabu.me/v2",
] as const;

export const defaultRelayRedundancy = 5;

export const selectDefaultRelayUrls = (
    namespace: string,
    redundancy: number = defaultRelayRedundancy,
): readonly string[] => {
    const urls = [...defaultRelayUrls];
    let seed = 2166136261;
    for (let i = 0; i < namespace.length; i++) {
        seed = Math.imul(seed ^ namespace.charCodeAt(i), 16777619);
    }
    const random = (): number => {
        seed ^= seed << 13;
        seed ^= seed >>> 17;
        seed ^= seed << 5;
        return (seed >>> 0) / 4294967296;
    };
    for (let i = urls.length - 1; i > 0; i--) {
        const j = Math.floor(random() * (i + 1));
        [urls[i], urls[j]] = [urls[j]!, urls[i]!];
    }
    return urls.slice(0, Math.max(0, Math.min(urls.length, Math.floor(redundancy))));
};

export interface NostrRelayOptions {
    topics: readonly string[];
    onEvent: (event: NostrEvent, relay: NostrRelay) => void;
    onOpen?: (relay: NostrRelay, reconnected: boolean) => void;
    onStatus?: (status: "connected" | "disconnected", relay: NostrRelay) => void;
    onError?: (error: unknown, relay: NostrRelay) => void;
    connectTimeout?: number;
    WebSocket?: typeof WebSocket;
}

export class NostrRelay {
    readonly url: string;
    readonly topics: readonly string[];
    readonly onEvent: NostrRelayOptions["onEvent"];
    readonly onOpen: NonNullable<NostrRelayOptions["onOpen"]>;
    readonly onStatus: NonNullable<NostrRelayOptions["onStatus"]>;
    readonly onError: NonNullable<NostrRelayOptions["onError"]>;
    readonly WebSocket?: typeof WebSocket;
    readonly connectTimeout: number;
    readonly subscriptionId: string;
    destroyed = false;
    didOpen = false;
    connected = false;
    reconnectDelay = 1000;
    socket: WebSocket | null = null;
    reconnectTimer: ReturnType<typeof setTimeout> | undefined;
    connectTimer: ReturnType<typeof setTimeout> | undefined;

    constructor(url: string, opts: NostrRelayOptions) {
        const parsedUrl = new URL(url);
        if (parsedUrl.protocol !== "ws:" && parsedUrl.protocol !== "wss:") {
            throw new Error(`Invalid relay URL: ${url}`);
        }
        this.url = url;
        this.topics = opts.topics;
        this.onEvent = opts.onEvent;
        this.onOpen = opts.onOpen ?? (() => {});
        this.onStatus = opts.onStatus ?? (() => {});
        this.onError = opts.onError ?? (() => {});
        this.WebSocket = opts.WebSocket ?? globalThis.WebSocket;
        this.connectTimeout = opts.connectTimeout ?? 10000;
        this.subscriptionId = randomHex(8);
        this.connect();
    }

    connect(): void {
        if (this.destroyed) return;
        clearTimeout(this.connectTimer);
        this.connectTimer = undefined;
        if (!this.WebSocket) {
            this.onError(new Error("WebSocket is not available"), this);
            return;
        }

        let socket: WebSocket;
        try {
            socket = new this.WebSocket(this.url);
            this.socket = socket;
        } catch (error) {
            this.socket = null;
            this.onError(error, this);
            this.scheduleReconnect();
            return;
        }
        this.connectTimer = setTimeout(() => {
            if (
                socket !== this.socket ||
                this.destroyed ||
                socket.readyState === this.WebSocket!.OPEN
            ) {
                return;
            }
            this.socket = null;
            this.connectTimer = undefined;
            this.onError(new Error(`WebSocket connection timed out: ${this.url}`), this);
            try {
                socket.close();
            } catch {
                // The reconnect below is still required if closing the stale socket fails.
            }
            this.scheduleReconnect();
        }, this.connectTimeout);
        socket.addEventListener("open", () => {
            if (socket !== this.socket || this.destroyed) return;
            clearTimeout(this.connectTimer);
            this.connectTimer = undefined;
            const reconnected = this.didOpen;
            this.didOpen = true;
            this.reconnectDelay = 1000;
            if (!this.subscribe()) {
                socket.close();
                return;
            }
            this.connected = true;
            this.onStatus("connected", this);
            this.onOpen(this, reconnected);
        });
        socket.addEventListener("message", (event) => {
            if (socket === this.socket) void this.handleMessage(event.data, socket);
        });
        socket.addEventListener("error", (event) => {
            if (socket === this.socket && !this.destroyed) this.onError(event, this);
        });
        socket.addEventListener("close", () => {
            if (socket !== this.socket || this.destroyed) return;
            clearTimeout(this.connectTimer);
            this.connectTimer = undefined;
            if (this.connected) {
                this.connected = false;
                this.onStatus("disconnected", this);
            }
            this.scheduleReconnect();
        });
    }

    subscribe(): boolean {
        return this.send([
            "REQ",
            this.subscriptionId,
            {
                kinds: [nostrEventKind],
                since: Math.floor(Date.now() / 1000) - 5,
                [`#${nostrTopicTag}`]: this.topics,
            },
        ]);
    }

    publish(event: NostrEvent): boolean {
        return this.send(["EVENT", event]);
    }

    async handleMessage(
        data: string | Blob | ArrayBufferLike,
        socket: WebSocket | null = this.socket,
    ): Promise<void> {
        if (!socket || socket !== this.socket || this.destroyed) return;
        let message: unknown;
        try {
            const text =
                typeof data === "string"
                    ? data
                    : data instanceof Blob
                      ? await data.text()
                      : new TextDecoder().decode(data as AllowSharedBufferSource);
            if (socket !== this.socket || this.destroyed) return;
            message = JSON.parse(text);
        } catch (error) {
            this.onError(error, this);
            return;
        }

        if (!Array.isArray(message) || typeof message[0] !== "string") return;
        if (message[0] === "NOTICE") {
            this.onError(new Error(`Nostr relay notice: ${String(message[1] ?? "")}`), this);
            return;
        }
        if (message[0] === "OK" && message[2] === false) {
            this.onError(
                new Error(`Nostr relay rejected event: ${String(message[3] ?? "")}`),
                this,
            );
            return;
        }
        if (message[0] !== "EVENT" || message[1] !== this.subscriptionId) return;

        const event = message[2];
        if (!isNostrEvent(event) || !hasSubscribedTopic(event, this.topics)) return;
        if (!(await verifyEvent(event)) || socket !== this.socket || this.destroyed) return;
        this.onEvent(event, this);
    }

    scheduleReconnect(): void {
        if (this.destroyed) return;
        clearTimeout(this.reconnectTimer);
        const jitteredDelay = this.reconnectDelay * (0.75 + Math.random() * 0.5);
        this.reconnectTimer = setTimeout(() => this.connect(), jitteredDelay);
        this.reconnectDelay = Math.min(this.reconnectDelay * 2, 30000);
    }

    send(message: unknown[]): boolean {
        if (!this.isOpen()) return false;
        try {
            this.socket.send(JSON.stringify(message));
            return true;
        } catch (error) {
            this.onError(error, this);
            return false;
        }
    }

    isOpen(): this is this & { socket: WebSocket; WebSocket: typeof WebSocket } {
        return !!this.socket && !!this.WebSocket && this.socket.readyState === this.WebSocket.OPEN;
    }

    destroy(): void {
        if (this.destroyed) return;
        this.destroyed = true;
        this.connected = false;
        clearTimeout(this.reconnectTimer);
        clearTimeout(this.connectTimer);
        this.connectTimer = undefined;
        if (this.isOpen()) this.send(["CLOSE", this.subscriptionId]);
        this.socket?.close();
        this.socket = null;
    }
}

const hasSubscribedTopic = (event: NostrEvent, topics: readonly string[]): boolean => {
    const topic = getEventTopic(event);
    return event.kind === nostrEventKind && topic !== null && topics.includes(topic);
};

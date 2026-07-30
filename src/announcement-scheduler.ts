import type { NostrRelay } from "./nostr-relay.js";

/**
 * Bounded, jittered retry for room announcements.
 *
 * The provider never announces periodically -- that would make it a poor public-relay citizen.
 * It announces on connection and on recovery only, so this scheduler covers the narrow windows
 * where the room would otherwise go silent: a peer we were talking to dropped, a handshake
 * failed, or the announcement published as a relay opened never landed. An episode fires at
 * most `maxAttempts` times with exponential backoff and then goes quiet for good.
 *
 * A retry is only worth arming while a relay is open to carry it, and only while the connection
 * generation that requested it is still live, so this owns the set of open relays and validates
 * every resumption against the generation captured when its episode began.
 */
export type AnnouncementSchedulerState =
    /** Nothing scheduled. */
    | "idle"
    /** A retry timer is pending. */
    | "armed"
    /** An announcement is in flight; the next retry is armed when it settles. */
    | "announcing"
    /** A handshake is in flight and owns the room. Retries resume if it fails. */
    | "paused"
    /** A retry is wanted, but no relay is open to carry it yet. */
    | "awaiting-relay"
    /** The episode used its whole retry budget. Only a new request revives it. */
    | "exhausted";

export interface AnnouncementSchedulerOptions {
    /** Publishes an announcement to every connected relay. */
    announce: () => Promise<unknown>;
    /** The connection generation a new episode belongs to. */
    generation: () => number;
    /** False once `generation` has been superseded, disconnected, or destroyed. */
    isLive: (generation: number) => boolean;
    maxAttempts?: number;
    baseDelay?: number;
}

interface Episode {
    attempt: number;
    generation: number;
    paused: boolean;
    timer?: ReturnType<typeof setTimeout>;
}

export class AnnouncementScheduler {
    private readonly options: Required<AnnouncementSchedulerOptions>;
    private readonly openRelays = new Set<NostrRelay>();
    private episode: Episode | null = null;
    private queued = false;

    constructor(options: AnnouncementSchedulerOptions) {
        this.options = { maxAttempts: 3, baseDelay: 500, ...options };
    }

    get state(): AnnouncementSchedulerState {
        if (!this.episode) return this.queued ? "awaiting-relay" : "idle";
        if (this.episode.paused) return "paused";
        if (this.episode.timer) return "armed";
        return this.episode.attempt >= this.options.maxAttempts ? "exhausted" : "announcing";
    }

    /** Attempts already made by the current episode. */
    get attempt(): number {
        return this.episode?.attempt ?? 0;
    }

    /**
     * The room's peer set changed in a way that warrants re-announcing. Restarts the episode
     * from attempt zero, discarding any budget a previous episode had already spent.
     */
    request(): void {
        if (this.openRelays.size === 0) {
            this.queued = true;
            return;
        }
        this.cancel();
        this.arm();
    }

    /** Suspends retries while a handshake owns the room. */
    pause(): void {
        if (!this.episode) return;
        this.episode.paused = true;
        clearTimeout(this.episode.timer);
        delete this.episode.timer;
    }

    /**
     * Releases a pause. A pause can outlive its episode (a cancel or a fresh request may have
     * replaced it while a handshake was in flight), in which case there is nothing to release
     * and this requests a new episode instead.
     */
    resume(): void {
        if (!this.episode) {
            this.request();
            return;
        }
        this.episode.paused = false;
        this.scheduleNext(this.episode);
    }

    /** Ends the current episode. Retries stay off until something requests a new one. */
    cancel(): void {
        if (!this.episode) return;
        clearTimeout(this.episode.timer);
        this.episode = null;
    }

    relayOpened(relay: NostrRelay): void {
        // The first relay to come up always queues a request: it backstops the announcement
        // built before any socket existed, which may have been dropped on the way out.
        if (this.openRelays.size === 0) this.queued = true;
        this.openRelays.add(relay);
    }

    relayClosed(relay: NostrRelay): void {
        this.openRelays.delete(relay);
        if (this.openRelays.size > 0) return;
        this.pause();
        this.queued = true;
    }

    /**
     * Starts a request queued while no relay was open. Call this once the announcement sent on
     * a freshly opened relay has settled -- arming any earlier would race the very announcement
     * the retry exists to back up.
     */
    startQueuedRequest(): void {
        if (!this.queued || this.openRelays.size === 0) return;
        this.queued = false;
        this.request();
    }

    /** Drops all retry and relay state, for provider disconnect. */
    reset(): void {
        this.cancel();
        this.openRelays.clear();
        this.queued = false;
    }

    /** Begins an episode if none is running. Unlike `request`, never resets a live episode. */
    private arm(): void {
        if (this.episode || !this.options.isLive(this.options.generation())) return;
        this.episode = { attempt: 0, generation: this.options.generation(), paused: false };
        this.scheduleNext(this.episode);
    }

    private scheduleNext(episode: Episode): void {
        if (
            this.episode !== episode ||
            !this.options.isLive(episode.generation) ||
            episode.paused ||
            episode.timer ||
            episode.attempt >= this.options.maxAttempts
        ) {
            return;
        }
        const delay = this.options.baseDelay * 2 ** episode.attempt * (0.75 + Math.random() * 0.5);
        episode.timer = setTimeout(() => {
            delete episode.timer;
            episode.attempt++;
            if (this.episode !== episode || !this.options.isLive(episode.generation)) return;
            void this.options.announce().finally(() => this.scheduleNext(episode));
        }, delay);
    }
}

import type { NostrRelay } from "./nostr-relay.js";

const maxAttempts = 3;
const baseDelay = 500;

interface Episode {
    attempt: number;
    paused: boolean;
    timer?: ReturnType<typeof setTimeout>;
}

/**
 * Bounded, jittered retry for room announcements.
 *
 * The provider never announces periodically -- that would make it a poor public-relay citizen.
 * It announces on connection and on recovery only, so this scheduler covers the narrow windows
 * where the room would otherwise go silent: a peer we were talking to dropped, a handshake
 * failed, or the announcement published as a relay opened never landed. An episode fires at
 * most `maxAttempts` times with exponential backoff and then goes quiet for good.
 *
 * A retry is only worth arming while a relay is open to carry it, so this owns the set of open
 * relays. The provider calls `reset` whenever its connection ends.
 */
export class AnnouncementScheduler {
    private readonly announce: () => Promise<unknown>;
    private readonly openRelays = new Set<NostrRelay>();
    private episode: Episode | null = null;
    private queued = false;

    constructor(announce: () => Promise<unknown>) {
        this.announce = announce;
    }

    /**
     * The room's peer set changed in a way that warrants re-announcing. Restarts the episode
     * from attempt zero, discarding any budget a previous episode had already spent. With no
     * relay open there is nothing to do: the next relay to open queues an episode anyway.
     */
    request(): void {
        if (this.openRelays.size === 0) return;
        this.cancel();
        this.episode = { attempt: 0, paused: false };
        this.scheduleNext(this.episode);
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
        if (this.openRelays.size === 0) this.cancel();
    }

    /**
     * Starts the request queued when the first relay opened. Call this once the announcement
     * sent on that relay has settled -- arming any earlier would race the very announcement the
     * retry exists to back up.
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

    private scheduleNext(episode: Episode): void {
        if (
            this.episode !== episode ||
            episode.paused ||
            episode.timer ||
            episode.attempt >= maxAttempts
        ) {
            return;
        }
        const delay = baseDelay * 2 ** episode.attempt * (0.75 + Math.random() * 0.5);
        episode.timer = setTimeout(() => {
            delete episode.timer;
            episode.attempt++;
            void this.announce().finally(() => this.scheduleNext(episode));
        }, delay);
    }
}

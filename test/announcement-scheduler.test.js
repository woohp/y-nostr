import assert from "node:assert/strict";
import test from "node:test";

const { AnnouncementScheduler } = await import("../dist/announcement-scheduler.js");

const relay = (url) => ({ url });

function createScheduler(context) {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    const counter = { announcements: 0 };
    const scheduler = new AnnouncementScheduler(async () => {
        counter.announcements++;
    });
    return { scheduler, counter };
}

// Advances past one backoff delay. Math.random is stubbed to 0, so the jitter factor is 0.75
// and delay N is 500 * 2**N * 0.75.
const tickAttempt = async (context, attempt) => {
    context.mock.timers.tick(500 * 2 ** attempt * 0.75);
    await Promise.resolve();
    await Promise.resolve();
};

test("a request waits for an open relay and its settled announcement", async (context) => {
    const { scheduler, counter } = createScheduler(context);
    scheduler.request();
    scheduler.relayOpened(relay("wss://a.test"));
    await tickAttempt(context, 0);
    assert.equal(counter.announcements, 0);

    scheduler.startQueuedRequest();
    await tickAttempt(context, 0);
    assert.equal(counter.announcements, 1);
});

test("an episode spends a bounded retry budget, and a new request restarts it", async (context) => {
    const { scheduler, counter } = createScheduler(context);
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();
    for (const attempt of [0, 1, 2]) await tickAttempt(context, attempt);
    context.mock.timers.tick(60_000);
    assert.equal(counter.announcements, 3);

    scheduler.request();
    await tickAttempt(context, 0);
    assert.equal(counter.announcements, 4);
});

test("pause suspends retries and resume continues the same budget", async (context) => {
    const { scheduler, counter } = createScheduler(context);
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();
    await tickAttempt(context, 0);

    scheduler.pause();
    context.mock.timers.tick(60_000);
    assert.equal(counter.announcements, 1);

    scheduler.resume();
    for (const attempt of [1, 2]) await tickAttempt(context, attempt);
    context.mock.timers.tick(60_000);
    assert.equal(counter.announcements, 3);
});

test("resume starts a new episode when the pause outlived its episode", async (context) => {
    const { scheduler, counter } = createScheduler(context);
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();
    scheduler.pause();
    scheduler.cancel();

    scheduler.resume();
    await tickAttempt(context, 0);
    assert.equal(counter.announcements, 1);
});

test("losing the last relay pauses retries until a relay reopens", async (context) => {
    const { scheduler, counter } = createScheduler(context);
    const first = relay("wss://a.test");
    const second = relay("wss://b.test");
    scheduler.relayOpened(first);
    scheduler.startQueuedRequest();
    scheduler.relayOpened(second);

    scheduler.relayClosed(first);
    await tickAttempt(context, 0);
    assert.equal(counter.announcements, 1, "retries survive while any relay is open");

    scheduler.relayClosed(second);
    context.mock.timers.tick(60_000);
    assert.equal(counter.announcements, 1);

    scheduler.relayOpened(first);
    scheduler.startQueuedRequest();
    await tickAttempt(context, 0);
    assert.equal(counter.announcements, 2, "the reopened relay starts a fresh episode");
});

test("cancel ends the episode and reset also forgets open relays", async (context) => {
    const { scheduler, counter } = createScheduler(context);
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();
    scheduler.cancel();
    context.mock.timers.tick(60_000);
    assert.equal(counter.announcements, 0);

    scheduler.request();
    await tickAttempt(context, 0);
    assert.equal(counter.announcements, 1);

    scheduler.reset();
    scheduler.request();
    context.mock.timers.tick(60_000);
    assert.equal(counter.announcements, 1);
});

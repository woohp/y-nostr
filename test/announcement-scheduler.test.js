import assert from "node:assert/strict";
import test from "node:test";

const { AnnouncementScheduler } = await import("../dist/announcement-scheduler.js");

const relay = (url) => ({ url });

function createScheduler(overrides = {}) {
    const announced = [];
    let generation = 0;
    let active = true;
    const scheduler = new AnnouncementScheduler({
        announce: async () => {
            announced.push(generation);
        },
        generation: () => generation,
        isLive: (episodeGeneration) => active && episodeGeneration === generation,
        ...overrides,
    });
    return {
        scheduler,
        announced,
        supersede: () => generation++,
        deactivate: () => {
            active = false;
        },
    };
}

// Advances past one backoff delay. Math.random is stubbed to 0, so the jitter factor is 0.75
// and delay N is 500 * 2**N * 0.75.
const tickAttempt = async (context, attempt) => {
    context.mock.timers.tick(500 * 2 ** attempt * 0.75);
    await Promise.resolve();
    await Promise.resolve();
};

const withTimers = (context) => {
    context.mock.method(Math, "random", () => 0);
    context.mock.timers.enable({ apis: ["setTimeout"] });
};

test("starts idle and queues a request until a relay is open", (context) => {
    withTimers(context);
    const { scheduler } = createScheduler();
    assert.equal(scheduler.state, "idle");

    scheduler.request();
    assert.equal(scheduler.state, "awaiting-relay");

    scheduler.relayOpened(relay("wss://a.test"));
    assert.equal(scheduler.state, "awaiting-relay");

    scheduler.startQueuedRequest();
    assert.equal(scheduler.state, "armed");
});

test("spends a bounded retry budget with exponential backoff", async (context) => {
    withTimers(context);
    const { scheduler, announced } = createScheduler();
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();

    for (const attempt of [0, 1, 2]) await tickAttempt(context, attempt);
    assert.equal(announced.length, 3);
    assert.equal(scheduler.attempt, 3);
    assert.equal(scheduler.state, "exhausted");

    // An exhausted episode stays quiet no matter how much time passes.
    context.mock.timers.tick(60_000);
    assert.equal(announced.length, 3);
});

test("a fresh request restarts an exhausted episode from attempt zero", async (context) => {
    withTimers(context);
    const { scheduler, announced } = createScheduler();
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();
    for (const attempt of [0, 1, 2]) await tickAttempt(context, attempt);
    assert.equal(scheduler.state, "exhausted");

    scheduler.request();
    assert.equal(scheduler.attempt, 0);
    assert.equal(scheduler.state, "armed");
    await tickAttempt(context, 0);
    assert.equal(announced.length, 4);
});

test("pause suspends retries and resume rearms the same episode", async (context) => {
    withTimers(context);
    const { scheduler, announced } = createScheduler();
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();
    await tickAttempt(context, 0);
    assert.equal(announced.length, 1);

    scheduler.pause();
    assert.equal(scheduler.state, "paused");
    context.mock.timers.tick(60_000);
    assert.equal(announced.length, 1);

    scheduler.resume();
    assert.equal(scheduler.state, "armed");
    assert.equal(scheduler.attempt, 1, "resume continues the budget rather than restarting it");
    await tickAttempt(context, 1);
    assert.equal(announced.length, 2);
});

test("resume starts a new episode when the pause outlived its episode", (context) => {
    withTimers(context);
    const { scheduler } = createScheduler();
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();

    scheduler.pause();
    scheduler.cancel();
    assert.equal(scheduler.state, "idle");

    scheduler.resume();
    assert.equal(scheduler.state, "armed");
});

test("losing the last relay pauses retries and requeues a request", (context) => {
    withTimers(context);
    const { scheduler } = createScheduler();
    const first = relay("wss://a.test");
    const second = relay("wss://b.test");
    scheduler.relayOpened(first);
    scheduler.startQueuedRequest();
    scheduler.relayOpened(second);

    // Retries survive while any relay can still carry them.
    scheduler.relayClosed(first);
    assert.equal(scheduler.state, "armed");

    scheduler.relayClosed(second);
    assert.equal(scheduler.state, "paused");

    // The requeued request only starts once a relay is back and its announcement settles.
    scheduler.relayOpened(first);
    scheduler.startQueuedRequest();
    assert.equal(scheduler.state, "armed");
    assert.equal(scheduler.attempt, 0);
});

test("cancel ends the episode and reset drops relay state too", (context) => {
    withTimers(context);
    const { scheduler, announced } = createScheduler();
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();

    scheduler.cancel();
    assert.equal(scheduler.state, "idle");
    context.mock.timers.tick(60_000);
    assert.equal(announced.length, 0);

    // cancel keeps the relay, so a request can arm immediately; reset forgets it.
    scheduler.request();
    assert.equal(scheduler.state, "armed");
    scheduler.reset();
    assert.equal(scheduler.state, "idle");
    scheduler.request();
    assert.equal(scheduler.state, "awaiting-relay");
});

test("a superseded generation stops announcing", async (context) => {
    withTimers(context);
    const { scheduler, announced, supersede } = createScheduler();
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();

    supersede();
    await tickAttempt(context, 0);
    assert.equal(announced.length, 0, "the episode belongs to a generation that is gone");
});

test("an inactive provider never arms", (context) => {
    withTimers(context);
    const { scheduler, deactivate } = createScheduler();
    deactivate();
    scheduler.relayOpened(relay("wss://a.test"));
    scheduler.startQueuedRequest();
    assert.equal(scheduler.state, "idle");
});

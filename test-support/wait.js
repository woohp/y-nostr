import assert from "node:assert/strict";

// Polls with setImmediate so it still works while tests mock setTimeout, and fails instead of
// hanging when a regression means the condition never holds.
export const waitFor = async (predicate, timeoutMs = 2000) => {
    const deadline = performance.now() + timeoutMs;
    while (!predicate()) {
        if (performance.now() > deadline) assert.fail("Timed out waiting for condition");
        await new Promise((resolve) => setImmediate(resolve));
    }
};

const { describe, it, before, after } = require("node:test");
const assert = require("node:assert/strict");
const {
  acquireIdempotencyLock,
  releaseIdempotencyLock,
} = require("../src/services/idempotencyService");
const redisClient = require("../src/config/redis");

describe("Redis Lock Ownership Tests", () => {
  before(async () => {
    // Ensure Redis is connected
    if (!redisClient.isOpen) {
      await redisClient.connect();
    }
  });

  after(async () => {
    // Clean up test keys
    const keys = await redisClient.keys("idempotency-lock:lock-test-*");
    if (keys.length > 0) {
      await redisClient.del(keys);
    }
    await redisClient.quit();
  });

  it("acquireIdempotencyLock returns a unique UUID token on success", async () => {
    const token = await acquireIdempotencyLock("lock-test-unique");
    assert.ok(token, "Token should be truthy on successful acquisition");
    assert.match(
      token,
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
      "Token should be a valid UUID"
    );

    // Verify the token was actually stored in Redis
    const stored = await redisClient.get("idempotency-lock:lock-test-unique");
    assert.equal(stored, token, "Redis should store the exact token");
  });

  it("acquireIdempotencyLock returns null when the lock is already held", async () => {
    const firstToken = await acquireIdempotencyLock("lock-test-contention");
    assert.ok(firstToken, "First acquisition should succeed");

    const secondToken = await acquireIdempotencyLock("lock-test-contention");
    assert.equal(secondToken, null, "Second acquisition must return null");
  });

  it("releaseIdempotencyLock succeeds with the correct token", async () => {
    const token = await acquireIdempotencyLock("lock-test-correct-release");
    assert.ok(token);

    const released = await releaseIdempotencyLock(
      "lock-test-correct-release",
      token
    );
    assert.equal(released, true, "Release should succeed with the correct token");

    // Verify the key was actually deleted
    const stored = await redisClient.get(
      "idempotency-lock:lock-test-correct-release"
    );
    assert.equal(stored, null, "Lock key should be deleted after release");
  });

  it("releaseIdempotencyLock fails with a mismatched token", async () => {
    const token = await acquireIdempotencyLock("lock-test-wrong-token");
    assert.ok(token);

    const released = await releaseIdempotencyLock(
      "lock-test-wrong-token",
      "wrong-token-value"
    );
    assert.equal(released, false, "Release must fail with incorrect token");

    // Verify the lock is still held with the original token
    const stored = await redisClient.get(
      "idempotency-lock:lock-test-wrong-token"
    );
    assert.equal(
      stored,
      token,
      "Lock should still be held after a mismatched release attempt"
    );
  });

  it("releaseIdempotencyLock returns false when called with null token", async () => {
    const released = await releaseIdempotencyLock("lock-test-null", null);
    assert.equal(released, false, "Should refuse to release with null token");
  });

  it("a second request can acquire the lock after the first correctly releases it", async () => {
    // Request A acquires
    const tokenA = await acquireIdempotencyLock("lock-test-reacquire");
    assert.ok(tokenA);

    // Request A releases
    const releasedA = await releaseIdempotencyLock(
      "lock-test-reacquire",
      tokenA
    );
    assert.equal(releasedA, true);

    // Request B should now be able to acquire
    const tokenB = await acquireIdempotencyLock("lock-test-reacquire");
    assert.ok(tokenB, "Second request should acquire after release");
    assert.notEqual(tokenA, tokenB, "Tokens should be different");
  });

  it("two concurrent locks get different tokens", async () => {
    const tokenA = await acquireIdempotencyLock("lock-test-diff-a");
    const tokenB = await acquireIdempotencyLock("lock-test-diff-b");

    assert.ok(tokenA);
    assert.ok(tokenB);
    assert.notEqual(tokenA, tokenB, "Each lock must get a unique token");
  });
});

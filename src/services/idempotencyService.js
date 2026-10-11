const crypto = require("crypto");
const redisClient = require("../config/redis");

const IDEMPOTENCY_TTL = 24 * 60 * 60; // 24 hours
const LOCK_TTL = 30; // 30 seconds

/**
 * Creates a deterministic SHA-256 fingerprint of the payment parameters.
 * Keys are sorted so object property order doesn't affect the hash.
 */
function createPayloadFingerprint(payload) {
  if (!payload || typeof payload !== "object") {
    return "";
  }
  const normalized = JSON.stringify(payload, Object.keys(payload).sort());
  return crypto.createHash("sha256").update(normalized).digest("hex");
}

async function getIdempotencyResult(key) {
  try {
    const result = await redisClient.get(`idempotency:${key}`);
    if (!result) {
      return null;
    }
    return JSON.parse(result);
  } catch (error) {
    console.error("Redis getIdempotencyResult error:", error.message);
    return null;
  }
}

async function saveIdempotencyResult(key, value, payloadFingerprint) {
  try {
    const entry = {
      ...value,
      _payloadFingerprint: payloadFingerprint || null,
    };

    await redisClient.set(
      `idempotency:${key}`,
      JSON.stringify(entry),
      {
        EX: IDEMPOTENCY_TTL,
      }
    );
  } catch (error) {
    console.error("Redis saveIdempotencyResult error:", error.message);
  }
}

/**
 * Acquires an idempotency lock and returns a unique ownership token.
 * Returns the token string on success, or null if the lock is already held.
 * On Redis failure, returns a fallback token so the request can proceed
 * (PostgreSQL unique constraints remain the final safeguard).
 */
async function acquireIdempotencyLock(key) {
  const token = crypto.randomUUID();
  try {
    const lockKey = `idempotency-lock:${key}`;
    const result = await redisClient.set(
      lockKey,
      token,
      {
        NX: true,
        EX: LOCK_TTL,
      }
    );
    return result === "OK" ? token : null;
  } catch (error) {
    console.error("Redis acquireIdempotencyLock error:", error.message);
    // If Redis is unreachable, return a token to avoid hard-failing requests,
    // relying on Postgres database transactions and unique constraints.
    return token;
  }
}

// Lua script: atomically deletes the key only when the stored value matches
// the caller's token. Returns 1 on successful delete, 0 otherwise.
const RELEASE_LOCK_SCRIPT = `
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
else
  return 0
end
`;

/**
 * Releases the idempotency lock only if the stored token matches.
 * This prevents a request from accidentally releasing another request's lock.
 * @param {string} key  - the idempotency key
 * @param {string} token - the ownership token returned by acquireIdempotencyLock
 * @returns {Promise<boolean>} true if the lock was released, false otherwise
 */
async function releaseIdempotencyLock(key, token) {
  if (!token) return false;
  try {
    const result = await redisClient.eval(
      RELEASE_LOCK_SCRIPT,
      { keys: [`idempotency-lock:${key}`], arguments: [token] }
    );
    return result === 1;
  } catch (error) {
    console.error("Redis releaseIdempotencyLock error:", error.message);
    return false;
  }
}

function verifyPayloadFingerprint(cachedResult, currentFingerprint) {
  if (!cachedResult || !cachedResult._payloadFingerprint) {
    return true; // backwards compatibility if no fingerprint stored
  }
  return cachedResult._payloadFingerprint === currentFingerprint;
}

module.exports = {
  getIdempotencyResult,
  saveIdempotencyResult,
  acquireIdempotencyLock,
  releaseIdempotencyLock,
  createPayloadFingerprint,
  verifyPayloadFingerprint,
};
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

async function acquireIdempotencyLock(key) {
  try {
    const lockKey = `idempotency-lock:${key}`;
    const result = await redisClient.set(
      lockKey,
      "PROCESSING",
      {
        NX: true,
        EX: LOCK_TTL,
      }
    );
    return result === "OK";
  } catch (error) {
    console.error("Redis acquireIdempotencyLock error:", error.message);
    // If Redis is unreachable, return true to avoid hard-failing requests,
    // relying on Postgres database transactions and unique constraints.
    return true;
  }
}

async function releaseIdempotencyLock(key) {
  try {
    await redisClient.del(`idempotency-lock:${key}`);
  } catch (error) {
    console.error("Redis releaseIdempotencyLock error:", error.message);
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
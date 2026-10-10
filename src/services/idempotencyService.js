const crypto = require("crypto");
const redisClient = require("../config/redis");

const IDEMPOTENCY_TTL = 24 * 60 * 60; // 24 hours
const LOCK_TTL = 30; // 30 seconds

/**
 * Create a SHA-256 fingerprint of the payment details.
 * This lets us detect when someone reuses an idempotency key
 * with different payment parameters.
 */
function createPayloadFingerprint(payload) {
  const normalized = JSON.stringify(payload, Object.keys(payload).sort());
  return crypto.createHash("sha256").update(normalized).digest("hex");
}

async function getIdempotencyResult(key) {
  const result = await redisClient.get(`idempotency:${key}`);

  if (!result) {
    return null;
  }

  return JSON.parse(result);
}

async function saveIdempotencyResult(key, value, payloadFingerprint) {
  const entry = {
    ...value,
    _payloadFingerprint: payloadFingerprint,
  };

  await redisClient.set(
    `idempotency:${key}`,
    JSON.stringify(entry),
    {
      EX: IDEMPOTENCY_TTL,
    }
  );
}

async function acquireIdempotencyLock(key) {
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
}

async function releaseIdempotencyLock(key) {
  await redisClient.del(`idempotency-lock:${key}`);
}

module.exports = {
  getIdempotencyResult,
  saveIdempotencyResult,
  acquireIdempotencyLock,
  releaseIdempotencyLock,
  createPayloadFingerprint,
};
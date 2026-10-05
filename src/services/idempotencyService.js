const redisClient = require("../config/redis");

const IDEMPOTENCY_TTL = 24 * 60 * 60;
const LOCK_TTL = 30;

async function getIdempotencyResult(key) {
  const result = await redisClient.get(`idempotency:${key}`);

  if (!result) {
    return null;
  }

  return JSON.parse(result);
}

async function saveIdempotencyResult(key, value) {
  await redisClient.set(
    `idempotency:${key}`,
    JSON.stringify(value),
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
};
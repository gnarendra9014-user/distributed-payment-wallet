const { describe, it } = require("node:test");
const assert = require("node:assert/strict");
const {
  createPayloadFingerprint,
  verifyPayloadFingerprint,
} = require("../src/services/idempotencyService");

describe("Idempotency Service Unit Tests", () => {
  it("generates deterministic SHA-256 fingerprint regardless of key order", () => {
    const payloadA = { sender_user_id: 1, receiver_user_id: 2, amount: 100 };
    const payloadB = { amount: 100, receiver_user_id: 2, sender_user_id: 1 };

    const hashA = createPayloadFingerprint(payloadA);
    const hashB = createPayloadFingerprint(payloadB);

    assert.equal(typeof hashA, "string");
    assert.equal(hashA.length, 64); // SHA-256 hex string length
    assert.equal(hashA, hashB, "Hashes must match regardless of property insertion order");
  });

  it("generates different fingerprints for different payment parameters", () => {
    const payload1 = { sender_user_id: 1, receiver_user_id: 2, amount: 100 };
    const payload2 = { sender_user_id: 1, receiver_user_id: 2, amount: 200 };
    const payload3 = { sender_user_id: 1, receiver_user_id: 3, amount: 100 };

    const hash1 = createPayloadFingerprint(payload1);
    const hash2 = createPayloadFingerprint(payload2);
    const hash3 = createPayloadFingerprint(payload3);

    assert.notEqual(hash1, hash2, "Different amounts must produce different fingerprints");
    assert.notEqual(hash1, hash3, "Different receivers must produce different fingerprints");
  });

  it("verifyPayloadFingerprint detects matching and mismatched fingerprints", () => {
    const currentFingerprint = createPayloadFingerprint({
      sender_user_id: 1,
      receiver_user_id: 2,
      amount: 50,
    });

    const matchingCache = {
      message: "Payment already processed",
      _payloadFingerprint: currentFingerprint,
    };

    const mismatchedCache = {
      message: "Payment already processed",
      _payloadFingerprint: "different-hash-value-12345",
    };

    assert.equal(verifyPayloadFingerprint(matchingCache, currentFingerprint), true);
    assert.equal(verifyPayloadFingerprint(mismatchedCache, currentFingerprint), false);
  });

  it("verifyPayloadFingerprint maintains backwards compatibility for cached entries without fingerprint", () => {
    const legacyCache = { message: "Payment already processed" };
    assert.equal(verifyPayloadFingerprint(legacyCache, "any-hash"), true);
  });
});

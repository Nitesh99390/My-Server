/**
 * drm.js - Sec-MS-GEC **v2** token generator (Edge 155+ "Read Aloud" DRM).
 *
 * Since ~2026-10-10 Microsoft rejects the legacy v1 token (plain SHA-256 of
 * "<ticks><TrustedClientToken>") with HTTP 403 on the websocket handshake.
 * Edge now sends  Sec-MS-GEC=<16 hex>-<64 hex>  and  Sec-MS-GEC-Version=2-...
 *
 * The 64-hex half is SHA-256 over a 57-byte secret prefix + "<ticks><token>".
 * The prefix itself is not public, but the SHA-256 mid-state after 14 rounds
 * of the first block is, together with the partial message-schedule words it
 * contributes - that is exactly what the constants below encode.  The 16-hex
 * half is a proprietary "CS64" checksum keyed by the first 8 bytes of that
 * digest.
 *
 * This is a faithful port of edge-tts (Python) 7.3.x `drm.py`
 * (rany2/edge-tts PR #492 / issue #490) using only plain 32-bit arithmetic,
 * so it runs unchanged in Cloudflare Workers and Node.  Output is verified
 * byte-for-byte against the Python package in test/unit.test.js.
 */

export const TRUSTED_CLIENT_TOKEN = "6A5AA1D4EAFF4E9FB37E23D68491D6F4";
const WIN_EPOCH = 11644473600;

// FIPS 180-4 initial hash value.
const SHA256_INITIAL = [
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];
// Working registers after rounds 0..13 of the first (secret) block.
const SHA256_STATE = [
  0xfd9fbea2, 0x91243ce7, 0xbe499094, 0xcd2e1e0a, 0x6c006f49, 0x04bd35ab, 0xfeb0a7e4, 0x03effbd2,
];
// Partial schedule words W[16..29] contributed by the secret prefix words.
const SHA256_SCHEDULE = [
  0xeb24bf9e, 0x1558691f, 0xa0c09ee2, 0x0cfa32ba, 0xa622ee8c, 0xd1dd9491, 0xc0dd25a0,
  0xf9e70ab6, 0xf0c31573, 0x7058ce4e, 0x7188e606, 0x00e5ab33, 0xd258b060, 0x72766564,
];
const K = [
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];
// The 57th secret byte lives in the top byte of W[14]; these fold it in.
const SHA256_ADJUSTMENT = 0xc9dc06a7;
const K_BLOCK = K.slice();
K_BLOCK[14] = SHA256_ADJUSTMENT;
const SIGMA0_ADJUSTMENT = 0x059c0b80;
const SIGMA1_ADJUSTMENT = 0x000b92c0;

const CS64_COEFFICIENTS = [
  [0xe82e91b1, 0xb32700d0], [0x44de24c6, 0x88dff51d], [0x3beb65bc, 0x29514038], [0xc89c5933, 0xb328cbdf],
  [0x8d1ec211, 0xd405b4d5], [0x314ef5d5, 0x26e55f3c], [0xe92491b1, 0x607801c6], [0xf0a5b9d5, 0x82b2c1fb],
  [0x06064cf3, 0xfa51a3da], [0xda79fa60, 0x207cc464], [0xbed20560, 0x3c3c74f3], [0x89f9c9f8, 0xd2d14b61],
  [0x35fa38c6, 0xbb04a2c1], [0xcb49e3c6, 0xc5626167],
];
const CS64_FACTOR = 0x82893933;
const CS64_ADJUSTMENT0 = 0xf2f90b1a;
const CS64_ADJUSTMENT1 = 0x0b1a0000;

// ---------------------------------------------------------------------------
// 32-bit helpers
// ---------------------------------------------------------------------------
const u32 = (x) => x >>> 0;
const add = (...xs) => {
  let s = 0;
  for (const x of xs) s = (s + x) >>> 0;
  return s;
};
const rotr = (x, n) => ((x >>> n) | (x << (32 - n))) >>> 0;
const rol16 = (x) => ((x << 16) | (x >>> 16)) >>> 0;
const sigma0 = (x) => (rotr(x, 7) ^ rotr(x, 18) ^ (x >>> 3)) >>> 0;
const sigma1 = (x) => (rotr(x, 17) ^ rotr(x, 19) ^ (x >>> 10)) >>> 0;
const Sigma0 = (x) => (rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22)) >>> 0;
const Sigma1 = (x) => (rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25)) >>> 0;
const ch = (e, f, g) => (g ^ (e & (f ^ g))) >>> 0;
const maj = (a, b, c) => ((a & b) | (c & (a | b))) >>> 0;

function expandSchedule(w, from = 16) {
  for (let i = from; i < 64; i++) {
    w[i] = add(sigma1(w[i - 2]), w[i - 7], sigma0(w[i - 15]), w[i - 16]);
  }
  return w;
}

/** Standard SHA-256 compression rounds [firstRound, 64) - returns working registers. */
function sha256Rounds(state, w, firstRound = 0) {
  let [a, b, c, d, e, f, g, h] = state;
  const consts = firstRound === 14 ? K_BLOCK : K;
  for (let i = firstRound; i < 64; i++) {
    const t1 = add(h, Sigma1(e), ch(e, f, g), consts[i], w[i]);
    const t2 = add(Sigma0(a), maj(a, b, c));
    h = g;
    g = f;
    f = e;
    e = add(d, t1);
    d = c;
    c = b;
    b = a;
    a = add(t1, t2);
  }
  return [a, b, c, d, e, f, g, h];
}

const be32 = (bytes, off) =>
  (((bytes[off] ?? 0) << 24) | ((bytes[off + 1] ?? 0) << 16) | ((bytes[off + 2] ?? 0) << 8) | (bytes[off + 3] ?? 0)) >>> 0;
const le32 = (bytes, off) =>
  (((bytes[off + 3] ?? 0) << 24) | ((bytes[off + 2] ?? 0) << 16) | ((bytes[off + 1] ?? 0) << 8) | (bytes[off] ?? 0)) >>> 0;

/** Finish the first (secret-prefixed) block using the first 7 data bytes. */
function sha256Init(data7) {
  const w = new Array(64).fill(0);
  w[14] = ((data7[0] << 16) | (data7[1] << 8) | data7[2]) >>> 0; // 3 bytes, top byte folded via adjustments
  w[15] = be32(data7, 3);
  for (let i = 16; i < 30; i++) {
    let v = SHA256_SCHEDULE[i - 16];
    if (i - 2 >= 14) v = add(v, i === 16 ? (sigma1(w[14]) ^ SIGMA1_ADJUSTMENT) >>> 0 : sigma1(w[i - 2]));
    if (i - 7 >= 14) v = add(v, w[i - 7]);
    if (i - 15 >= 14) v = add(v, i === 29 ? (sigma0(w[14]) ^ SIGMA0_ADJUSTMENT) >>> 0 : sigma0(w[i - 15]));
    w[i] = v;
  }
  w[30] = u32(add(sigma1(w[28]), w[23], sigma0(w[15]), w[14], SHA256_ADJUSTMENT) - K[14]);
  expandSchedule(w, 31);
  const working = sha256Rounds(SHA256_STATE, w, 14);
  return SHA256_INITIAL.map((x, i) => add(x, working[i]));
}

/** SHA-256(secret 57-byte prefix || data) as 32 bytes. */
export function sha256Digest(data) {
  if (data.length < 7) throw new RangeError("Token salt must contain at least seven bytes");
  const length = 57 + data.length;
  const rest = data.subarray(7);
  const padLen = (((55 - length) % 64) + 64) % 64;
  const padded = new Uint8Array(rest.length + 1 + padLen + 8);
  padded.set(rest, 0);
  padded[rest.length] = 0x80;
  const bits = BigInt(length) * 8n;
  for (let i = 0; i < 8; i++) padded[padded.length - 1 - i] = Number((bits >> BigInt(8 * i)) & 0xffn);

  let state = sha256Init(data.subarray(0, 7));
  for (let off = 0; off < padded.length; off += 64) {
    const w = new Array(64).fill(0);
    for (let i = 0; i < 16; i++) w[i] = be32(padded, off + 4 * i);
    expandSchedule(w);
    const working = sha256Rounds(state, w);
    state = state.map((x, i) => add(x, working[i]));
  }
  const out = new Uint8Array(32);
  state.forEach((v, i) => {
    out[4 * i] = v >>> 24;
    out[4 * i + 1] = (v >>> 16) & 0xff;
    out[4 * i + 2] = (v >>> 8) & 0xff;
    out[4 * i + 3] = v & 0xff;
  });
  return out;
}

/** LE 32-bit words of (0x00 || data), zero-padded to an even count. */
function wordsLe4Even(data) {
  const buf = new Uint8Array(1 + data.length + ((-(1 + data.length) % 4) + 4) % 4);
  buf.set(data, 1);
  const words = [];
  for (let i = 0; i < buf.length; i += 4) words.push(le32(buf, i));
  if (words.length % 2) words.push(0);
  return words;
}

/** Proprietary CS64 checksum keyed by the first 8 digest bytes -> [lo, hi]. */
export function cs64Digest(data, digest) {
  const keyA = (le32(digest, 0) | 1) >>> 0;
  const keyB = (le32(digest, 4) | 1) >>> 0;
  const factors = [u32(Math.imul(keyA, CS64_FACTOR)), u32(Math.imul(keyB, CS64_FACTOR))];
  const terms = CS64_COEFFICIENTS.map(([coef], i) => u32(Math.imul(coef, factors[i % 2])));
  const offsets = CS64_COEFFICIENTS.map(([, off]) => off);
  wordsLe4Even(data).forEach((word, i) => {
    let term = u32(Math.imul(word, i % 2 === 0 ? keyA : keyB));
    let offset = u32(Math.imul(rol16(word), i % 2 === 0 ? 0xef0569fb : 0xc31713db));
    if (i === 0) {
      term = add(term, u32(Math.imul(CS64_ADJUSTMENT0, factors[0])));
      offset = add(offset, CS64_ADJUSTMENT1);
    }
    terms.push(term);
    offsets.push(offset);
  });
  let swapAcc = 0;
  let swapLast = 0;
  let state = 0;
  let acc = 0;
  for (let i = 0; i < terms.length; i += 2) {
    const x = terms[i];
    const y = terms[i + 1];
    let t = rol16(add(offsets[i], x));
    let q = u32(Math.imul(t, 0x76f50f34));
    swapAcc = add(swapAcc, add(u32(Math.imul(rol16(q), 0xc3efea97)), q));
    t = rol16(add(offsets[i + 1], y));
    q = u32(Math.imul(t, 0x1881ce3c));
    swapLast = add(u32(Math.imul(rol16(q), 0x35bd1ec9)), q);
    swapAcc = add(swapAcc, swapLast);

    state = add(u32(Math.imul(state, keyA)), x);
    state = u32(Math.imul(rol16(state), 0xcf98b111));
    state = u32(Math.imul(rol16(state), 0x87085b9f));
    state = u32(Math.imul(rol16(state), 0x12ceb96d));
    state = u32(Math.imul(rol16(state), 0x257e1d83));
    acc = add(acc, state);
    t = add(u32(Math.imul(state, keyB)), y);
    t = u32(Math.imul(rol16(t), 0xa27416f5));
    t = u32(Math.imul(rol16(t), 0xd38396ff));
    state = u32(Math.imul(rol16(u32(Math.imul(rol16(t), 0x7c932b89))), 0xbfa49f69));
    acc = add(acc, state);
  }
  return [u32(swapLast ^ state), u32(swapAcc ^ acc)];
}

const hexUpper = (bytes) => [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("").toUpperCase();

function le32Bytes(v) {
  return [v & 0xff, (v >>> 8) & 0xff, (v >>> 16) & 0xff, (v >>> 24) & 0xff];
}

const tokenCache = new Map(); // salt string -> token (one entry per 5-minute window)

/** "<16 hex>-<64 hex>" for the given salt string "<ticks><TrustedClientToken>". */
export function tokenHashV2(saltString) {
  const cached = tokenCache.get(saltString);
  if (cached) return cached;
  const data = new TextEncoder().encode(saltString);
  const digest = sha256Digest(data);
  const [lo, hi] = cs64Digest(data, digest);
  const token = `${hexUpper([...le32Bytes(lo), ...le32Bytes(hi)])}-${hexUpper(digest)}`;
  if (tokenCache.size > 8) tokenCache.clear();
  tokenCache.set(saltString, token);
  return token;
}

/**
 * Windows file-time ticks (100 ns) floored to 5 minutes, formatted exactly like
 * Python's f"{ticks:.0f}" for the same float path.
 */
export function windowTicks(unixSeconds) {
  let ticks = unixSeconds + WIN_EPOCH;
  ticks -= ticks % 300;
  return (BigInt(Math.floor(ticks)) * 10_000_000n).toString();
}

/** v2 Sec-MS-GEC for a unix timestamp (seconds, already skew-corrected). */
export function generateSecMsGecV2(unixSeconds) {
  return tokenHashV2(`${windowTicks(unixSeconds)}${TRUSTED_CLIENT_TOKEN}`);
}

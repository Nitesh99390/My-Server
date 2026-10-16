import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DRM, TRUSTED_CLIENT_TOKEN, SEC_MS_GEC_VERSION, splitTextByByteLength, mkssml, parseHeaders, xmlEscape, Communicate } from "../src/edge_tts.js";
import { generateSecMsGecV2, windowTicks } from "../src/drm.js";
import { validateParams, detectScript, voiceCanRead, normalizeText, buildSubtitles } from "../src/validate.js";
import { splitPieces, mapLimit, looksThrottled, classifyError } from "../src/index.js";
import { WSServerHandshakeError, WebSocketError, NoAudioReceived } from "../src/edge_tts.js";

test("Sec-MS-GEC v1 (legacy) matches plain SHA-256", async () => {
  const token = await DRM.generateSecMsGecV1();
  let ticks = Date.now() / 1000 + 11644473600;
  ticks -= ticks % 300;
  const ref = createHash("sha256").update(`${Math.floor(ticks)}0000000${TRUSTED_CLIENT_TOKEN}`).digest("hex").toUpperCase();
  assert.equal(token.length, 64);
  assert.equal(token, ref);
});

// Reference values produced by the Python package edge-tts 7.3.1
// (edge_tts.drm.generate_sec_ms_gec_v2(ts)) - the algorithm Edge 155 uses.
const V2_VECTORS = [
  [1760597400, "E6B2EA85E132E639-1424A40540B5F1D780EFB7F80E58E55F2607F92810E82D39E373D9D3FF119543"],
  [1760597700, "1BED3D4889D806E0-A21100562F9C2C5563C063813F003ECED0091BCE769DA65B57C52810FA614B74"],
  [0, "52BC7A17B50DFEC9-28C4394775C8751CD2F6859C030C06DB1489ACAB3B3A7651831FC26895177ED6"],
  [1234567890, "BA405D1577B8E0CE-61F01E88539977A1A1E969572D82EECFA45FB1E5DEBD122D6ACBAF0054F959E5"],
  [1800000000, "99E54519B490BE5C-E162DB0977FBDCB9B86DD8148BF1BCDC3C0303176CE39852A42B415935C20654"],
  [1760598000.5, "103B83AA16DC44EC-D53A34C539229019C0E3E463311DA764E6225F46E3F9A936FA168131A0B26AFA"],
];

test("Sec-MS-GEC v2 matches edge-tts 7.3.1 reference vectors byte for byte", async () => {
  for (const [ts, expected] of V2_VECTORS) assert.equal(generateSecMsGecV2(ts), expected);
  // same 5-minute window -> same token
  assert.equal(generateSecMsGecV2(1760597400), generateSecMsGecV2(1760597699));
  assert.notEqual(generateSecMsGecV2(1760597400), generateSecMsGecV2(1760597700));
  assert.equal(windowTicks(1760597400), "134050710000000000"); // 5-min floored Windows file time (100 ns ticks)
  const live = await DRM.generateSecMsGec();
  assert.match(live, /^[0-9A-F]{16}-[0-9A-F]{64}$/, "DRM.generateSecMsGec must emit the v2 format");
  assert.match(SEC_MS_GEC_VERSION, /^2-\d+\.\d+\.\d+\.\d+$/, "version header must announce v2");
});

test("splitTextByByteLength respects byte limit, utf-8 and entities", () => {
  const hindi = "यह एक परीक्षण वाक्य है। ".repeat(400);
  const chunks = [...splitTextByByteLength(xmlEscape(hindi), 4096)];
  assert.ok(chunks.length > 1);
  for (const c of chunks) assert.ok(Buffer.byteLength(c) <= 4096, "chunk too big");
  assert.equal(chunks.join(" ").replace(/\s+/g, " ").trim(), hindi.replace(/\s+/g, " ").trim());
  const ent = [...splitTextByByteLength("a".repeat(4090) + " b &amp; c", 4096)];
  assert.ok(ent.every((c) => !/&[^;]*$/.test(c)));
});

test("mkssml + parseHeaders", () => {
  const ssml = mkssml({ voice: "V", rate: "+0%", volume: "+0%", pitch: "+0Hz" }, "hi");
  assert.match(ssml, /<voice name='V'><prosody pitch='\+0Hz' rate='\+0%' volume='\+0%'>hi<\/prosody>/);
  const hdr = "Path:audio\r\nContent-Type:audio/mpeg"; const h = parseHeaders(hdr + "\r\n\r\nxxx", hdr.length);
  assert.equal(h.Path, "audio");
  assert.equal(h["Content-Type"], "audio/mpeg");
});

test("Communicate expands voice name", () => {
  const c = new Communicate("hello", "hi-IN-MadhurNeural");
  assert.equal(c.voice, "Microsoft Server Speech Text to Speech Voice (hi-IN, MadhurNeural)");
  assert.throws(() => new Communicate("x", "bad"));
  assert.throws(() => new Communicate("x", "hi-IN-MadhurNeural", { rate: "fast" }));
});

test("validateParams returns the stable 400 messages", () => {
  const opts = { maxTextLength: 6000, defaultVoice: "hi-IN-MadhurNeural" };
  assert.equal(validateParams({}, opts).error, "Field 'text' must be a string");
  assert.equal(validateParams({ text: "  " }, opts).error, "Field 'text' is required and cannot be empty");
  assert.equal(validateParams({ text: "***" }, opts).error, "Text contains no pronounceable characters (symbols/punctuation only)");
  assert.match(validateParams({ text: "x".repeat(6001) }, opts).error, /Text too long/);
  assert.match(validateParams({ text: "नमस्ते", voice: "en-IN-PrabhatNeural" }, opts).error, /cannot pronounce Devanagari/);
  assert.match(validateParams({ text: "hi", voice: "nope" }, opts).error, /Invalid voice name/);
  assert.match(validateParams({ text: "hi", rate: "+500%" }, opts).error, /Invalid rate/);
  const ok = validateParams({ text: "hello", rate: "20%", pitch: "5Hz", volume: "10%" }, opts);
  assert.equal(ok.error, null);
  assert.deepEqual(ok.params, { text: "hello", voice: "hi-IN-MadhurNeural", rate: "+20%", pitch: "+5Hz", volume: "+10%" });
});

test("detectScript / voiceCanRead / normalizeText", () => {
  assert.equal(detectScript("नमस्ते दुनिया hello"), "hi");
  assert.equal(detectScript("hello"), "en");
  assert.equal(detectScript("123"), null);
  assert.ok(voiceCanRead("hi-IN-MadhurNeural", "hi"));
  assert.ok(!voiceCanRead("en-IN-PrabhatNeural", "hi"));
  assert.equal(normalizeText("a\r\nb\n\n\n\nc   d\x00"), "a\nb\n\nc d");
});

test("buildSubtitles", () => {
  const { srt, vtt } = buildSubtitles([
    { text: "Hello", start_ms: 0, end_ms: 500 },
    { text: "world.", start_ms: 600, end_ms: 1000 },
    { text: "Bye", start_ms: 1200, end_ms: 1500 },
  ]);
  assert.match(srt, /^1\n00:00:00,000 --> 00:00:01,000\nHello world\.\n\n2\n/);
  assert.match(vtt, /^WEBVTT\n\n00:00:00\.000 --> 00:00:01\.000\nHello world\./);
});

test("splitPieces keeps every piece under the byte limit and loses no words", () => {
  const hindi = "यह एक लंबा परीक्षण वाक्य है जिसे कई टुकड़ों में बांटा जाएगा। ".repeat(60);
  const pieces = splitPieces(hindi, 3800);
  assert.ok(pieces.length >= 3, `expected >=3 pieces, got ${pieces.length}`);
  for (const p of pieces) assert.ok(Buffer.byteLength(p) <= 3800, "piece too big");
  assert.equal(pieces.join(" ").replace(/\s+/g, " ").trim(), hindi.replace(/\s+/g, " ").trim());
  assert.deepEqual(splitPieces("short", 3800), ["short"]);
  assert.deepEqual(splitPieces("   ", 3800), ["   "]);
});

test("mapLimit preserves order, honours the limit and propagates the first error", async () => {
  let inFlight = 0;
  let peak = 0;
  const order = await mapLimit([5, 1, 4, 2, 3], 2, async (ms, i) => {
    inFlight += 1;
    peak = Math.max(peak, inFlight);
    await new Promise((r) => setTimeout(r, ms * 5));
    inFlight -= 1;
    return `${i}:${ms}`;
  });
  assert.deepEqual(order, ["0:5", "1:1", "2:4", "3:2", "4:3"]);
  assert.ok(peak <= 2 && peak >= 2, `peak in-flight was ${peak}`);
  assert.deepEqual(await mapLimit([], 4, async () => 1), []);
  await assert.rejects(
    mapLimit([1, 2, 3], 3, async (n) => {
      if (n === 2) throw new Error("boom");
      return n;
    }),
    /boom/,
  );
});

// Live test against Microsoft's endpoint (skipped without network / when EDGE_TTS_LIVE=0).
test("live synthesis via Edge-TTS (needs network)", { skip: process.env.EDGE_TTS_LIVE === "0" }, async () => {
  const c = new Communicate("नमस्ते दुनिया, यह एक परीक्षण है।", "hi-IN-MadhurNeural", { boundary: "WordBoundary" });
  let bytes = 0;
  let words = 0;
  let first = null;
  for await (const ch of c.stream()) {
    if (ch.type === "audio") {
      if (!first) first = ch.data;
      bytes += ch.data.length;
    } else words++;
  }
  assert.ok(bytes > 1000, "got audio");
  assert.ok(words > 0, "got word boundaries");
  assert.ok(first[0] === 0xff && (first[1] & 0xe0) === 0xe0, "MPEG frame sync");
});

test("looksThrottled: only real 403/429 rate limits count, transient network errors do not", () => {
  assert.equal(looksThrottled(new WSServerHandshakeError(403)), true);
  assert.equal(looksThrottled(new WSServerHandshakeError(429)), true);
  assert.equal(looksThrottled(new Error("HTTP 429 Too Many Requests")), true);
  assert.equal(looksThrottled(new Error("microsoft is throttling")), true);
  // these used to bench a whole Worker for minutes:
  assert.equal(looksThrottled(new WSServerHandshakeError(0, "ClientConnectorError: connect timeout")), false);
  assert.equal(looksThrottled(new WSServerHandshakeError(502)), false);
  assert.equal(looksThrottled(new WebSocketError("Connection closed before turn.end (partial audio discarded)")), false);
  assert.equal(looksThrottled(new WebSocketError("No data from Edge for 30s (receive timeout)")), false);
  assert.equal(looksThrottled(new NoAudioReceived()), false);
  assert.equal(looksThrottled(null), false);
  assert.equal(classifyError(new WSServerHandshakeError(403)), "throttle");
  assert.equal(classifyError(new NoAudioReceived()), "transient");
});

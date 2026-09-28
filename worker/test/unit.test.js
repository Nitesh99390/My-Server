import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { DRM, TRUSTED_CLIENT_TOKEN, splitTextByByteLength, mkssml, parseHeaders, xmlEscape, Communicate } from "../src/edge_tts.js";
import { validateParams, detectScript, voiceCanRead, normalizeText, buildSubtitles } from "../src/validate.js";
import { splitPieces, mapLimit } from "../src/index.js";

test("Sec-MS-GEC matches the reference Python algorithm", async () => {
  const token = await DRM.generateSecMsGec();
  let ticks = Date.now() / 1000 + 11644473600;
  ticks -= ticks % 300;
  const ref = createHash("sha256").update(`${Math.floor(ticks)}0000000${TRUSTED_CLIENT_TOKEN}`).digest("hex").toUpperCase();
  assert.equal(token.length, 64);
  assert.equal(token, ref);
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

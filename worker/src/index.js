/**
 * =============================================================================
 *  Edge-TTS Render Server - Cloudflare Worker edition   (v4.1.1-cf)
 * =============================================================================
 *  Drop-in replacement for app.py: the Telegram bot (bot.py) talks to it with
 *  exactly the same endpoints, payloads, headers and status codes.
 *
 *  Endpoints
 *  ---------
 *    GET  /                 -> plain "OK"
 *    GET  /health           -> JSON status, version, limits
 *    GET  /stats            -> JSON runtime statistics (per isolate)
 *    GET  /voices           -> voice catalogue (?locale= | ?lang= | ?gender= | ?q=)
 *    GET  /voices/locales   -> locales with counts
 *    POST /tts              -> {text, voice, rate, pitch, volume} -> audio/mpeg
 *    GET  /tts              -> same via query string
 *    POST /tts/stream       -> audio/mpeg streamed while synthesising
 *    POST /tts/subtitles    -> {duration_ms, srt, vtt, words[], audio_base64?}
 *
 *  Environment (wrangler.toml [vars] / secrets)
 *  --------------------------------------------
 *    API_KEY (secret)  MAX_TEXT_LENGTH  MAX_CONCURRENCY  DEFAULT_VOICE
 *    TTS_RETRIES  ATTEMPT_TIMEOUT  ENABLE_CORS  CACHE_TTL
 * =============================================================================
 */

import { Communicate, listVoices } from "./edge_tts.js";
import { buildSubtitles, validateParams } from "./validate.js";

const VERSION = "4.1.1-cf";
const VOICE_CACHE_TTL_MS = 6 * 3600 * 1000;
const MAX_BODY_BYTES = 2 * 1024 * 1024;
// Date.now() is frozen at 0 in the Worker global scope; initialise lazily on the first request.
let startTime = 0;
function uptimeSeconds() {
  if (!startTime) startTime = Date.now();
  return Math.floor((Date.now() - startTime) / 1000);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
function envInt(env, name, dflt) {
  const n = parseInt(env?.[name] ?? "", 10);
  return Number.isFinite(n) ? n : dflt;
}
function envBool(env, name, dflt) {
  const v = env?.[name];
  if (v === undefined || v === null || v === "") return dflt;
  return ["1", "true", "yes", "on"].includes(String(v).trim().toLowerCase());
}

function config(env) {
  return {
    apiKey: (env?.API_KEY || "").trim(),
    maxTextLength: Math.max(1, envInt(env, "MAX_TEXT_LENGTH", 6000)),
    maxConcurrency: Math.max(1, Math.min(envInt(env, "MAX_CONCURRENCY", 6), 8)),
    defaultVoice: (env?.DEFAULT_VOICE || "hi-IN-MadhurNeural").trim(),
    retries: Math.max(1, envInt(env, "TTS_RETRIES", 3)),
    attemptTimeout: Math.max(10, envInt(env, "ATTEMPT_TIMEOUT", 75)),
    // Whole-request budget; keep below the bot's CHUNK_TIMEOUT (240 s).
    synthTimeout: Math.max(5, envInt(env, "SYNTH_TIMEOUT", 200)),
    cors: envBool(env, "ENABLE_CORS", true),
    cacheTtl: Math.max(0, envInt(env, "CACHE_TTL", 3600)),
  };
}

// ---------------------------------------------------------------------------
// Per-isolate runtime state (statistics + throttle awareness + voice cache)
// ---------------------------------------------------------------------------
const stats = {
  requests: 0,
  success: 0,
  failed: 0,
  rejected: 0,
  cacheHits: 0,
  chars: 0,
  audioBytes: 0,
  totalLatency: 0,
  lastError: null,
  lastErrorAt: null,
  record(ok, chars = 0, nbytes = 0, latency = 0, error = null, cached = false) {
    this.requests += 1;
    if (ok) {
      this.success += 1;
      this.chars += chars;
      this.audioBytes += nbytes;
      this.totalLatency += latency;
      if (cached) this.cacheHits += 1;
    } else {
      this.failed += 1;
      this.lastError = error;
      this.lastErrorAt = Date.now() / 1000;
    }
  },
  snapshot() {
    return {
      requests_total: this.requests,
      success: this.success,
      failed: this.failed,
      rejected: this.rejected,
      cache_hits: this.cacheHits,
      characters_synthesised: this.chars,
      audio_bytes_served: this.audioBytes,
      avg_latency_seconds: this.success ? Math.round((this.totalLatency / this.success) * 1000) / 1000 : 0,
      last_error: this.lastError,
      last_error_at: this.lastErrorAt,
    };
  },
};

const runner = {
  active: 0,
  throttleEvents: 0,
  cooldownUntil: 0,
  lastThrottleAt: 0,
  noteThrottle() {
    this.throttleEvents += 1;
    this.lastThrottleAt = Date.now();
    const pause = Math.min(20, 1.5 * this.throttleEvents);
    this.cooldownUntil = Math.max(this.cooldownUntil, Date.now() + pause * 1000);
    return pause;
  },
  noteSuccess() {
    if (this.throttleEvents && Date.now() > this.cooldownUntil + 60_000) this.throttleEvents = 0;
  },
  get throttled() {
    return Date.now() < this.cooldownUntil || (this.throttleEvents >= 3 && Date.now() - this.lastThrottleAt < 120_000);
  },
};

const voiceCache = { data: null, ts: 0, names: new Set(), promise: null };

async function getVoices() {
  if (voiceCache.data && Date.now() - voiceCache.ts < VOICE_CACHE_TTL_MS) return voiceCache.data;
  if (!voiceCache.promise) {
    voiceCache.promise = (async () => {
      try {
        const raw = await listVoices();
        const voices = raw.map((v) => ({
          name: v.ShortName,
          gender: v.Gender,
          locale: v.Locale,
          friendly_name: v.FriendlyName || "",
          personalities: v.VoiceTag?.VoicePersonalities || [],
          content_categories: v.VoiceTag?.ContentCategories || [],
        }));
        voices.sort((a, b) => `${a.locale || ""}|${a.name || ""}`.localeCompare(`${b.locale || ""}|${b.name || ""}`));
        voiceCache.data = voices;
        voiceCache.ts = Date.now();
        voiceCache.names = new Set(voices.map((v) => v.name).filter(Boolean));
        return voices;
      } catch (err) {
        console.error("Could not fetch voice list:", err?.message || err);
        return voiceCache.data || [];
      } finally {
        voiceCache.promise = null;
      }
    })();
  }
  return voiceCache.promise;
}

/** Known voice names if the catalogue is fresh, otherwise null (never block a request on it). */
function knownVoices() {
  if (!voiceCache.names.size || Date.now() - voiceCache.ts >= VOICE_CACHE_TTL_MS) return null;
  return voiceCache.names;
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------
class ServiceBusy extends Error {
  constructor(retryAfter, maxConcurrency) {
    super(`Server busy: all ${maxConcurrency} synthesis slots in use`);
    this.name = "ServiceBusy";
    this.retryAfter = retryAfter;
  }
}
class Throttled extends Error {
  constructor(retryAfter, detail) {
    super(detail);
    this.name = "Throttled";
    this.retryAfter = retryAfter;
  }
}
class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.name = "HttpError";
    this.status = status;
  }
}

function looksThrottled(err) {
  const text = String(err?.message || err).toLowerCase();
  return (
    text.includes("403") ||
    text.includes("429") ||
    text.includes("too many") ||
    text.includes("throttl") ||
    [
      "WSServerHandshakeError",
      "ClientResponseError",
      "WebSocketError",
      "NoAudioReceived",
      "ServerDisconnectedError",
      "ClientConnectorError",
    ].includes(err?.name)
  );
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function withTimeout(promise, ms, message) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new HttpError(504, message)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// ---------------------------------------------------------------------------
// Core synthesis
// ---------------------------------------------------------------------------
async function synthesizeOnce(params, collectWords) {
  const communicate = new Communicate(params.text, params.voice, {
    rate: params.rate,
    pitch: params.pitch,
    volume: params.volume,
    boundary: "WordBoundary",
  });
  const parts = [];
  let total = 0;
  let durationMs = 0;
  const words = [];
  for await (const chunk of communicate.stream()) {
    if (chunk.type === "audio") {
      parts.push(chunk.data);
      total += chunk.data.length;
    } else if (chunk.type === "WordBoundary" || chunk.type === "SentenceBoundary") {
      const start = chunk.offset / 10_000;
      const dur = chunk.duration / 10_000;
      durationMs = Math.max(durationMs, Math.trunc(start + dur));
      if (collectWords) words.push({ text: chunk.text, start_ms: Math.trunc(start), end_ms: Math.trunc(start + dur) });
    }
  }
  if (!total) {
    const e = new Error("Empty audio stream");
    e.name = "NoAudioReceived";
    throw e;
  }
  const audio = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    audio.set(p, off);
    off += p.length;
  }
  return { audio, durationMs, words };
}

async function synthesize(params, cfg, collectWords = false) {
  let lastErr = null;
  const deadline = Date.now() + (cfg.synthTimeout - 2) * 1000;
  for (let attempt = 1; attempt <= cfg.retries; attempt++) {
    const remaining = deadline - Date.now();
    if (remaining <= 1000) break;
    runner.active += 1;
    let pause = 0;
    try {
      const wait = runner.cooldownUntil - Date.now();
      if (wait > 0) await sleep(Math.min(wait, remaining / 2));
      const result = await withTimeout(
        synthesizeOnce(params, collectWords),
        Math.max(5000, Math.min(cfg.attemptTimeout * 1000, deadline - Date.now())),
        "Synthesis attempt timed out",
      );
      runner.noteSuccess();
      return result;
    } catch (err) {
      lastErr = err;
      pause = looksThrottled(err) ? runner.noteThrottle() : 0;
      console.warn(`Synthesis attempt ${attempt}/${cfg.retries} failed (${err?.name}): ${err?.message}`);
    } finally {
      runner.active -= 1;
    }
    if (attempt < cfg.retries) {
      const backoff = Math.min(30, 1.5 ** attempt + pause + 0.1 + Math.random() * 0.7);
      await sleep(Math.min(backoff * 1000, Math.max(0, deadline - Date.now())));
    }
  }
  const detail = `TTS failed after ${cfg.retries} attempts: ${lastErr?.name || "Error"}: ${lastErr?.message || lastErr}`;
  if (lastErr && looksThrottled(lastErr)) {
    throw new Throttled(Math.min(60, 10 * Math.max(1, runner.throttleEvents)), detail);
  }
  throw new Error(detail);
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
function json(body, status = 200, headers = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

function errorResponse(ctx, message, status = 400, headers = {}) {
  return json({ error: message, status, request_id: ctx.requestId }, status, headers);
}

function timingSafeEqual(a, b) {
  const ea = new TextEncoder().encode(a);
  const eb = new TextEncoder().encode(b);
  if (ea.length !== eb.length) return false;
  let diff = 0;
  for (let i = 0; i < ea.length; i++) diff |= ea[i] ^ eb[i];
  return diff === 0;
}

function checkApiKey(request, url, cfg) {
  if (!cfg.apiKey) return true;
  let supplied = request.headers.get("X-API-Key") || url.searchParams.get("api_key") || "";
  if (!supplied) {
    const auth = request.headers.get("Authorization") || "";
    if (auth.startsWith("Bearer ")) supplied = auth.slice(7);
  }
  return timingSafeEqual(supplied.trim(), cfg.apiKey);
}

async function readPayload(request, url) {
  if (request.method === "POST") {
    const len = parseInt(request.headers.get("Content-Length") || "0", 10);
    if (len > MAX_BODY_BYTES) throw new HttpError(413, "Request body too large");
    const ctype = (request.headers.get("Content-Type") || "").split(";")[0].trim().toLowerCase();
    const raw = await request.text();
    if (raw.length > MAX_BODY_BYTES) throw new HttpError(413, "Request body too large");
    let payload = null;
    if (raw) {
      try {
        payload = JSON.parse(raw);
      } catch {
        payload = null;
      }
    }
    if (payload === null && (ctype === "application/x-www-form-urlencoded" || ctype === "multipart/form-data")) {
      try {
        payload = Object.fromEntries(new URLSearchParams(raw));
      } catch {
        payload = null;
      }
    }
    if (payload === null && ctype === "text/plain" && raw) {
      payload = { text: raw };
    }
    if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return null;
    for (const k of ["voice", "rate", "pitch", "volume"]) {
      if (!(k in payload) && url.searchParams.has(k)) payload[k] = url.searchParams.get(k);
    }
    return payload;
  }
  return Object.fromEntries(url.searchParams.entries());
}

async function cacheKey(params) {
  const raw = new TextEncoder().encode(`${params.voice}|${params.rate}|${params.pitch}|${params.volume}|${params.text}`);
  const digest = await crypto.subtle.digest("SHA-256", raw);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

function cacheRequest(url, key) {
  return new Request(`${url.origin}/__cache/tts/${key}`, { method: "GET" });
}

function sanitizeFilename(name) {
  let f = String(name || "speech.mp3").replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 80) || "speech.mp3";
  if (!f.toLowerCase().endsWith(".mp3")) f += ".mp3";
  return f;
}

function mapSynthError(ctx, err) {
  if (err instanceof HttpError) {
    stats.record(false, 0, 0, 0, err.message);
    return errorResponse(ctx, err.status === 504 ? "Synthesis timed out; please retry" : err.message, err.status);
  }
  if (err instanceof ServiceBusy) {
    stats.rejected += 1;
    return errorResponse(ctx, err.message, 503, { "Retry-After": String(err.retryAfter) });
  }
  if (err instanceof Throttled) {
    console.error(`[${ctx.requestId}] TTS throttled: ${err.message}`);
    stats.record(false, 0, 0, 0, err.message);
    return errorResponse(ctx, err.message, 503, { "Retry-After": String(err.retryAfter) });
  }
  if (err?.message?.startsWith("TTS failed after")) {
    console.error(`[${ctx.requestId}] TTS error: ${err.message}`);
    stats.record(false, 0, 0, 0, err.message);
    return errorResponse(ctx, err.message, 502);
  }
  console.error(`[${ctx.requestId}] Unexpected TTS failure`, err);
  stats.record(false, 0, 0, 0, `${err?.name}: ${err?.message}`);
  return errorResponse(ctx, `Internal error: ${err?.name || "Error"}: ${err?.message || err}`, 500);
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------
function health(cfg) {
  return json({
    status: "ok",
    version: VERSION,
    runtime: "cloudflare-workers",
    uptime_seconds: uptimeSeconds(),
    max_text_length: cfg.maxTextLength,
    max_concurrency: cfg.maxConcurrency,
    active_jobs: runner.active,
    free_slots: Math.max(0, cfg.maxConcurrency - runner.active),
    queued: 0,
    min_start_gap_ms: 0,
    throttled: runner.throttled,
    throttle_events: runner.throttleEvents,
    auth_required: Boolean(cfg.apiKey),
    default_voice: cfg.defaultVoice,
    rate_limit_per_minute: 0,
    cache: { enabled: cfg.cacheTtl > 0, ttl_seconds: cfg.cacheTtl, backend: "cloudflare-cache-api" },
    edge_tts_version: "js-port-of-7.2.8",
  });
}

async function voicesRoute(url) {
  const locale = (url.searchParams.get("locale") || "").trim().toLowerCase();
  const lang = (url.searchParams.get("lang") || "").trim().toLowerCase();
  const gender = (url.searchParams.get("gender") || "").trim().toLowerCase();
  const q = (url.searchParams.get("q") || "").trim().toLowerCase();
  let data = await getVoices();
  if (locale) data = data.filter((v) => (v.locale || "").toLowerCase() === locale);
  else if (lang) data = data.filter((v) => (v.locale || "").toLowerCase().startsWith(lang + "-"));
  if (gender) data = data.filter((v) => (v.gender || "").toLowerCase() === gender);
  if (q) {
    data = data.filter(
      (v) => (v.name || "").toLowerCase().includes(q) || (v.friendly_name || "").toLowerCase().includes(q),
    );
  }
  return json({ count: data.length, voices: data });
}

async function voiceLocalesRoute() {
  const counts = {};
  for (const v of await getVoices()) {
    const k = v.locale || "unknown";
    counts[k] = (counts[k] || 0) + 1;
  }
  const locales = Object.keys(counts)
    .sort()
    .map((k) => ({ locale: k, voices: counts[k] }));
  return json({ count: locales.length, locales });
}

async function ttsRoute(request, url, cfg, ctx) {
  const payload = await readPayload(request, url);
  if (payload === null) return errorResponse(ctx, 'Request body must be JSON: {"text": "..."}');
  const { params, error } = validateParams(payload, {
    maxTextLength: cfg.maxTextLength,
    defaultVoice: cfg.defaultVoice,
    knownVoices: knownVoices(),
  });
  if (error) return errorResponse(ctx, error);

  const started = Date.now();
  let audio;
  let durationMs = 0;
  let hit = false;
  const key = await cacheKey(params);
  const cache = cfg.cacheTtl > 0 && globalThis.caches ? caches.default : null;
  const cReq = cache ? cacheRequest(url, key) : null;

  if (cache) {
    try {
      const cached = await cache.match(cReq);
      if (cached) {
        audio = new Uint8Array(await cached.arrayBuffer());
        durationMs = parseInt(cached.headers.get("X-Duration-Ms") || "0", 10) || 0;
        hit = audio.length > 0;
      }
    } catch (err) {
      console.warn("Cache lookup failed:", err?.message || err);
    }
  }

  if (!hit) {
    try {
      ({ audio, durationMs } = await synthesize(params, cfg));
    } catch (err) {
      return mapSynthError(ctx, err);
    }
    if (!durationMs) durationMs = Math.trunc((params.text.length / 15) * 1000); // ~15 chars/s fallback
    if (cache) {
      const toStore = new Response(audio, {
        headers: {
          "Content-Type": "audio/mpeg",
          "Cache-Control": `public, max-age=${cfg.cacheTtl}`,
          "X-Duration-Ms": String(durationMs),
        },
      });
      const put = cache.put(cReq, toStore).catch((e) => console.warn("Cache put failed:", e?.message || e));
      ctx.waitUntil?.(put);
    }
  }

  const latency = (Date.now() - started) / 1000;
  stats.record(true, params.text.length, audio.length, latency, null, hit);
  console.log(
    `[${ctx.requestId}] TTS ok | voice=${params.voice} chars=${params.text.length} bytes=${audio.length} ` +
      `dur=${(durationMs / 1000).toFixed(1)}s took=${latency.toFixed(2)}s cache=${hit ? "HIT" : "MISS"}`,
  );

  const filename = sanitizeFilename(url.searchParams.get("filename") || payload.filename);
  const download = ["1", "true"].includes(String(payload.download ?? "").toLowerCase());
  return new Response(audio, {
    status: 200,
    headers: {
      "Content-Type": "audio/mpeg",
      "Content-Length": String(audio.length),
      "Content-Disposition": `${download ? "attachment" : "inline"}; filename="${filename}"`,
      "X-Duration-Ms": String(durationMs),
      "X-Char-Count": String(params.text.length),
      "X-Voice": params.voice,
      "X-Cache": hit ? "HIT" : "MISS",
      "Cache-Control": "no-store",
    },
  });
}

async function ttsStreamRoute(request, url, cfg, ctx) {
  const payload = await readPayload(request, url);
  if (payload === null) return errorResponse(ctx, 'Request body must be JSON: {"text": "..."}');
  const { params, error } = validateParams(payload, {
    maxTextLength: cfg.maxTextLength,
    defaultVoice: cfg.defaultVoice,
    knownVoices: knownVoices(),
  });
  if (error) return errorResponse(ctx, error);

  const started = Date.now();
  const communicate = new Communicate(params.text, params.voice, {
    rate: params.rate,
    pitch: params.pitch,
    volume: params.volume,
    boundary: "WordBoundary",
  });
  const iterator = communicate.stream();

  // Fetch the first audio chunk BEFORE sending HTTP 200 so early failures are real errors.
  let first;
  try {
    first = await withTimeout(
      (async () => {
        for (;;) {
          const { value, done } = await iterator.next();
          if (done) return null;
          if (value.type === "audio" && value.data.length) return value.data;
        }
      })(),
      cfg.synthTimeout * 1000,
      "Stream timed out",
    );
  } catch (err) {
    if (err instanceof HttpError) {
      stats.record(false, 0, 0, 0, err.message);
      return errorResponse(ctx, err.message, 504);
    }
    if (looksThrottled(err)) runner.noteThrottle();
    console.error(`[${ctx.requestId}] Stream error: ${err?.name}: ${err?.message}`);
    stats.record(false, 0, 0, 0, String(err?.message || err));
    return errorResponse(ctx, "Upstream synthesis failed before any audio was received", 502);
  }
  if (first === null) {
    stats.record(false, 0, 0, 0, "The upstream service returned no audio");
    return errorResponse(ctx, "Upstream synthesis failed before any audio was received", 502);
  }

  const deadline = started + cfg.synthTimeout * 1000;
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const pump = (async () => {
    let sent = 0;
    let completed = false;
    let failure = "Client disconnected";
    try {
      await writer.write(first);
      sent += first.length;
      for (;;) {
        if (Date.now() > deadline) {
          failure = "Stream timed out";
          throw new Error(failure);
        }
        const { value, done } = await iterator.next();
        if (done) break;
        if (value.type === "audio" && value.data.length) {
          await writer.write(value.data);
          sent += value.data.length;
        }
      }
      completed = true;
      await writer.close();
    } catch (err) {
      failure = String(err?.message || err);
      try {
        await writer.abort(err); // After headers: abort instead of claiming full success.
      } catch {
        /* ignore */
      }
    } finally {
      stats.record(completed, params.text.length, sent, (Date.now() - started) / 1000, completed ? null : failure);
      console.log(`[${ctx.requestId}] Stream finished | bytes=${sent} complete=${completed}`);
    }
  })();
  ctx.waitUntil?.(pump);

  return new Response(readable, {
    status: 200,
    headers: {
      "Content-Type": "audio/mpeg",
      "X-Char-Count": String(params.text.length),
      "X-Voice": params.voice,
      "Cache-Control": "no-store",
      "X-Accel-Buffering": "no",
    },
  });
}

function bytesToBase64(bytes) {
  let binary = "";
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

async function ttsSubtitlesRoute(request, url, cfg, ctx) {
  const payload = await readPayload(request, url);
  if (payload === null) return errorResponse(ctx, 'Request body must be JSON: {"text": "..."}');
  const { params, error } = validateParams(payload, {
    maxTextLength: cfg.maxTextLength,
    defaultVoice: cfg.defaultVoice,
    knownVoices: knownVoices(),
  });
  if (error) return errorResponse(ctx, error);
  const includeAudio = ["1", "true"].includes(
    String(payload.include_audio ?? url.searchParams.get("include_audio") ?? "").toLowerCase(),
  );
  const fmt = String(payload.format ?? url.searchParams.get("format") ?? "json").toLowerCase();
  if (!["json", "srt", "vtt"].includes(fmt)) return errorResponse(ctx, "format must be json, srt or vtt");

  const started = Date.now();
  let result;
  try {
    result = await synthesize(params, cfg, true);
  } catch (err) {
    return mapSynthError(ctx, err);
  }
  const { audio, durationMs, words } = result;
  const { srt, vtt } = buildSubtitles(words);
  stats.record(true, params.text.length, audio.length, (Date.now() - started) / 1000);

  const subHeaders = { "X-Duration-Ms": String(durationMs), "X-Voice": params.voice };
  if (fmt === "srt") {
    return new Response(srt, { headers: { "Content-Type": "text/plain; charset=utf-8", ...subHeaders } });
  }
  if (fmt === "vtt") {
    return new Response(vtt, { headers: { "Content-Type": "text/vtt; charset=utf-8", ...subHeaders } });
  }
  const body = {
    voice: params.voice,
    char_count: params.text.length,
    duration_ms: durationMs,
    word_count: words.length,
    words,
    srt,
    vtt,
  };
  if (includeAudio) {
    body.audio_base64 = bytesToBase64(audio);
    body.audio_mime = "audio/mpeg";
  }
  return json(body);
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------
function withCommonHeaders(resp, ctx, cfg) {
  const headers = new Headers(resp.headers);
  headers.set("X-Request-ID", ctx.requestId);
  headers.set("X-Server-Version", VERSION);
  if (cfg.cors) {
    headers.set("Access-Control-Allow-Origin", "*");
    headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    headers.set("Access-Control-Allow-Headers", "Content-Type, X-API-Key, X-Request-ID, Authorization");
    headers.set("Access-Control-Expose-Headers", "X-Duration-Ms, X-Char-Count, X-Voice, X-Cache, X-Request-ID, Content-Length");
  }
  return new Response(resp.body, { status: resp.status, statusText: resp.statusText, headers });
}

async function route(request, env, execCtx) {
  const cfg = config(env);
  uptimeSeconds();
  const url = new URL(request.url);
  const suppliedId = request.headers.get("X-Request-ID") || "";
  const ctx = {
    requestId: /^[A-Za-z0-9._-]{1,64}$/.test(suppliedId) ? suppliedId : crypto.randomUUID().replace(/-/g, "").slice(0, 16),
    waitUntil: (p) => execCtx?.waitUntil?.(p),
  };

  // Warm the voice catalogue in the background (non-blocking), once per isolate/6h.
  if (!voiceCache.promise && (!voiceCache.data || Date.now() - voiceCache.ts >= VOICE_CACHE_TTL_MS)) {
    ctx.waitUntil(getVoices());
  }

  let resp;
  try {
    if (request.method === "OPTIONS" && cfg.cors) {
      resp = new Response(null, { status: 204 });
    } else {
      const path = url.pathname.replace(/\/+$/, "") || "/";
      const method = request.method;
      const needAuth = () => {
        if (!checkApiKey(request, url, cfg)) {
          stats.rejected += 1;
          return errorResponse(ctx, "Unauthorized: invalid or missing API key", 401);
        }
        return null;
      };

      if (path === "/" && (method === "GET" || method === "HEAD")) {
        resp = new Response(method === "HEAD" ? null : "OK", { status: 200, headers: { "Content-Type": "text/html; charset=utf-8" } });
      } else if (path === "/health" && method === "GET") {
        resp = health(cfg);
      } else if (path === "/stats" && method === "GET") {
        resp = needAuth() || json({ ...stats.snapshot(), uptime_seconds: uptimeSeconds(), active_jobs: runner.active, cache: { enabled: cfg.cacheTtl > 0, backend: "cloudflare-cache-api" } });
      } else if (path === "/voices" && method === "GET") {
        resp = needAuth() || (await voicesRoute(url));
      } else if (path === "/voices/locales" && method === "GET") {
        resp = needAuth() || (await voiceLocalesRoute());
      } else if (path === "/tts" && (method === "POST" || method === "GET")) {
        resp = needAuth() || (await ttsRoute(request, url, cfg, ctx));
      } else if (path === "/tts/stream" && (method === "POST" || method === "GET")) {
        resp = needAuth() || (await ttsStreamRoute(request, url, cfg, ctx));
      } else if (path === "/tts/subtitles" && (method === "POST" || method === "GET")) {
        resp = needAuth() || (await ttsSubtitlesRoute(request, url, cfg, ctx));
      } else if (["/", "/health", "/stats", "/voices", "/voices/locales", "/tts", "/tts/stream", "/tts/subtitles"].includes(path)) {
        resp = errorResponse(ctx, "Method not allowed", 405);
      } else {
        resp = errorResponse(ctx, "Not found", 404);
      }
    }
  } catch (err) {
    if (err instanceof HttpError) {
      resp = errorResponse(ctx, err.message, err.status);
    } else {
      console.error(`[${ctx.requestId}] Unhandled error`, err);
      resp = errorResponse(ctx, "Internal server error", 500);
    }
  }
  return withCommonHeaders(resp, ctx, cfg);
}

export default {
  fetch: route,
};

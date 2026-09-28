/**
 * edge_tts.js - Microsoft Edge "Read Aloud" TTS client for Cloudflare Workers.
 *
 * A faithful port of the wire protocol implemented by the Python `edge-tts`
 * package (v7.x): DRM `Sec-MS-GEC` token, `speech.config` + SSML messages,
 * 2-byte-header binary audio frames, `audio.metadata` word boundaries,
 * 4096-byte text splitting with CBR offset compensation, and the 403 clock-skew
 * retry.  Only Web-platform APIs are used (fetch/WebSocket/crypto), so the same
 * file runs in Workers and in Node >= 20 (tests).
 */

export const TRUSTED_CLIENT_TOKEN = "6A5AA1D4EAFF4E9FB37E23D68491D6F4";
const BASE_URL = "speech.platform.bing.com/consumer/speech/synthesize/readaloud";
const WSS_URL = `wss://${BASE_URL}/edge/v1?TrustedClientToken=${TRUSTED_CLIENT_TOKEN}`;
const VOICE_LIST_URL = `https://${BASE_URL}/voices/list?trustedclienttoken=${TRUSTED_CLIENT_TOKEN}`;

const CHROMIUM_FULL_VERSION = "143.0.3650.75";
const CHROMIUM_MAJOR_VERSION = CHROMIUM_FULL_VERSION.split(".")[0];
export const SEC_MS_GEC_VERSION = `1-${CHROMIUM_FULL_VERSION}`;

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " +
  `Chrome/${CHROMIUM_MAJOR_VERSION}.0.0.0 Safari/537.36 Edg/${CHROMIUM_MAJOR_VERSION}.0.0.0`;

const BASE_HEADERS = {
  "User-Agent": USER_AGENT,
  "Accept-Encoding": "gzip, deflate, br, zstd",
  "Accept-Language": "en-US,en;q=0.9",
};

const WSS_HEADERS = {
  Pragma: "no-cache",
  "Cache-Control": "no-cache",
  Origin: "chrome-extension://jdiccldimpdaibmpdkjnbmckianbfold",
  ...BASE_HEADERS,
};

const VOICE_HEADERS = {
  Authority: "speech.platform.bing.com",
  "Sec-CH-UA": `" Not;A Brand";v="99", "Microsoft Edge";v="${CHROMIUM_MAJOR_VERSION}", "Chromium";v="${CHROMIUM_MAJOR_VERSION}"`,
  "Sec-CH-UA-Mobile": "?0",
  Accept: "*/*",
  "Sec-Fetch-Site": "none",
  "Sec-Fetch-Mode": "cors",
  "Sec-Fetch-Dest": "empty",
  ...BASE_HEADERS,
};

// audio-24khz-48kbitrate-mono-mp3 is 48 kbps CBR; offsets are 100 ns ticks.
const TICKS_PER_SECOND = 10_000_000;
const MP3_BITRATE_BPS = 48_000;
const MAX_SSML_TEXT_BYTES = 4096;
const WIN_EPOCH = 11644473600;

// ---------------------------------------------------------------------------
// Errors (names mirror the Python package so error strings stay compatible
// with the bot's throttle heuristics: "NoAudioReceived", "403", "WebSocketError").
// ---------------------------------------------------------------------------
export class EdgeTTSError extends Error {
  constructor(message) {
    super(message);
    this.name = "EdgeTTSError";
  }
}
export class NoAudioReceived extends EdgeTTSError {
  constructor(message = "No audio was received. Please verify that your parameters are correct.") {
    super(message);
    this.name = "NoAudioReceived";
  }
}
export class UnexpectedResponse extends EdgeTTSError {
  constructor(message) {
    super(message);
    this.name = "UnexpectedResponse";
  }
}
export class UnknownResponse extends EdgeTTSError {
  constructor(message) {
    super(message);
    this.name = "UnknownResponse";
  }
}
export class WebSocketError extends EdgeTTSError {
  constructor(message) {
    super(message);
    this.name = "WebSocketError";
  }
}
export class WSServerHandshakeError extends EdgeTTSError {
  constructor(status, message) {
    super(message || `WebSocket handshake failed with HTTP ${status}`);
    this.name = "WSServerHandshakeError";
    this.status = status;
  }
}

// ---------------------------------------------------------------------------
// DRM: Sec-MS-GEC token with clock-skew correction
// ---------------------------------------------------------------------------
export const DRM = {
  clockSkewSeconds: 0,

  unixTimestamp() {
    return Date.now() / 1000 + DRM.clockSkewSeconds;
  },

  adjustClockSkew(serverDateHeader) {
    if (!serverDateHeader) return false;
    const parsed = Date.parse(serverDateHeader);
    if (Number.isNaN(parsed)) return false;
    DRM.clockSkewSeconds += parsed / 1000 - DRM.unixTimestamp();
    return true;
  },

  /** SHA-256(uppercase hex) of  <windows file time floored to 5 min><token>. */
  async generateSecMsGec() {
    let ticks = DRM.unixTimestamp() + WIN_EPOCH;
    ticks -= ticks % 300;
    // 100-nanosecond intervals.  Value is < 2^53 for the foreseeable future,
    // but use BigInt to get exact integer formatting like Python's f"{ticks:.0f}".
    const fileTime = BigInt(Math.floor(ticks)) * 10_000_000n;
    const data = new TextEncoder().encode(`${fileTime}${TRUSTED_CLIENT_TOKEN}`);
    const digest = await crypto.subtle.digest("SHA-256", data);
    return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("").toUpperCase();
  },

  generateMuid() {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("").toUpperCase();
  },

  headersWithMuid(headers) {
    return { ...headers, Cookie: `muid=${DRM.generateMuid()};` };
  },
};

// ---------------------------------------------------------------------------
// Text helpers (identical behaviour to edge_tts.communicate)
// ---------------------------------------------------------------------------
export function removeIncompatibleCharacters(text) {
  let out = "";
  for (const ch of text) {
    const code = ch.codePointAt(0);
    if ((code >= 0 && code <= 8) || code === 11 || code === 12 || (code >= 14 && code <= 31)) {
      out += " ";
    } else {
      out += ch;
    }
  }
  return out;
}

export function xmlEscape(text) {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export function xmlUnescape(text) {
  return text.replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&");
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const lossyDecoder = new TextDecoder("utf-8");

function lastIndexOfByte(bytes, byte, limit) {
  for (let i = Math.min(limit, bytes.length) - 1; i >= 0; i--) {
    if (bytes[i] === byte) return i;
  }
  return -1;
}

function safeUtf8SplitPoint(bytes, limit) {
  let splitAt = Math.min(limit, bytes.length);
  while (splitAt > 0) {
    try {
      decoder.decode(bytes.subarray(0, splitAt));
      return splitAt;
    } catch {
      splitAt -= 1;
    }
  }
  return splitAt;
}

function adjustForXmlEntity(bytes, splitAt) {
  // Never split inside an unterminated "&...;" entity.
  while (splitAt > 0) {
    const amp = lastIndexOfByte(bytes, 0x26 /* & */, splitAt);
    if (amp < 0) break;
    let terminated = false;
    for (let i = amp; i < splitAt; i++) {
      if (bytes[i] === 0x3b /* ; */) {
        terminated = true;
        break;
      }
    }
    if (terminated) break;
    splitAt = amp;
  }
  return splitAt;
}

function trimBytes(bytes) {
  let s = 0;
  let e = bytes.length;
  const ws = (b) => b === 0x20 || b === 0x0a || b === 0x0d || b === 0x09 || b === 0x0b || b === 0x0c;
  while (s < e && ws(bytes[s])) s++;
  while (e > s && ws(bytes[e - 1])) e--;
  return bytes.subarray(s, e);
}

/** Split escaped text into UTF-8 chunks of at most `byteLength` bytes. */
export function* splitTextByByteLength(text, byteLength = MAX_SSML_TEXT_BYTES) {
  if (byteLength <= 0) throw new RangeError("byteLength must be greater than 0");
  let bytes = typeof text === "string" ? encoder.encode(text) : new Uint8Array(text);
  while (bytes.length > byteLength) {
    let splitAt = lastIndexOfByte(bytes, 0x0a, byteLength);
    if (splitAt < 0) splitAt = lastIndexOfByte(bytes, 0x20, byteLength);
    if (splitAt < 0) splitAt = safeUtf8SplitPoint(bytes, byteLength);
    splitAt = adjustForXmlEntity(bytes, splitAt);
    if (splitAt < 0) throw new RangeError("Maximum byte length is too small or invalid text structure");
    const chunk = trimBytes(bytes.subarray(0, splitAt));
    if (chunk.length) yield lossyDecoder.decode(chunk);
    bytes = bytes.subarray(splitAt > 0 ? splitAt : 1);
  }
  const rest = trimBytes(bytes);
  if (rest.length) yield lossyDecoder.decode(rest);
}

export function mkssml({ voice, rate, volume, pitch }, escapedText) {
  return (
    "<speak version='1.0' xmlns='http://www.w3.org/2001/10/synthesis' xml:lang='en-US'>" +
    `<voice name='${voice}'>` +
    `<prosody pitch='${pitch}' rate='${rate}' volume='${volume}'>` +
    escapedText +
    "</prosody></voice></speak>"
  );
}

const DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

/** JavaScript-style date string as sent by the Edge browser. */
export function dateToString(d = new Date()) {
  const p = (n) => String(n).padStart(2, "0");
  return (
    `${DAYS[d.getUTCDay()]} ${MONTHS[d.getUTCMonth()]} ${p(d.getUTCDate())} ${d.getUTCFullYear()} ` +
    `${p(d.getUTCHours())}:${p(d.getUTCMinutes())}:${p(d.getUTCSeconds())} GMT+0000 (Coordinated Universal Time)`
  );
}

export function connectId() {
  return crypto.randomUUID().replace(/-/g, "");
}

function ssmlHeadersPlusData(requestId, timestamp, ssml) {
  return (
    `X-RequestId:${requestId}\r\n` +
    "Content-Type:application/ssml+xml\r\n" +
    `X-Timestamp:${timestamp}Z\r\n` + // Not a mistake - Microsoft Edge bug.
    "Path:ssml\r\n\r\n" +
    ssml
  );
}

function speechConfigMessage(wordBoundary) {
  const wd = wordBoundary ? "true" : "false";
  const sq = wordBoundary ? "false" : "true";
  return (
    `X-Timestamp:${dateToString()}\r\n` +
    "Content-Type:application/json; charset=utf-8\r\n" +
    "Path:speech.config\r\n\r\n" +
    '{"context":{"synthesis":{"audio":{"metadataoptions":{' +
    `"sentenceBoundaryEnabled":"${sq}","wordBoundaryEnabled":"${wd}"` +
    '},"outputFormat":"audio-24khz-48kbitrate-mono-mp3"}}}}\r\n'
  );
}

/** Parse "Key:Value\r\n..." headers out of a text/binary frame. */
export function parseHeaders(bytesOrText, headerLength) {
  const headers = {};
  const headerText =
    typeof bytesOrText === "string"
      ? bytesOrText.slice(0, headerLength)
      : lossyDecoder.decode(bytesOrText.subarray(0, headerLength));
  for (const line of headerText.split("\r\n")) {
    if (!line) continue;
    const idx = line.indexOf(":");
    if (idx < 0) continue;
    headers[line.slice(0, idx)] = line.slice(idx + 1);
  }
  return headers;
}

// ---------------------------------------------------------------------------
// WebSocket connection (works in Workers and Node >= 22)
// ---------------------------------------------------------------------------
async function openWebSocket(url, headers, connectTimeoutMs) {
  // Cloudflare Workers: fetch() with Upgrade lets us send Cookie/Origin/UA headers.
  const isWorker = typeof WebSocketPair !== "undefined";
  if (isWorker) {
    const httpsUrl = url.replace(/^wss:/, "https:");
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), connectTimeoutMs);
    let resp;
    try {
      resp = await fetch(httpsUrl, {
        headers: { ...headers, Upgrade: "websocket" },
        signal: ctrl.signal,
      });
    } catch (err) {
      throw new WSServerHandshakeError(0, `ClientConnectorError: ${err?.message || err}`);
    } finally {
      clearTimeout(timer);
    }
    const ws = resp.webSocket;
    if (!ws) {
      const err = new WSServerHandshakeError(resp.status, `WebSocket handshake failed with HTTP ${resp.status}`);
      err.headers = resp.headers;
      throw err;
    }
    ws.accept();
    return ws;
  }
  // Node / other runtimes (tests): prefer the `ws` package so the required
  // Origin / User-Agent / Cookie headers can be sent; fall back to the global
  // WebSocket (which cannot set headers and is therefore refused by Microsoft).
  let WSImpl = null;
  try {
    WSImpl = (await import("ws")).default;
  } catch {
    WSImpl = null;
  }
  return new Promise((resolve, reject) => {
    const ws = WSImpl ? new WSImpl(url, { headers, perMessageDeflate: true }) : new WebSocket(url);
    ws.binaryType = "arraybuffer";
    const timer = setTimeout(() => {
      reject(new WSServerHandshakeError(0, "WebSocket connect timeout"));
      try {
        ws.close();
      } catch {
        /* ignore */
      }
    }, connectTimeoutMs);
    const onOpen = () => {
      clearTimeout(timer);
      resolve(ws);
    };
    const onError = (ev) => {
      clearTimeout(timer);
      reject(new WSServerHandshakeError(0, ev?.message || ev?.error?.message || "WebSocket connection error"));
    };
    if (WSImpl) {
      ws.on("unexpected-response", (_req, res) => {
        clearTimeout(timer);
        const err = new WSServerHandshakeError(res.statusCode, `WebSocket handshake failed with HTTP ${res.statusCode}`);
        err.headers = new Headers(Object.entries(res.headers).filter(([, v]) => typeof v === "string"));
        reject(err);
      });
    }
    ws.addEventListener("open", onOpen);
    ws.addEventListener("error", onError);
  });
}

/** Async iterator over the websocket's messages; ends when the socket closes. */
function messageIterator(ws) {
  const queue = [];
  let waiter = null;
  let done = false;
  let error = null;

  const push = (item) => {
    queue.push(item);
    if (waiter) {
      const w = waiter;
      waiter = null;
      w();
    }
  };
  ws.addEventListener("message", (ev) => push({ data: ev.data }));
  ws.addEventListener("close", (ev) => {
    done = true;
    if (ev && ev.code && ev.code !== 1000 && ev.code !== 1005) {
      error = new WebSocketError(`WebSocket closed with code ${ev.code}${ev.reason ? `: ${ev.reason}` : ""}`);
    }
    push(null);
  });
  ws.addEventListener("error", (ev) => {
    error = new WebSocketError(ev?.message || "WebSocket error");
    done = true;
    push(null);
  });

  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    async next() {
      while (queue.length === 0) {
        if (done) {
          if (error) throw error;
          return { done: true, value: undefined };
        }
        await new Promise((r) => {
          waiter = r;
        });
      }
      const item = queue.shift();
      if (item === null) {
        if (error) throw error;
        return { done: true, value: undefined };
      }
      return { done: false, value: item.data };
    },
    async return() {
      done = true;
      return { done: true, value: undefined };
    },
  };
}

// ---------------------------------------------------------------------------
// Communicate
// ---------------------------------------------------------------------------
const VOICE_RE = /^([a-z]{2,})-([A-Z]{2,})-(.+Neural)$/;
const RATE_RE = /^[+-]\d+%$/;
const VOLUME_RE = /^[+-]\d+%$/;
const PITCH_RE = /^[+-]\d+Hz$/;

export class Communicate {
  /**
   * @param {string} text
   * @param {string} voice
   * @param {{rate?:string, volume?:string, pitch?:string, boundary?:"WordBoundary"|"SentenceBoundary",
   *          connectTimeout?:number, receiveTimeout?:number}} [opts]
   */
  constructor(text, voice = "en-US-EmmaMultilingualNeural", opts = {}) {
    if (typeof text !== "string") throw new TypeError("text must be str");
    const m = VOICE_RE.exec(voice);
    if (!m) throw new Error(`Invalid voice '${voice}'.`);
    // Accept the short "lang-REGION-NameNeural" form and expand like the Python lib.
    let lang = m[1];
    let region = m[2];
    let name = m[3];
    if (name.includes("-")) {
      region = `${region}-${name.slice(0, name.indexOf("-"))}`;
      name = name.slice(name.indexOf("-") + 1);
    }
    this.voice = `Microsoft Server Speech Text to Speech Voice (${lang}-${region}, ${name})`;
    this.rate = opts.rate ?? "+0%";
    this.volume = opts.volume ?? "+0%";
    this.pitch = opts.pitch ?? "+0Hz";
    if (!RATE_RE.test(this.rate)) throw new Error(`Invalid rate '${this.rate}'.`);
    if (!VOLUME_RE.test(this.volume)) throw new Error(`Invalid volume '${this.volume}'.`);
    if (!PITCH_RE.test(this.pitch)) throw new Error(`Invalid pitch '${this.pitch}'.`);
    this.boundary = opts.boundary ?? "SentenceBoundary";
    this.connectTimeoutMs = (opts.connectTimeout ?? 10) * 1000;
    this.receiveTimeoutMs = (opts.receiveTimeout ?? 60) * 1000;
    this.texts = [...splitTextByByteLength(xmlEscape(removeIncompatibleCharacters(text)), MAX_SSML_TEXT_BYTES)];
    this.state = {
      offsetCompensation: 0,
      lastDurationOffset: 0,
      chunkAudioBytes: 0,
      cumulativeAudioBytes: 0,
      streamWasCalled: false,
      aborted: false,
      ws: null,
    };
  }

  /** Close the live websocket (if any) and stop the stream - used on timeouts. */
  abort() {
    this.state.aborted = true;
    const ws = this.state.ws;
    if (ws) {
      try {
        ws.close(1000, "aborted");
      } catch {
        /* ignore */
      }
    }
  }

  #parseMetadata(text) {
    const parsed = JSON.parse(text);
    for (const meta of parsed.Metadata || []) {
      const type = meta.Type;
      if (type === "WordBoundary" || type === "SentenceBoundary") {
        return {
          type,
          offset: meta.Data.Offset + this.state.offsetCompensation,
          duration: meta.Data.Duration,
          text: xmlUnescape(meta.Data.text.Text),
        };
      }
      if (type === "SessionEnd") continue;
      throw new UnknownResponse(`Unknown metadata type: ${type}`);
    }
    throw new UnexpectedResponse("No WordBoundary metadata found");
  }

  #compensateOffset() {
    this.state.cumulativeAudioBytes += this.state.chunkAudioBytes;
    this.state.offsetCompensation = Math.floor(
      (this.state.cumulativeAudioBytes * 8 * TICKS_PER_SECOND) / MP3_BITRATE_BPS,
    );
    this.state.chunkAudioBytes = 0;
  }

  async *#streamOne(partialText) {
    const url =
      `${WSS_URL}&ConnectionId=${connectId()}` +
      `&Sec-MS-GEC=${await DRM.generateSecMsGec()}` +
      `&Sec-MS-GEC-Version=${SEC_MS_GEC_VERSION}`;
    if (this.state.aborted) throw new WebSocketError("aborted");
    const ws = await openWebSocket(url, DRM.headersWithMuid(WSS_HEADERS), this.connectTimeoutMs);
    this.state.ws = ws;
    if (this.state.aborted) {
      try {
        ws.close(1000, "aborted");
      } catch {
        /* ignore */
      }
      throw new WebSocketError("aborted");
    }
    let audioWasReceived = false;
    let receiveTimer = null;
    let receiveTimedOut = false;
    const messages = messageIterator(ws);

    const armReceiveTimeout = () => {
      if (receiveTimer) clearTimeout(receiveTimer);
      receiveTimer = setTimeout(() => {
        receiveTimedOut = true;
        try {
          ws.close(1000, "receive timeout");
        } catch {
          /* ignore */
        }
      }, this.receiveTimeoutMs);
    };

    try {
      ws.send(speechConfigMessage(this.boundary === "WordBoundary"));
      ws.send(
        ssmlHeadersPlusData(
          connectId(),
          dateToString(),
          mkssml({ voice: this.voice, rate: this.rate, volume: this.volume, pitch: this.pitch }, partialText),
        ),
      );
      armReceiveTimeout();

      let turnEnded = false;
      for await (const data of messages) {
        armReceiveTimeout();
        if (typeof data === "string") {
          const sep = data.indexOf("\r\n\r\n");
          const headers = parseHeaders(data, sep);
          const body = data.slice(sep + 4);
          const path = headers.Path;
          if (path === "audio.metadata") {
            const meta = this.#parseMetadata(body);
            this.state.lastDurationOffset = meta.offset + meta.duration;
            yield meta;
          } else if (path === "turn.end") {
            this.#compensateOffset();
            turnEnded = true;
            break;
          } else if (path !== "response" && path !== "turn.start") {
            throw new UnknownResponse("Unknown path received");
          }
        } else {
          const bytes = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer ?? data);
          if (bytes.length < 2) {
            throw new UnexpectedResponse("We received a binary message, but it is missing the header length.");
          }
          const headerLength = (bytes[0] << 8) | bytes[1];
          if (headerLength > bytes.length) {
            throw new UnexpectedResponse("The header length is greater than the length of the data.");
          }
          const headers = parseHeaders(bytes.subarray(2), headerLength);
          const audio = bytes.subarray(2 + headerLength);
          if (headers.Path !== "audio") {
            throw new UnexpectedResponse("Received binary message, but the path is not audio.");
          }
          const ctype = headers["Content-Type"];
          if (ctype !== undefined && ctype !== "audio/mpeg") {
            throw new UnexpectedResponse("Received binary message, but with an unexpected Content-Type.");
          }
          if (ctype === undefined) {
            if (audio.length === 0) continue;
            throw new UnexpectedResponse("Received binary message with no Content-Type, but with data.");
          }
          if (audio.length === 0) {
            throw new UnexpectedResponse("Received binary message, but it is missing the audio data.");
          }
          audioWasReceived = true;
          this.state.chunkAudioBytes += audio.length;
          yield { type: "audio", data: audio.slice() };
        }
      }
      if (receiveTimedOut) throw new WebSocketError("No data from Edge for " + this.receiveTimeoutMs / 1000 + "s (receive timeout)");
      if (this.state.aborted) throw new WebSocketError("aborted");
      if (!turnEnded && !audioWasReceived) {
        throw new NoAudioReceived();
      }
      if (!turnEnded) throw new WebSocketError("Connection closed before turn.end (partial audio discarded)");
    } finally {
      if (receiveTimer) clearTimeout(receiveTimer);
      this.state.ws = null;
      try {
        ws.close(1000, "done");
      } catch {
        /* ignore */
      }
    }
    if (!audioWasReceived) throw new NoAudioReceived();
  }

  /** Async generator of {type:"audio",data} | {type:"WordBoundary"|"SentenceBoundary",offset,duration,text}. */
  async *stream() {
    if (this.state.streamWasCalled) throw new Error("stream can only be called once.");
    this.state.streamWasCalled = true;
    for (const partial of this.texts) {
      this.state.chunkAudioBytes = 0;
      try {
        yield* this.#streamOne(partial);
      } catch (err) {
        if (!(err instanceof WSServerHandshakeError) || err.status !== 403) throw err;
        // Clock skew: re-sync from the server's Date header and retry once.
        DRM.adjustClockSkew(err.headers?.get?.("Date"));
        this.state.chunkAudioBytes = 0;
        yield* this.#streamOne(partial);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Voice list
// ---------------------------------------------------------------------------
export async function listVoices() {
  const fetchOnce = async () =>
    fetch(`${VOICE_LIST_URL}&Sec-MS-GEC=${await DRM.generateSecMsGec()}&Sec-MS-GEC-Version=${SEC_MS_GEC_VERSION}`, {
      headers: DRM.headersWithMuid(VOICE_HEADERS),
    });
  let resp = await fetchOnce();
  if (resp.status === 403) {
    DRM.adjustClockSkew(resp.headers.get("Date"));
    resp = await fetchOnce();
  }
  if (!resp.ok) throw new Error(`Voice list request failed with HTTP ${resp.status}`);
  const data = await resp.json();
  for (const v of data) {
    v.VoiceTag ??= {};
    v.VoiceTag.ContentCategories ??= [];
    v.VoiceTag.VoicePersonalities ??= [];
  }
  return data;
}

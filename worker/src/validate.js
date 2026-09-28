/**
 * validate.js - request validation, text normalisation and script detection.
 * Port of the corresponding helpers in app.py (v4.1.1) so the Worker returns
 * the exact same 400 messages the bot already understands.
 */

export const RATE_RE = /^[+-]\d{1,3}%$/;
export const PITCH_RE = /^[+-]\d{1,3}Hz$/;
export const VOLUME_RE = /^[+-]\d{1,3}%$/;
export const VOICE_RE = /^[a-z]{2,3}-[A-Za-z]{2,4}(-[A-Za-z]+)?-[A-Za-z0-9]+Neural$/;
// eslint-disable-next-line no-control-regex
const CONTROL_CHARS_RE = /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g;
const MULTI_SPACE_RE = /[ \t]{2,}/g;
const MULTI_NEWLINE_RE = /\n{3,}/g;
const WORD_CHAR_RE = /[\p{L}\p{N}_]/u;

export function normalizeText(text) {
  return text
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .replace(CONTROL_CHARS_RE, "")
    .replace(MULTI_SPACE_RE, " ")
    .replace(MULTI_NEWLINE_RE, "\n\n")
    .trim();
}

// Indic unicode blocks -> language code.  English neural voices return an
// EMPTY stream for these scripts (NoAudioReceived), indistinguishable from IP
// throttling - so we reject such requests with a clear 400 instead.
const SCRIPT_BLOCKS = [
  ["hi", 0x0900, 0x097f],
  ["bn", 0x0980, 0x09ff],
  ["pa", 0x0a00, 0x0a7f],
  ["gu", 0x0a80, 0x0aff],
  ["or", 0x0b00, 0x0b7f],
  ["ta", 0x0b80, 0x0bff],
  ["te", 0x0c00, 0x0c7f],
  ["kn", 0x0c80, 0x0cff],
  ["ml", 0x0d00, 0x0d7f],
  ["ur", 0x0600, 0x06ff],
  ["ur", 0x0750, 0x077f],
];
const SCRIPT_VOICE_LANGS = {
  hi: ["hi", "mr", "ne"],
  bn: ["bn"],
  gu: ["gu"],
  ta: ["ta"],
  te: ["te"],
  kn: ["kn"],
  ml: ["ml"],
  ur: ["ur"],
  pa: ["hi", "mr", "ne"],
  or: ["hi", "mr", "ne"],
};
export const SCRIPT_NAMES = {
  hi: "Devanagari (Hindi)",
  bn: "Bengali",
  gu: "Gujarati",
  ta: "Tamil",
  te: "Telugu",
  kn: "Kannada",
  ml: "Malayalam",
  ur: "Urdu",
  pa: "Punjabi",
  or: "Odia",
  en: "Latin",
};
const LETTER_RE = /\p{L}/u;

/** Dominant script language of `text` ("hi", "en", ...); null if no letters. */
export function detectScript(text, sample = 20_000) {
  const counts = {};
  let i = 0;
  for (const ch of text) {
    if (i++ >= sample) break;
    if (!LETTER_RE.test(ch)) continue;
    const o = ch.codePointAt(0);
    if (o < 0x0250) {
      counts.en = (counts.en || 0) + 1;
      continue;
    }
    for (const [lang, lo, hi] of SCRIPT_BLOCKS) {
      if (o >= lo && o <= hi) {
        counts[lang] = (counts[lang] || 0) + 1;
        break;
      }
    }
  }
  const entries = Object.entries(counts);
  if (!entries.length) return null;
  entries.sort((a, b) => b[1] - a[1]);
  return entries[0][0];
}

export function voiceCanRead(voice, script) {
  if (script === null || script === undefined || script === "en") return true;
  const allowed = SCRIPT_VOICE_LANGS[script];
  if (!allowed) return true;
  return allowed.includes(voice.split("-", 1)[0].toLowerCase());
}

function str(v) {
  return v === undefined || v === null ? "" : String(v);
}

/**
 * Validate a request payload.
 * @returns {{params: object|null, error: string|null}}
 */
export function validateParams(params, { maxTextLength, defaultVoice, knownVoices = null }) {
  if (typeof params.text !== "string") return { params: null, error: "Field 'text' must be a string" };
  const text = normalizeText(params.text);
  if (!text) return { params: null, error: "Field 'text' is required and cannot be empty" };
  if (text.length > maxTextLength) {
    return { params: null, error: `Text too long (${text.length} chars). Maximum is ${maxTextLength}` };
  }
  if (!WORD_CHAR_RE.test(text)) {
    return { params: null, error: "Text contains no pronounceable characters (symbols/punctuation only)" };
  }

  const voice = (str(params.voice) || defaultVoice).trim();
  let rate = (str(params.rate) || "+0%").trim().replace(/ /g, "");
  let pitch = (str(params.pitch) || "+0Hz").trim().replace(/ /g, "");
  let volume = (str(params.volume) || "+0%").trim().replace(/ /g, "");

  // Be lenient: "20%" -> "+20%", "10Hz" -> "+10Hz"
  if (rate && !"+-".includes(rate[0])) rate = "+" + rate;
  if (pitch && !"+-".includes(pitch[0])) pitch = "+" + pitch;
  if (volume && !"+-".includes(volume[0])) volume = "+" + volume;

  if (!VOICE_RE.test(voice)) {
    return { params: null, error: `Invalid voice name: '${voice}' (example: hi-IN-MadhurNeural)` };
  }
  if (knownVoices && knownVoices.size && !knownVoices.has(voice)) {
    return { params: null, error: `Unknown voice: '${voice}'. Use GET /voices to list available voices` };
  }
  const script = detectScript(text);
  if (!voiceCanRead(voice, script)) {
    return {
      params: null,
      error:
        `Voice '${voice}' cannot pronounce ${SCRIPT_NAMES[script] || script} text ` +
        "(Edge-TTS returns silence). Use a matching voice, e.g. hi-IN-MadhurNeural",
    };
  }
  const inRange = (s, lo, hi) => {
    const n = parseInt(s, 10);
    return Number.isFinite(n) && n >= lo && n <= hi;
  };
  if (!RATE_RE.test(rate) || !inRange(rate.slice(0, -1), -99, 200)) {
    return { params: null, error: `Invalid rate: '${rate}' (example: +20% or -10%)` };
  }
  if (!PITCH_RE.test(pitch) || !inRange(pitch.slice(0, -2), -100, 100)) {
    return { params: null, error: `Invalid pitch: '${pitch}' (example: +10Hz or -5Hz)` };
  }
  if (!VOLUME_RE.test(volume) || !inRange(volume.slice(0, -1), -100, 100)) {
    return { params: null, error: `Invalid volume: '${volume}' (example: +30% or -10%)` };
  }
  return { params: { text, voice, rate, pitch, volume }, error: null };
}

// ---------------------------------------------------------------------------
// Subtitles
// ---------------------------------------------------------------------------
function fmtSrtTime(ms) {
  const h = Math.floor(ms / 3_600_000);
  let rem = ms % 3_600_000;
  const m = Math.floor(rem / 60_000);
  rem %= 60_000;
  const s = Math.floor(rem / 1000);
  const ms2 = rem % 1000;
  const p = (n, w = 2) => String(n).padStart(w, "0");
  return `${p(h)}:${p(m)}:${p(s)},${p(ms2, 3)}`;
}
const fmtVttTime = (ms) => fmtSrtTime(ms).replace(",", ".");

/** Group word boundaries into readable cues; returns {srt, vtt}. */
export function buildSubtitles(words, maxWords = 8, maxMs = 4000) {
  const cues = [];
  let buf = [];
  let start = 0;
  for (const w of words) {
    if (!buf.length) start = w.start_ms;
    buf.push(w.text);
    const end = w.end_ms;
    const endsSentence = /[.!?।؟。]$/.test(w.text.trimEnd());
    if (buf.length >= maxWords || end - start >= maxMs || endsSentence) {
      cues.push([start, end, buf.join(" ")]);
      buf = [];
    }
  }
  if (buf.length && words.length) cues.push([start, words[words.length - 1].end_ms, buf.join(" ")]);

  const srt = [];
  const vtt = ["WEBVTT", ""];
  cues.forEach(([s, e, t], i) => {
    srt.push(String(i + 1), `${fmtSrtTime(s)} --> ${fmtSrtTime(e)}`, t, "");
    vtt.push(`${fmtVttTime(s)} --> ${fmtVttTime(e)}`, t, "");
  });
  return { srt: srt.join("\n").trim() + "\n", vtt: vtt.join("\n").trim() + "\n" };
}

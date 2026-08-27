// Relay server for the two-device pairing prototype, PLUS a small server-side
// translation proxy.
//
// WebSocket relay (unchanged from before):
// - Lets one phone create a session (gets a 6-character code)
// - Lets a second phone join that session using the code
// - Relays every message between the two phones instantly (no polling delay)
// - Tracks who's online so both sides can show a live connection status
//
// NEW — HTTP translation proxy (POST /translate):
// - The browser used to call an unofficial, undocumented Google endpoint directly.
// That's a legal gray area — it's not Google's licensed API, just a reverse-engineered
// endpoint the Google Translate website itself uses, so it isn't something a "fully
// legal" site should depend on. It's also been known to break/rate-limit without notice,
// and some users are behind filtering that blocks Google domains directly, even though
// this server (hosted on Render, outside Iran) is reachable fine.
// - Now the browser calls THIS server instead. This server holds API keys (never
// exposed to the browser) and translates using, in order: Claude (Anthropic API) →
// DeepL → LibreTranslate. Claude and DeepL are optional (only used if you set an API
// key); LibreTranslate needs no key or account at all — it's free, open-source
// software, and the public instance used here is offered specifically for this kind
// of use, so there's no terms-of-service ambiguity like with the old Google endpoint.
//
// What this deliberately does NOT do (kept simple on purpose for a prototype):
// - No persistence — if the server restarts, all sessions are gone (fine for a demo,
// not fine for a real product; a real product would use Redis or a database)
// - No auth beyond the session code itself
// - Only two participants per session (a third joiner replaces the guest slot)
// - No caching/rate-limiting on /translate — fine for demo traffic, but add some
// (e.g. a per-IP limiter) before pointing serious traffic at it
//
// Deploy this on Render.com (free tier, no credit card): see DEPLOY.md in this folder.
// ANTHROPIC_API_KEY and DEEPL_API_KEY are both optional — set either (or neither) in
// Render's dashboard. LibreTranslate always works as the free, no-key baseline.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY || '';
// Haiku is intentionally used here (not Sonnet/Opus): translating one short spoken
// sentence at a time needs to be fast and cheap far more than it needs frontier
// reasoning, and Haiku 4.5 is Anthropic's current model for exactly that kind of task.
const CLAUDE_MODEL = 'claude-haiku-4-5-20251001';

const DEEPL_API_KEY = process.env.DEEPL_API_KEY || '';
// DeepL free-tier keys end with ":fx" and must be called at api-free.deepl.com;
// paid keys use api.deepl.com. Detected automatically so you don't have to configure it.
const DEEPL_BASE = DEEPL_API_KEY.endsWith(':fx')
  ? 'https://api-free.deepl.com'
  : 'https://api.deepl.com';

// sessions: Map<code, { host: ws|null, guest: ws|null, hostLang, guestLang }>
const sessions = new Map();

function makeCode() {
  const chars = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no ambiguous chars (0/O, 1/I/L)
  let c = '';
  for (let i = 0; i < 6; i++) c += chars[Math.floor(Math.random() * chars.length)];
  return c;
}

function send(ws, obj) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function otherSide(session, role) {
  return role === 'host' ? session.guest : session.host;
}

function broadcastPresence(code) {
  const s = sessions.get(code);
  if (!s) return;
  send(s.host, { type: 'presence', partnerOnline: !!(s.guest && s.guest.readyState === s.guest.OPEN) });
  send(s.guest, { type: 'presence', partnerOnline: !!(s.host && s.host.readyState === s.host.OPEN) });
}

// --- translation proxy helpers -------------------------------------------------

// Full language names for the prompt sent to Claude — plain names read more reliably
// in a system prompt than raw ISO codes. Mirrors the LANGS list in index.html.
const LANG_NAMES = {
  fa: 'Persian (Farsi)', ar: 'Arabic', en: 'English', tr: 'Turkish', fr: 'French',
  de: 'German', es: 'Spanish', it: 'Italian', ru: 'Russian', ja: 'Japanese',
  ko: 'Korean', hi: 'Hindi', ur: 'Urdu', pt: 'Portuguese', nl: 'Dutch',
  sv: 'Swedish', pl: 'Polish', uk: 'Ukrainian', id: 'Indonesian', vi: 'Vietnamese',
  th: 'Thai', he: 'Hebrew', el: 'Greek', ro: 'Romanian', bn: 'Bengali', ms: 'Malay',
};
function langName(code) {
  return LANG_NAMES[code] || code;
}

async function translateWithClaude(text, fromCode, toCode) {
  if (!ANTHROPIC_API_KEY) throw new Error('no-anthropic-key');
  const fromName = langName(fromCode);
  const toName = langName(toCode);
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: CLAUDE_MODEL,
      max_tokens: 500,
      system: 'You are the translation engine inside a live speech-translation app used for real spoken conversation. ' +
        'Translate the user\'s message from ' + fromName + ' to ' + toName + '. ' +
        'Preserve tone and meaning naturally, the way a fluent bilingual interpreter would speak it out loud. ' +
        'Reply with ONLY the translated text — no quotation marks, no notes, no alternate options, no explanations.',
      messages: [{ role: 'user', content: text }],
    }),
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error('claude-http-' + resp.status + (body ? ': ' + body.slice(0, 200) : ''));
  }
  const data = await resp.json();
  const block = data && data.content && data.content.find((b) => b.type === 'text');
  const translated = block && block.text && block.text.trim();
  if (!translated) throw new Error('claude-bad-response');
  return translated;
}

// Our app's 2-letter codes map 1:1 onto DeepL's codes (just uppercase), with one
// wrinkle: DeepL prefers a regional variant for English *targets* ("EN-US" instead
// of plain "EN") for slightly better quality. Every other language in this app's
// list (fa, ar, tr, fr, de, es, it, ru, ja, ko, hi, ur, pt, nl, sv, pl, uk, id, vi,
// th, he, el, ro, bn, ms) is supported by DeepL as a plain uppercase code.
function toDeepLTarget(code) {
  if (code === 'en') return 'EN-US';
  return code.toUpperCase();
}
function toDeepLSource(code) {
  return code.toUpperCase();
}

async function translateWithDeepL(text, fromCode, toCode) {
  if (!DEEPL_API_KEY) throw new Error('no-deepl-key');
  const resp = await fetch(DEEPL_BASE + '/v2/translate', {
    method: 'POST',
    headers: {
      'Authorization': 'DeepL-Auth-Key ' + DEEPL_API_KEY,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      text: [text],
      source_lang: toDeepLSource(fromCode),
      target_lang: toDeepLTarget(toCode),
    }),
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => '');
    throw new Error('deepl-http-' + resp.status + (body ? ': ' + body.slice(0, 200) : ''));
  }
  const data = await resp.json();
  const translated = data && data.translations && data.translations[0] && data.translations[0].text;
  if (!translated) throw new Error('deepl-bad-response');
  return translated;
}

// The always-available, no-key fallback: a public LibreTranslate instance.
async function translateWithLibreTranslate(text, fromCode, toCode) {
  // translate.terraprint.co is a community-run public instance of LibreTranslate
  // (fully open-source, AGPL-licensed, no ties to any proprietary provider). It's
  // designed and offered specifically for public use like this — no key, no account,
  // no terms being bent, unlike calling an undocumented Google endpoint.
  const url = 'https://translate.terraprint.co/translate';
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10000);
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ q: text, source: fromCode, target: toCode, format: 'text' }),
      signal: controller.signal,
    });
    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      throw new Error('libretranslate-http-' + resp.status + (body ? ': ' + body.slice(0, 200) : ''));
    }
    const data = await resp.json();
    if (data && typeof data.translatedText === 'string') {
      return data.translatedText;
    }
    throw new Error('libretranslate-bad-response');
  } finally {
    clearTimeout(timer);
  }
}

async function translateText(text, fromCode, toCode) {
  // Order: Claude (if configured) → DeepL (if configured) → LibreTranslate (always
  // available, no key, fully open-source). LibreTranslate covers every language in
  // this app except Vietnamese (vi) — if you need Vietnamese reliably, add an
  // ANTHROPIC_API_KEY or DEEPL_API_KEY later and it'll be picked up automatically.
  try {
    const translated = await translateWithClaude(text, fromCode, toCode);
    return { translated, engine: 'claude' };
  } catch (claudeErr) {
    try {
      const translated = await translateWithDeepL(text, fromCode, toCode);
      return { translated, engine: 'deepl-fallback', claudeError: claudeErr.message };
    } catch (deeplErr) {
      try {
        const translated = await translateWithLibreTranslate(text, fromCode, toCode);
        return { translated, engine: 'libretranslate-fallback', claudeError: claudeErr.message, deeplError: deeplErr.message };
      } catch (libreErr) {
        throw new Error('all engines failed — claude: ' + claudeErr.message + ' | deepl: ' + deeplErr.message + ' | libretranslate: ' + libreErr.message);
      }
    }
  }
}

function readJsonBody(req, maxBytes) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > maxBytes) { reject(new Error('payload-too-large')); req.destroy(); return; }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch (e) { reject(new Error('invalid-json')); }
    });
    req.on('error', reject);
  });
}

// --- HTTP server: health check + /translate proxy -----------------------------

const server = http.createServer(async (req, res) => {
  // CORS: this server is called from a browser on a different origin (the static
  // site), so every response needs these headers, including the OPTIONS preflight.
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  if (req.method === 'POST' && req.url === '/translate') {
    try {
      const body = await readJsonBody(req, 20000); // 20KB is plenty for a spoken sentence
      const { text, source, target } = body;
      if (!text || !source || !target) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'text, source و target لازم است' }));
        return;
      }
      const result = await translateText(String(text), String(source), String(target));
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message || 'ترجمه انجام نشد' }));
    }
    return;
  }

  // simple health check endpoint — also what keeps Render's free tier happy to report "up"
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  const status = [
    ANTHROPIC_API_KEY ? 'Claude configured' : 'Claude NOT configured',
    DEEPL_API_KEY ? 'DeepL configured' : 'DeepL NOT configured',
    'LibreTranslate fallback always available (open-source, no key needed)',
  ].join(', ');
  res.end('translation relay server is running (' + status + ')');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  ws.role = null;
  ws.code = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    if (msg.type === 'create') {
      // host creates a new session
      const code = makeCode();
      sessions.set(code, { host: ws, guest: null, hostLang: msg.lang, guestLang: null });
      ws.role = 'host';
      ws.code = code;
      send(ws, { type: 'created', code });
      return;
    }

    if (msg.type === 'join') {
      const s = sessions.get(msg.code);
      if (!s) { send(ws, { type: 'error', message: 'جلسه‌ای با این کد پیدا نشد' }); return; }
      s.guest = ws;
      s.guestLang = msg.lang;
      ws.role = 'guest';
      ws.code = msg.code;
      send(ws, { type: 'joined', code: msg.code, partnerLang: s.hostLang });
      send(s.host, { type: 'guestJoined', partnerLang: msg.lang });
      broadcastPresence(msg.code);
      return;
    }

    if (msg.type === 'chat') {
      // relay a translated message straight to the other participant, no storage involved
      const s = sessions.get(ws.code);
      if (!s) return;
      const target = otherSide(s, ws.role);
      send(target, { type: 'chat', from: ws.role, original: msg.original, translated: msg.translated });
      return;
    }

    if (msg.type === 'leave') {
      cleanupConnection(ws);
      return;
    }
  });

  ws.on('close', () => cleanupConnection(ws));
});

function cleanupConnection(ws) {
  if (!ws.code) return;
  const s = sessions.get(ws.code);
  if (!s) return;
  if (ws.role === 'host') s.host = null;
  if (ws.role === 'guest') s.guest = null;
  broadcastPresence(ws.code);
  // if both sides are gone, free the session
  if (!s.host && !s.guest) sessions.delete(ws.code);
}

server.listen(PORT, () => {
  const status = [
    ANTHROPIC_API_KEY ? 'Claude configured' : 'Claude NOT configured',
    DEEPL_API_KEY ? 'DeepL configured' : 'DeepL NOT configured',
  ].join(', ');
  console.log('relay server listening on port ' + PORT + ' — ' + status);
});

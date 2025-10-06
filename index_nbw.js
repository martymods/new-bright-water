require('dotenv').config();
const express = require('express');
const dayjs = require('dayjs');
const cron = require('node-cron');
const path = require('path');
const fs = require('fs');
const fsp = fs.promises;
const crypto = require('crypto');
const { EventEmitter } = require('events');
const Stripe = require('stripe');
const OpenAI = require('openai');                           // 👈 NEW
const { Readable } = require('stream');
const stripe = Stripe(process.env.STRIPE_SECRET_KEY || '');
const hasStripeSecret = Boolean(process.env.STRIPE_SECRET_KEY);
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY || '' }); // 👈 NEW
const STATIC_APP_BASE_URL = process.env.PUBLIC_BASE_URL || process.env.APP_BASE_URL || '';
const DEFAULT_APP_BASE_URL = (STATIC_APP_BASE_URL || 'https://www.delcotechdivision.com').replace(/\/$/, '');
const APP_BASE_URL = DEFAULT_APP_BASE_URL;

const fetch = global.fetch || require('node-fetch');
if (!global.fetch) {
  global.fetch = fetch;
}

const { twiml: { VoiceResponse } } = require('twilio');
const { sendSMS, client: twilioClient } = require('./services/twilioClient');
const { upsertByPhone, findAll } = require('./services/sheets');
const { subscribeCalendlyWebhook } = require('./services/calendly');
const { setStep, setField, get: getState } = require('./lib/leadStore');
const { createUserRouter } = require('./routes/users');
const createIntegrationsRouter = require('./routes/integrations');
const createSupportRouter = require('./routes/support');
const createShopifyRouter = require('./routes/shopify');
const createAuditRouter = require('./routes/audit');
const createDesignRouter = require('./routes/design');
const createTeamRouter = require('./routes/teams');
const createWarehouseRouter = require('./routes/warehouse');
const createGivingRouter = require('./routes/giving');
const { bootstrapDemoData, shouldBootstrapDemo, DEMO_DEFAULTS } = require('./lib/bootstrapDemo');

const jwt = require('jsonwebtoken');

// DoorDash Drive credentials (create in the Developer Portal)
const DD_DEVELOPER_ID  = process.env.DD_DEVELOPER_ID  || '';
const DD_KEY_ID        = process.env.DD_KEY_ID        || '';
const DD_SIGNING_B64   = process.env.DD_SIGNING_SECRET || ''; // base64 string

// Pickup (your store) — used for all quotes
const STORE_NAME   = process.env.STORE_NAME   || 'Delco Kitchen';
const STORE_PHONE  = process.env.STORE_PHONE_E164 || '+16105550123'; // E.164 required
const STORE_ADDR   = process.env.STORE_ADDRESS || '123 Market St';
const STORE_CITY   = process.env.STORE_CITY    || 'Philadelphia';
const STORE_STATE  = process.env.STORE_STATE   || 'PA';
const STORE_ZIP    = process.env.STORE_ZIP     || '19106';
const TAX_RATE     = Number(process.env.TAX_RATE || '0.00'); // e.g. 0.06
const MENU = [
  { id:'wing_ding', name:'Wing Ding', price:11.00 },
  { id:'hot_chicken', name:'Hot Chicken', price:16.00 },
  { id:'chicken_boi', name:'Chicken Boi', price:11.00 },
  { id:'wing_party', name:'Wing Party', price:18.00 },
  { id:'big_wingers', name:'Big Wingers', price:19.00 },
  { id:'no_chicken_burger', name:'No Chicken Burger', price:11.00 },
  { id:'fries', name:'Fries', price:9.00 },
  { id:'onion_rings', name:'Onion Rings', price:9.00 },
  { id:'pretzel_cheesesteak', name:'Pretzel Cheesesteak', price:11.00 },
  { id:'pretzel_chicken_cheesesteak', name:'Pretzel Chicken Cheesesteak', price:11.00 },
];

const app = express();
const BODY_LIMIT = process.env.REQUEST_BODY_LIMIT || '15mb';

const demoBootstrapPromise = bootstrapDemoData();

const TTS_CACHE_DIR = path.join(__dirname, '.cache', 'tts');
fs.mkdirSync(TTS_CACHE_DIR, { recursive: true });

const COLD_CALLER_SMS_LINK = process.env.COLD_CALLER_SMS_LINK
  || 'https://www.delcotechdivision.com/';
const COLD_CALLER_SMS_MEDIA = `${APP_BASE_URL}/image/${encodeURIComponent('lifelike AI cold caller.png')}`;

const CSV_ROOT_DIR = path.join(__dirname, 'public', 'csvFIles');
const CSV_ROOT_ABSOLUTE = path.resolve(CSV_ROOT_DIR);
const CSV_PREVIEW_ROW_LIMIT = Number(process.env.CSV_PREVIEW_ROW_LIMIT || 200);
const CSV_AUTOMATION_MAX_LEADS = Number(process.env.CSV_AUTOMATION_MAX_LEADS || 150);
const CSV_AUTOMATION_PARSE_LIMIT = CSV_AUTOMATION_MAX_LEADS + 400; // buffer for skips

const automationEmitter = new EventEmitter();
automationEmitter.setMaxListeners(100);

function publishAutomationEvent(event){
  if(!event || typeof event !== 'object') return;
  automationEmitter.emit('automation', {
    ...event,
    timestamp: new Date().toISOString(),
  });
}

const ELEVENLABS_MODEL_ID = process.env.ELEVENLABS_MODEL_ID || 'eleven_multilingual_v2';
const FALLBACK_ELEVENLABS_VOICE_ID = '21m00Tcm4TlvDq8ikWAM';
const ELEVENLABS_DEFAULT_VOICE_ID = process.env.ELEVENLABS_VOICE_ID || FALLBACK_ELEVENLABS_VOICE_ID;
const ELEVENLABS_MAX_CONCURRENCY = Math.max(1, Number(process.env.ELEVENLABS_MAX_CONCURRENCY || '4'));
const ELEVENLABS_MAX_ATTEMPTS = Math.max(1, Number(process.env.ELEVENLABS_MAX_ATTEMPTS || '3'));
const ELEVENLABS_RETRY_BASE_DELAY_MS = Math.max(50, Number(process.env.ELEVENLABS_RETRY_BASE_DELAY_MS || '300'));
const ELEVENLABS_RETRY_MAX_DELAY_MS = Math.max(
  ELEVENLABS_RETRY_BASE_DELAY_MS,
  Number(process.env.ELEVENLABS_RETRY_MAX_DELAY_MS || '2000'),
);
const ELEVENLABS_RETRY_JITTER_MS = Math.max(0, Number(process.env.ELEVENLABS_RETRY_JITTER_MS || '120'));
const ELEVENLABS_PRESETS = {
  warm: {
    label: 'Warm & friendly (ElevenLabs)',
    variant: 'warm',
    voiceId: process.env.ELEVENLABS_VOICE_ID_WARM || ELEVENLABS_DEFAULT_VOICE_ID,
    modelId: ELEVENLABS_MODEL_ID,
    voiceSettings: {
      stability: 0.48,
      similarity_boost: 0.9,
      style: 0.55,
      use_speaker_boost: true,
    },
  },
  bold: {
    label: 'Bold & confident (ElevenLabs)',
    variant: 'bold',
    voiceId: process.env.ELEVENLABS_VOICE_ID_BOLD || ELEVENLABS_DEFAULT_VOICE_ID,
    modelId: ELEVENLABS_MODEL_ID,
    voiceSettings: {
      stability: 0.32,
      similarity_boost: 0.88,
      style: 0.2,
      use_speaker_boost: true,
    },
  },
  calm: {
    label: 'Calm & precise (ElevenLabs)',
    variant: 'calm',
    voiceId: process.env.ELEVENLABS_VOICE_ID_CALM || ELEVENLABS_DEFAULT_VOICE_ID,
    modelId: ELEVENLABS_MODEL_ID,
    voiceSettings: {
      stability: 0.72,
      similarity_boost: 0.92,
      style: 0.12,
      use_speaker_boost: true,
    },
  },
};

const POLLY_PRESETS = {
  warm: { label: 'Warm & friendly (Polly.Joanna)', voice: 'Polly.Joanna', language: 'en-US', variant: 'warm' },
  bold: { label: 'Bold & confident (Polly.Matthew)', voice: 'Polly.Matthew', language: 'en-US', variant: 'bold' },
  calm: { label: 'Calm & precise (Polly.Amy)', voice: 'Polly.Amy', language: 'en-GB', variant: 'calm' },
};

const USE_ELEVENLABS = Boolean(process.env.ELEVENLABS_API_KEY);

const CALLER_VOICE_PRESETS = USE_ELEVENLABS ? ELEVENLABS_PRESETS : POLLY_PRESETS;

function normalizePhoneNumber(value) {
  if (!value) return '';
  let digits = String(value).trim();
  digits = digits.replace(/[^\d+]/g, '');
  if (!digits) return '';
  if (digits.startsWith('+')) return digits;
  if (digits.startsWith('00')) return `+${digits.slice(2)}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  if (digits.length === 10) return `+1${digits}`;
  if (!digits.startsWith('+')) return `+${digits}`;
  return digits;
}

function maskPhoneNumberForLog(value) {
  if (!value) return value;
  const str = String(value).trim();
  if (!str) return str;
  const digitsOnly = str.replace(/[^\d]/g, '');
  if (digitsOnly.length <= 4) {
    return str;
  }
  const suffix = digitsOnly.slice(-4);
  const hasPlus = str.startsWith('+');
  return `${hasPlus ? '+' : ''}***${suffix}`;
}

function sanitizeLine(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function sanitizeForTts(value) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, 800);
}

function resolveAppBaseUrl(req) {
  const envBase = (STATIC_APP_BASE_URL || '').trim();
  if (envBase) {
    return envBase.replace(/\/$/, '');
  }

  if (req) {
    const forwardedHost = req.headers?.['x-forwarded-host'];
    const hostHeader = Array.isArray(forwardedHost) ? forwardedHost[0] : forwardedHost;
    const host = hostHeader || req.get?.('host') || '';
    if (host) {
      const protoHeader = req.headers?.['x-forwarded-proto'];
      const forwardedProto = Array.isArray(protoHeader) ? protoHeader[0] : (protoHeader || '');
      const protocol = forwardedProto.split(',')[0] || req.protocol || 'https';
      return `${protocol}://${host}`.replace(/\/$/, '');
    }
  }

  return DEFAULT_APP_BASE_URL;
}

function absoluteUrl(pathname = '/', base) {
  const resolvedBase = (base || '').replace(/\/$/, '');
  const suffix = pathname.startsWith('/') ? pathname : `/${pathname}`;
  return `${resolvedBase}${suffix}`;
}

function buildTtsUrl(text, variantKey, base) {
  const sanitized = sanitizeForTts(text);
  const params = new URLSearchParams();
  if (sanitized) params.set('t', sanitized);
  if (variantKey) params.set('variant', variantKey);
  const search = params.toString();
  const path = `/audio/tts${search ? `?${search}` : ''}`;
  return absoluteUrl(path, base || DEFAULT_APP_BASE_URL);
}

const ttsCachePromises = new Map();
let activeElevenLabsRequests = 0;
const elevenLabsQueue = [];
const scheduleElevenLabsNext = typeof setImmediate === 'function'
  ? setImmediate
  : (fn) => setTimeout(fn, 0);

function wait(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function acquireElevenLabsSlot() {
  if (ELEVENLABS_MAX_CONCURRENCY <= 0) {
    return () => {};
  }

  return new Promise(resolve => {
    const tryAcquire = () => {
      if (activeElevenLabsRequests < ELEVENLABS_MAX_CONCURRENCY) {
        activeElevenLabsRequests += 1;
        resolve(() => {
          activeElevenLabsRequests = Math.max(0, activeElevenLabsRequests - 1);
          const next = elevenLabsQueue.shift();
          if (next) {
            scheduleElevenLabsNext(next);
          }
        });
      } else {
        elevenLabsQueue.push(tryAcquire);
      }
    };

    tryAcquire();
  });
}

function base64UrlEncode(str) {
  return Buffer.from(String(str), 'utf8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function base64UrlDecode(str) {
  if (!str) return '';
  let normalized = String(str).replace(/-/g, '+').replace(/_/g, '/');
  while (normalized.length % 4 !== 0) {
    normalized += '=';
  }
  return Buffer.from(normalized, 'base64').toString('utf8');
}

function encodeCallState(state) {
  try {
    return base64UrlEncode(JSON.stringify(state || {}));
  } catch (error) {
    console.error('[ColdCaller] Failed to encode call state', error);
    return '';
  }
}

function decodeCallState(encoded) {
  try {
    const json = base64UrlDecode(encoded);
    if (!json) return null;
    return JSON.parse(json);
  } catch (error) {
    console.error('[ColdCaller] Failed to decode call state', error);
    return null;
  }
}

async function ensureTtsClip({ text, variantKey, preset, baseUrl }) {
  if (!USE_ELEVENLABS) {
    return '';
  }

  const sanitized = sanitizeForTts(text);
  if (!sanitized) {
    return '';
  }

  const resolvedVariant = variantKey || preset?.variant || 'warm';
  const resolvedPreset = preset || ELEVENLABS_PRESETS[resolvedVariant] || ELEVENLABS_PRESETS.warm;
  const voiceId = resolvedPreset?.voiceId || ELEVENLABS_DEFAULT_VOICE_ID;
  if (!voiceId) {
    return '';
  }

  const hash = crypto
    .createHash('sha1')
    .update(JSON.stringify([resolvedVariant, voiceId, sanitized]))
    .digest('hex');

  const filename = `${hash}.mp3`;
  const filePath = path.join(TTS_CACHE_DIR, filename);
  const url = absoluteUrl(`/static/tts/${filename}`, baseUrl || DEFAULT_APP_BASE_URL);

  try {
    await fs.promises.access(filePath, fs.constants.F_OK);
    return url;
  } catch (error) {
    // Cache miss, continue to fetch
  }

  if (ttsCachePromises.has(hash)) {
    await ttsCachePromises.get(hash);
    return url;
  }

  const fetchPromise = (async () => {
    const response = await streamElevenLabsAudio({
      text: sanitized,
      preset: resolvedPreset,
      voiceId,
    });

    if (!response?.ok) {
      const bodyText = response ? await response.text().catch(() => '') : '';
      throw new Error(`ElevenLabs cache miss failed (${response?.status || 'no-status'}): ${bodyText.slice(0, 160)}`);
    }

    const arrayBuffer = await response.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    await fs.promises.writeFile(filePath, buffer);
  })();

  ttsCachePromises.set(hash, fetchPromise);

  try {
    await fetchPromise;
    return url;
  } catch (error) {
    console.error('[ColdCaller] Failed to cache TTS clip', {
      message: error?.message || error,
      variant: resolvedVariant,
      textPreview: sanitized.slice(0, 80),
    });
    try {
      await fs.promises.rm(filePath, { force: true });
    } catch (_) {
      // ignore cleanup errors
    }
    return buildTtsUrl(sanitized, resolvedVariant, baseUrl);
  } finally {
    ttsCachePromises.delete(hash);
  }
}

async function appendSpeech(target, text, { variantKey, preset, baseUrl }) {
  const sanitized = sanitizeForTts(text);
  if (!sanitized) {
    return;
  }

  const resolvedVariant = variantKey || preset?.variant || 'warm';
  const resolvedPreset = preset || CALLER_VOICE_PRESETS[resolvedVariant] || CALLER_VOICE_PRESETS.warm;

  if (USE_ELEVENLABS) {
    const clipUrl = await ensureTtsClip({ text: sanitized, variantKey: resolvedVariant, preset: resolvedPreset, baseUrl });
    if (clipUrl) {
      target.play(clipUrl);
      return;
    }
  }

  const voice = resolvedPreset?.voice || 'Polly.Joanna';
  const language = resolvedPreset?.language || 'en-US';
  target.say({ voice, language }, sanitized);
}

const callSmsTracker = new Map();
const CALL_SMS_TRACKER_LIMIT = 500;

function resolveVariantPreset(variantKey) {
  const variant = variantKey || 'warm';
  const preset = CALLER_VOICE_PRESETS[variant] || CALLER_VOICE_PRESETS.warm;
  return { variant, preset };
}

function extractLeadNumber(body) {
  const candidate = body?.To || body?.Called || body?.Caller || body?.From || '';
  return normalizePhoneNumber(candidate);
}

function normalizeSpeech(text) {
  return String(text || '').trim().toLowerCase();
}

function isAffirmativeSpeech(text) {
  if (!text) return false;
  return /(yes|yeah|yep|sure|ok|okay|affirmative|correct|go ahead|do it|sounds good|works|alright|please)/i.test(text);
}

function isNegativeSpeech(text) {
  if (!text) return false;
  return /(no|not now|later|busy|stop|don't|do not|cant|cannot|can't|maybe another time|another time|nope|hang up)/i.test(text);
}

function requestsLink(text) {
  if (!text) return false;
  return /(text|send|link|message|sms|email it|email me)/i.test(text);
}

function detectLeadIntent(text) {
  if (!text) return '';
  if (/(buy|buyer|purchase|acquir)/i.test(text)) return 'buy';
  if (/(sell|listing|list|seller)/i.test(text)) return 'sell';
  if (/(invest|investment|investor|portfolio)/i.test(text)) return 'invest';
  if (/(rent|rental|tenant|lease)/i.test(text)) return 'rent';
  return '';
}

async function scanCsvDirectory(dirPath, relativePath = '') {
  let entries;
  try {
    entries = await fsp.readdir(dirPath, { withFileTypes: true });
  } catch (error) {
    if (error?.code === 'ENOENT') {
      return [];
    }
    throw error;
  }

  const items = [];
  for (const entry of entries) {
    if (!entry) continue;
    if (entry.name.startsWith('.')) continue;
    const absoluteChild = path.join(dirPath, entry.name);
    const relativeChild = relativePath ? path.join(relativePath, entry.name) : entry.name;
    if (entry.isDirectory()) {
      const children = await scanCsvDirectory(absoluteChild, relativeChild);
      items.push({
        type: 'directory',
        name: entry.name,
        path: relativeChild.split(path.sep).join('/'),
        items: children,
      });
      continue;
    }
    if (!entry.isFile()) continue;
    if (!/\.csv$/i.test(entry.name)) continue;
    let stats = null;
    try {
      stats = await fsp.stat(absoluteChild);
    } catch (error) {
      stats = null;
    }
    const normalizedPath = relativeChild.split(path.sep).join('/');
    items.push({
      type: 'file',
      name: entry.name,
      path: normalizedPath,
      url: `/csvFIles/${normalizedPath}`,
      size: stats ? stats.size : null,
      modified: stats ? stats.mtime.toISOString() : null,
    });
  }

  items.sort((a, b) => {
    if (a.type !== b.type) {
      return a.type === 'directory' ? -1 : 1;
    }
    return a.name.localeCompare(b.name, undefined, { sensitivity: 'base' });
  });

  return items;
}

function normalizeCsvRelativePath(value = '') {
  const trimmed = String(value || '').trim();
  if (!trimmed) {
    return '';
  }
  const normalized = path.normalize(trimmed).replace(/^\.(\\|\/)/, '');
  return normalized.replace(/^[\\/]+/, '');
}

function resolveCsvFile(relativePath = '') {
  const normalized = normalizeCsvRelativePath(relativePath);
  const absolute = path.resolve(path.join(CSV_ROOT_DIR, normalized));
  const rootPrefix = `${CSV_ROOT_ABSOLUTE}${path.sep}`;
  if (absolute !== CSV_ROOT_ABSOLUTE && !absolute.startsWith(rootPrefix)) {
    const error = new Error('invalid_csv_path');
    error.code = 'invalid_csv_path';
    throw error;
  }
  return { relative: normalized, absolute };
}

function stripBom(value) {
  if (!value) return value;
  return value.replace(/^\ufeff/, '');
}

function tidyCell(value) {
  if (value == null) return '';
  return stripBom(String(value).replace(/\r/g, '')).trim();
}

function parseCsvText(text, { maxRows = 1000 } = {}) {
  const rows = [];
  if (!text) return rows;
  const limit = Math.max(1, Math.min(maxRows, 10000));
  let current = [];
  let cell = '';
  let inQuotes = false;

  const pushCell = () => {
    current.push(tidyCell(cell));
    cell = '';
  };

  const pushRow = () => {
    if (!inQuotes) {
      rows.push(current);
      current = [];
    }
  };

  for (let i = 0; i < text.length; i += 1) {
    const char = text[i];
    if (char === '"') {
      if (inQuotes && text[i + 1] === '"') {
        cell += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === ',' && !inQuotes) {
      pushCell();
      continue;
    }
    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && text[i + 1] === '\n') {
        i += 1;
      }
      pushCell();
      pushRow();
      if (rows.length >= limit) {
        return rows;
      }
      continue;
    }
    cell += char;
  }

  pushCell();
  if (current.length || cell) {
    pushRow();
  }

  return rows.slice(0, limit);
}

function guessColumnType(label, samples = []) {
  const lower = String(label || '').toLowerCase();
  if (/email|e-mail/.test(lower)) return 'email';
  if (/phone|cell|mobile|contact|tel|sms|dial/.test(lower)) return 'phone';
  if (/first\s*name/.test(lower)) return 'first_name';
  if (/last\s*name|surname/.test(lower)) return 'last_name';
  if (/name/.test(lower)) return 'name';
  if (/company|business|firm|broker|agency|organization/.test(lower)) return 'company';
  if (/city|town|metro/.test(lower)) return 'city';
  if (/state|region|province/.test(lower)) return 'state';
  if (/zip|postal|postcode/.test(lower)) return 'postal';
  if (/title|role|position/.test(lower)) return 'title';
  if (/industry|sector|vertical/.test(lower)) return 'industry';
  if (/status|stage|priority/.test(lower)) return 'status';

  const trimmedSamples = samples.filter(Boolean).map((sample) => String(sample).trim());
  if (trimmedSamples.some((sample) => /@/.test(sample))) return 'email';
  if (trimmedSamples.some((sample) => sample.replace(/[^\d]/g, '').length >= 10)) return 'phone';
  if (trimmedSamples.length && trimmedSamples.every((sample) => /^(mr\.?|mrs\.?|ms\.?|dr\.?|[A-Za-z])[\w\s'.-]*$/.test(sample))) {
    return 'name';
  }
  return 'text';
}

function buildColumnSummaries(header, dataRows) {
  return header.map((label, index) => {
    const samples = [];
    for (const row of dataRows) {
      const value = tidyCell(row[index]);
      if (value) {
        samples.push(value);
      }
      if (samples.length >= 3) break;
    }
    return {
      index,
      key: `c_${index}`,
      label: label || `Column ${index + 1}`,
      lowerLabel: String(label || '').toLowerCase(),
      type: guessColumnType(label, samples),
      samples,
    };
  });
}

function summarizeRecordForContext(record, exclude = new Set(), limit = 3) {
  const parts = [];
  for (const [key, value] of Object.entries(record)) {
    if (!value) continue;
    if (exclude.has(key)) continue;
    if (/email|e-mail/.test(key)) continue;
    const cleanKey = key.replace(/_/g, ' ');
    parts.push(`${cleanKey}: ${value}`);
    if (parts.length >= limit) break;
  }
  return parts.join(' • ');
}

function buildLeadRecords(columns, dataRows) {
  const leads = [];
  const skipped = [];
  const phoneColumns = columns.filter((col) => col.type === 'phone');
  const firstNameColumn = columns.find((col) => col.type === 'first_name');
  const lastNameColumn = columns.find((col) => col.type === 'last_name');
  const fullNameColumn = columns.find((col) => col.type === 'name');
  const companyColumn = columns.find((col) => col.type === 'company');

  const seenPhones = new Set();

  dataRows.forEach((row, rowIndex) => {
    const record = {};
    columns.forEach((col) => {
      record[col.label || `Column ${col.index + 1}`] = tidyCell(row[col.index]);
    });

    let phoneValue = '';
    if (phoneColumns.length) {
      for (const phoneCol of phoneColumns) {
        const candidate = tidyCell(row[phoneCol.index]);
        if (candidate) {
          phoneValue = candidate;
          break;
        }
      }
    } else {
      for (const value of row) {
        const candidate = tidyCell(value);
        if (candidate && candidate.replace(/[^\d]/g, '').length >= 10) {
          phoneValue = candidate;
          break;
        }
      }
    }

    const normalizedPhone = normalizePhoneNumber(phoneValue);
    if (!normalizedPhone) {
      skipped.push({ index: rowIndex + 1, reason: 'missing_phone', record });
      return;
    }
    if (seenPhones.has(normalizedPhone)) {
      skipped.push({ index: rowIndex + 1, reason: 'duplicate_phone', record });
      return;
    }

    let leadName = '';
    if (fullNameColumn) {
      leadName = tidyCell(row[fullNameColumn.index]);
    }
    const firstName = tidyCell(firstNameColumn ? row[firstNameColumn.index] : '');
    const lastName = tidyCell(lastNameColumn ? row[lastNameColumn.index] : '');
    if (!leadName) {
      leadName = [firstName, lastName].filter(Boolean).join(' ').trim();
    }
    if (!leadName) {
      leadName = tidyCell(companyColumn ? row[companyColumn.index] : '') || 'Lead';
    }

    const excludeKeys = new Set();
    [fullNameColumn, firstNameColumn, lastNameColumn, companyColumn, ...phoneColumns].forEach((col) => {
      if (col) {
        excludeKeys.add(col.label || `Column ${col.index + 1}`);
      }
    });
    const context = summarizeRecordForContext(record, excludeKeys, 3);

    leads.push({
      name: leadName || 'Lead',
      phone: normalizedPhone,
      firstName: firstName || leadName.split(' ')[0] || '',
      record,
      context,
    });
    seenPhones.add(normalizedPhone);
  });

  return { leads, skipped };
}

async function loadCsvAutomationData(relativePath, { maxRows = CSV_AUTOMATION_PARSE_LIMIT } = {}) {
  const { absolute, relative } = resolveCsvFile(relativePath);
  const text = await fsp.readFile(absolute, 'utf8');
  const rows = parseCsvText(text, { maxRows });
  const header = rows[0] || [];
  const dataRows = rows.slice(1);
  const columns = buildColumnSummaries(header, dataRows);
  const { leads, skipped } = buildLeadRecords(columns, dataRows);
  let stats = null;
  try {
    stats = await fsp.stat(absolute);
  } catch (error) {
    stats = null;
  }

  return {
    file: {
      path: relative,
      name: path.basename(relative),
      size: stats ? stats.size : null,
      modified: stats ? stats.mtime.toISOString() : null,
    },
    header,
    dataRows,
    columns,
    leads,
    skipped,
  };
}

async function sendColdCallerLink(toNumber, callSid) {
  const normalized = normalizePhoneNumber(toNumber);
  if (!normalized) {
    return false;
  }

  if (callSid && callSmsTracker.has(callSid)) {
    return false;
  }

  try {
    await sendSMS(
      normalized,
      `Here's the quick link: ${COLD_CALLER_SMS_LINK}`,
      {
        source: 'cold_caller_auto_link',
        callSid: callSid || null,
        originalTo: toNumber,
      },
      {
        mediaUrl: [COLD_CALLER_SMS_MEDIA],
      },
    );
    if (callSid) {
      callSmsTracker.set(callSid, Date.now());
      if (callSmsTracker.size > CALL_SMS_TRACKER_LIMIT) {
        const oldestKey = callSmsTracker.keys().next().value;
        if (oldestKey) {
          callSmsTracker.delete(oldestKey);
        }
      }
    }
    return true;
  } catch (error) {
    console.error('[ColdCaller] Failed to send SMS link', {
      message: error?.message || error,
      to: maskPhoneNumberForLog(normalized),
    });
    return false;
  }
}

function clearCallSmsRecord(callSid) {
  if (!callSid) return;
  callSmsTracker.delete(callSid);
}

const contentSecurityPolicy = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://js.stripe.com https://cdn.jsdelivr.net https://assets.calendly.com https://player.vimeo.com",
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.jsdelivr.net https://assets.calendly.com",
  "font-src 'self' data: https://fonts.gstatic.com",
  "img-src 'self' data: blob: https://*",
  "media-src 'self' data: blob: https://cdn.coverr.co https://player.vimeo.com",
  "connect-src 'self' https: wss: data:",
  "frame-src 'self' https://js.stripe.com https://hooks.stripe.com https://assets.calendly.com https://calendly.com https://*.calendly.com https://www.youtube.com https://player.vimeo.com",
  "form-action 'self' https://hooks.stripe.com",
  "base-uri 'self'",
  "object-src 'none'",
].join('; ');

app.use((_, res, next) => {
  res.setHeader('Content-Security-Policy', contentSecurityPolicy);
  next();
});

app.get('/api/food/menu', (_req,res)=> res.json({ items: MENU, taxRate: TAX_RATE }));


app.use(express.urlencoded({ extended: true, limit: BODY_LIMIT })); // Twilio posts form-url-encoded
app.use(express.json({ limit: BODY_LIMIT }));

app.use('/api/giving', createGivingRouter({
  stripe,
  hasStripe: hasStripeSecret,
  getAppBaseUrl: resolveAppBaseUrl,
}));
app.use('/api/users', createUserRouter());
app.use('/api/integrations', createIntegrationsRouter());
app.use('/api/support', createSupportRouter(openai));
app.use('/api/shopify', createShopifyRouter());
app.use('/api/audit', createAuditRouter());
app.use('/api/design', createDesignRouter());
app.use('/api/teams', createTeamRouter());
app.use('/api/warehouse', createWarehouseRouter());

app.use('/static/tts', express.static(TTS_CACHE_DIR, {
  setHeaders: (res) => {
    res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
  },
}));

// 👉 Serve the landing page & assets from /public
app.use(express.static(path.join(__dirname, 'public')));

function parseElevenLabsError(message) {
  if (!message) return null;
  try {
    const parsed = JSON.parse(message);
    if (parsed?.detail) {
      return {
        code: parsed.detail.status || null,
        detail: parsed.detail.message || message,
      };
    }
    return {
      code: parsed.status || null,
      detail: parsed.message || message,
    };
  } catch (err) {
    return null;
  }
}

async function streamElevenLabsAudio({ text, preset, voiceId }) {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  const endpoint = `https://api.elevenlabs.io/v1/text-to-speech/${voiceId}?optimize_streaming_latency=1&output_format=mp3_44100_128`;
  const body = {
    text,
    model_id: preset.modelId || ELEVENLABS_MODEL_ID,
    voice_settings: preset.voiceSettings,
  };

  const release = await acquireElevenLabsSlot();

  try {
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      attempt += 1;
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'xi-api-key': apiKey,
          'content-type': 'application/json',
          accept: 'audio/mpeg',
        },
        body: JSON.stringify(body),
      });

      const shouldRetry = response.status === 429 && attempt < ELEVENLABS_MAX_ATTEMPTS;
      if (!shouldRetry) {
        return response;
      }

      const retryAfterHeader = Number(response.headers?.get?.('retry-after'));
      const baseDelay = Number.isFinite(retryAfterHeader) && retryAfterHeader >= 0
        ? retryAfterHeader * 1000
        : ELEVENLABS_RETRY_BASE_DELAY_MS * (2 ** (attempt - 1));
      const jitter = ELEVENLABS_RETRY_JITTER_MS ? Math.random() * ELEVENLABS_RETRY_JITTER_MS : 0;
      const delayMs = Math.min(ELEVENLABS_RETRY_MAX_DELAY_MS, baseDelay + jitter);

      console.warn('[ColdCaller] ElevenLabs 429 received, retrying', {
        attempt,
        delayMs: Math.round(delayMs),
      });

      if (response?.body?.cancel) {
        try {
          response.body.cancel();
        } catch (err) {
          console.warn('[ColdCaller] Failed to cancel ElevenLabs response body', err?.message || err);
        }
      } else if (response?.body?.destroy) {
        try {
          response.body.destroy();
        } catch (err) {
          console.warn('[ColdCaller] Failed to destroy ElevenLabs response body', err?.message || err);
        }
      }

      await wait(delayMs);
    }
  } finally {
    if (typeof release === 'function') {
      release();
    }
  }
}

async function streamOpenAITts({ text, res }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return false;
  }

  const voice = process.env.OPENAI_TTS_VOICE || 'alloy';
  const model = process.env.OPENAI_TTS_MODEL || 'gpt-4o-mini-tts';

  try {
    console.info('[ColdCaller] Using OpenAI TTS fallback', { model, voice, textLength: text.length });
    const speech = await openai.audio.speech.create({
      model,
      voice,
      input: text,
      format: 'mp3',
    });

    const arrayBuffer = await speech.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);
    res.setHeader('Content-Type', 'audio/mpeg');
    res.setHeader('Cache-Control', 'no-store');
    res.send(buffer);
    return true;
  } catch (error) {
    console.error('[ColdCaller] OpenAI TTS fallback failed', {
      message: error?.message || error,
      stack: error?.stack,
    });
    return false;
  }
}

app.get('/audio/tts', async (req, res) => {
  if (!USE_ELEVENLABS) {
    return res.status(503).send('ElevenLabs voice is not configured.');
  }

  try {
    const rawText = typeof req.query?.t === 'string' ? req.query.t : '';
    const text = sanitizeForTts(rawText) || 'Hello from ElevenLabs.';
    const variantKey = typeof req.query?.variant === 'string'
      ? req.query.variant.toLowerCase()
      : 'warm';
    const preset = ELEVENLABS_PRESETS[variantKey] || ELEVENLABS_PRESETS.warm;
    const voiceId = preset.voiceId || ELEVENLABS_DEFAULT_VOICE_ID;
    if (!voiceId) {
      return res.status(500).send('Missing ElevenLabs voice.');
    }

    console.info('[ColdCaller] /audio/tts request', {
      variant: variantKey,
      textLength: text.length,
      textPreview: text.slice(0, 80),
      query: req.query,
    });

    const response = await streamElevenLabsAudio({ text, preset, voiceId });

    console.info('[ColdCaller] ElevenLabs response received', { status: response.status });

    if (response.ok && response.body) {
      res.setHeader('Content-Type', 'audio/mpeg');
      res.setHeader('Cache-Control', 'no-store');
      Readable.fromWeb(response.body).pipe(res);
      console.info('[ColdCaller] ElevenLabs audio proxied successfully');
      return;
    }

    const errorMessage = await response.text().catch(() => '');
    const parsedError = parseElevenLabsError(errorMessage);
    const quotaExceeded = (response.status === 401 || response.status === 402 || response.status === 429)
      && parsedError?.code === 'quota_exceeded';

    console.error('[ColdCaller] ElevenLabs error response', {
      status: response.status,
      message: errorMessage,
      parsed: parsedError,
    });

    if (quotaExceeded) {
      const fallbackSuccess = await streamOpenAITts({ text, res });
      if (fallbackSuccess) {
        return;
      }
      return res.status(429).send('ElevenLabs quota exceeded and fallback unavailable.');
    }

    return res.status(502).send(`ElevenLabs error: ${errorMessage}`);
  } catch (error) {
    console.error('[ColdCaller] ElevenLabs TTS proxy error', {
      message: error?.message || error,
      stack: error?.stack,
    });
    res.status(500).send('ElevenLabs TTS unavailable.');
  }
});

// One canonical booking URL for the site + SMS
app.get('/book', (req, res) => {
  const url = process.env.CALENDLY_SCHEDULING_LINK || '';
  if (url) return res.redirect(url);     // 302 → your Calendly event
  return res.redirect('/checkout');      // fallback if env not set
});

// 1) Serve everything in /public (so /robots.txt, /favicon.ico work)
app.use(express.static(path.join(__dirname, 'public'), {
  extensions: ['html'] // lets /dental resolve to index.html automatically
}));

// 2) Be explicit for /dental to be safe
app.use('/dental', express.static(path.join(__dirname, 'public', 'dental'), {
  extensions: ['html']
}));

// mount the API
app.use('/api/eligibility', require('./routes/eligibility'));

const BUSINESS = process.env.BUSINESS_NAME || 'Our Team';
const CAL_LINK = process.env.CALENDLY_SCHEDULING_LINK || '#';
const REVIEW_LINK = process.env.REVIEW_LINK || '';
const DIAL_TIMEOUT = parseInt(process.env.DIAL_TIMEOUT || '20', 10);

const DENTAL_PLANS = [
  {
    slug: 'starter',
    name: 'Starter Smile',
    setupCents: 19900,
    monthlyCents: 14900,
    description: '1 provider, 1 location. Includes insurance eligibility + missed-call text back.',
  },
  {
    slug: 'growth',
    name: 'Growth Practice',
    setupCents: 49900,
    monthlyCents: 34900,
    description: 'Up to 3 providers, 2 locations. Deposits, recall campaigns, WhatsApp intake.',
  },
  {
    slug: 'premium',
    name: 'Premium Care',
    setupCents: 99900,
    monthlyCents: 79900,
    description: 'Unlimited providers, up to 5 locations. Multi-location dashboards + co-pay estimates.',
  },
];

const REAL_ESTATE_PLANS = [
  {
    slug: 'solo',
    name: 'Solo Agent Sprint',
    setupCents: 14900,
    monthlyCents: 15900,
    description: '1 agent seat. SMS concierge follow-up, auto lead capture, checkout links. No outbound voice.',
    features: [
      'SMS lead rescue + pay links',
      'Public data scrapers (FSBO, expired)',
      'Calendly + Sheets logging',
    ],
  },
  {
    slug: 'signal',
    name: 'Signal Team Stack',
    setupCents: 29900,
    monthlyCents: 34900,
    description: 'Up to 3 agents. Adds non-premium AI dialing with standard voices, instant text scripts.',
    features: [
      'Non-premium AI voice dialer (US/CA)',
      'Shared script vault + objection swaps',
      'Instant text + email recaps',
    ],
  },
  {
    slug: 'squadron',
    name: 'Power Dial Squadron',
    setupCents: 44900,
    monthlyCents: 64900,
    description: 'High-volume teams. Multi-seat queue, speech recognition branches, live scoreboard.',
    features: [
      'Speech recognition + sentiment tags',
      'Round-robin queues & transfers',
      'Script coach with real-time edits',
    ],
  },
  {
    slug: 'enterprise',
    name: 'Enterprise Command',
    setupCents: 69900,
    monthlyCents: 129900,
    description: 'Enterprise brokers. Premium voice cloning, multilingual routing, CRM + Slack sync.',
    features: [
      'Premium cloned voices & personas',
      'Multilingual speech + translation',
      'Custom CRM, Slack, Sheets automations',
    ],
  },
];

// Health
app.get('/health', (_, res) => res.json({ ok: true }));

// ---------------------------------------------------------------------
// Front-end config (safe to expose PUBLISHABLE IDs only)
// ---------------------------------------------------------------------
app.get('/config', (_, res) => {
  res.json({
    stripePk: process.env.STRIPE_PUBLISHABLE_KEY || '',
    paypalClientId: process.env.PAYPAL_CLIENT_ID || '',
    paypalPlanId: process.env.PAYPAL_PLAN_ID || '',
    demoAccount: shouldBootstrapDemo ? {
      email: DEMO_DEFAULTS.email,
      password: DEMO_DEFAULTS.password,
    } : null,
    demoShopify: shouldBootstrapDemo ? {
      shopDomain: DEMO_DEFAULTS.shopDomain,
    } : null,
    dentalPlans: DENTAL_PLANS.map(plan => ({
      slug: plan.slug,
      name: plan.name,
      setup: plan.setupCents / 100,
      monthly: plan.monthlyCents / 100,
      description: plan.description,
    })),
    realEstatePlans: REAL_ESTATE_PLANS.map(plan => ({
      slug: plan.slug,
      name: plan.name,
      setup: plan.setupCents / 100,
      monthly: plan.monthlyCents / 100,
      description: plan.description,
      features: plan.features,
    })),
  });
});

app.post('/api/real-estate/checkout', async (req, res) => {
  try {
    const slug = String(req.body?.plan || '').toLowerCase();
    const plan = REAL_ESTATE_PLANS.find(p => p.slug === slug);

    if (!plan) {
      return res.status(400).json({ error: 'invalid_plan' });
    }

    const lineItems = [];

    if (plan.monthlyCents) {
      lineItems.push({
        price_data: {
          currency: 'usd',
          unit_amount: plan.monthlyCents,
          recurring: { interval: 'month' },
          product_data: {
            name: `${plan.name} — Monthly Subscription`,
          },
        },
        quantity: 1,
      });
    }

    if (plan.setupCents) {
      lineItems.push({
        price_data: {
          currency: 'usd',
          unit_amount: plan.setupCents,
          product_data: {
            name: `${plan.name} — Onboarding & Setup`,
          },
        },
        quantity: 1,
      });
    }

    if (!lineItems.length) {
      return res.status(400).json({ error: 'unsupported_plan' });
    }

    const baseUrl = resolveAppBaseUrl(req);

    const session = await stripe.checkout.sessions.create({
      mode: 'subscription',
      allow_promotion_codes: true,
      line_items: lineItems,
      success_url: `${baseUrl}/real-estate-cold-caller.html?plan=${plan.slug}&checkout=success`,
      cancel_url: `${baseUrl}/real-estate-cold-caller.html?plan=${plan.slug}&checkout=cancel`,
      metadata: {
        productLine: 'real-estate-cold-caller',
        plan: plan.slug,
      },
    });

    if (session?.url) {
      return res.json({ url: session.url });
    }

    return res.json({ id: session.id });
  } catch (error) {
    console.error('Real estate checkout error:', error?.raw?.message || error?.message, error);
    return res.status(500).json({ error: 'stripe_error' });
  }
});

app.post('/api/real-estate/text-link', async (req, res) => {
  try {
    const { phone, lead = '', plan: planSlug = '' } = req.body || {};
    const normalized = normalizePhoneNumber(phone);

    if (!normalized) {
      return res.status(400).json({ error: 'invalid_phone' });
    }

    const plan = REAL_ESTATE_PLANS.find(p => p.slug === String(planSlug || '').toLowerCase()) || null;
    const leadName = sanitizeLine(lead).split(' ')[0] || '';
    const greeting = leadName ? `Hey ${leadName},` : 'Hey there,';
    const baseUrl = resolveAppBaseUrl(req);
    const anchor = plan ? `#plan-${plan.slug}` : '#plans';
    const link = `${baseUrl}/real-estate-cold-caller.html${anchor}`;
    const highlight = plan
      ? `Recommended loadout: ${plan.name}.`
      : 'Here is the control deck for our real estate command center.';

    const message = `${greeting} it's ${BUSINESS}. ${highlight} Grab your spot here: ${link} — Reply STOP to opt out.`;

    await sendSMS(normalized, message, {
      source: 'real_estate_text_link',
      plan: plan?.slug || '',
      leadName: leadName || '',
      appBaseUrl: baseUrl,
    });

    return res.json({ ok: true });
  } catch (error) {
    console.error('Real estate SMS link error:', error?.message || error, error);
    return res.status(500).json({ error: 'sms_failed' });
  }
});

async function queueColdCallerCall({
  to,
  leadName = '',
  goal = '',
  intro = '',
  script = '',
  voice = 'warm',
  handoffNumber = '',
  appBaseUrl = DEFAULT_APP_BASE_URL,
}) {
  const toNumber = normalizePhoneNumber(to);
  if (!toNumber) {
    const error = new Error('invalid_to');
    error.code = 'invalid_to';
    throw error;
  }

  const fromNumber = normalizePhoneNumber(process.env.TWILIO_NUMBER);
  if (!fromNumber) {
    const error = new Error('missing_from_number');
    error.code = 'missing_from_number';
    throw error;
  }

  const presetKey = String(voice || '').toLowerCase();
  const preset = CALLER_VOICE_PRESETS[presetKey] || CALLER_VOICE_PRESETS.warm;
  const variantKey = preset?.variant || presetKey || 'warm';

  const logPayload = {
    to: maskPhoneNumberForLog(toNumber),
    leadName: leadName ? '[redacted]' : '',
    goal: goal ? goal.slice(0, 120) : '',
    intro: intro ? intro.slice(0, 120) : '',
    scriptLength: script ? String(script).length : 0,
    variant: variantKey,
    handoffConfigured: Boolean(handoffNumber),
    appBaseUrl,
  };
  console.info('[ColdCaller] Dial request received', logPayload);

  const sanitizedIntro = sanitizeLine(intro);
  const greetingLine = leadName
    ? `Hey ${leadName}, Alex with Delco Realty.`
    : 'Hey, Alex with Delco Realty.';
  const quickIntroLine = sanitizedIntro
    || (goal ? `Quick question about ${goal}.` : 'Quick question for you.');
  const introQuestionLine = 'Do you have a minute right now?';

  const providedLines = String(script || '')
    .split(/\r?\n/)
    .map(sanitizeLine)
    .filter(Boolean);

  const positiveAckLine = leadName ? `Appreciate it, ${leadName}.` : 'Appreciate it.';
  const valueBridgeLine = providedLines[0]
    || (goal ? `It's about ${goal}.` : 'We spotted something worth a quick look.');
  const qualifierQuestion = providedLines[1]
    || 'Are you looking to buy, sell, or invest in the next 90 days?';
  const smsOfferLine = providedLines[2]
    || 'I just texted you a quick link so you can see the options.';
  const declineLine = providedLines[3]
    || 'No worries — I just texted you the link. Have a great day!';
  const handoffPromptLine = providedLines[4]
    || 'Want me to connect you with a teammate now?';
  const connectLine = 'One moment while I connect you to a teammate.';

  const sanitizedHandoff = sanitizeLine(handoffNumber);
  let handoff = '';
  if (sanitizedHandoff) {
    handoff = normalizePhoneNumber(sanitizedHandoff);
    if (!handoff) {
      const error = new Error('invalid_handoff');
      error.code = 'invalid_handoff';
      throw error;
    }
  }

  const callState = {
    variant: variantKey,
    introLines: [quickIntroLine, introQuestionLine].filter(Boolean),
    positiveAckLine,
    valueBridgeLine,
    qualifierQuestion,
    smsOfferLine,
    declineLine,
    handoffPromptLine,
    connectLine,
    handoffNumber: handoff,
    prefillIntent: '',
    introNoSpeechCount: 0,
    qualifyNoSpeechCount: 0,
  };
  const encodedState = encodeCallState(callState);
  const introAction = absoluteUrl(`/voice/cold-caller/intro?state=${encodedState}`, appBaseUrl);

  if (USE_ELEVENLABS) {
    const warmupLines = [
      greetingLine,
      quickIntroLine,
      introQuestionLine,
      positiveAckLine,
      valueBridgeLine,
      qualifierQuestion,
      smsOfferLine,
      declineLine,
      handoffPromptLine,
      connectLine,
    ].filter(Boolean);

    await Promise.all(warmupLines.map((line) =>
      ensureTtsClip({ text: line, variantKey, preset, baseUrl: appBaseUrl })
        .catch((error) => {
          console.warn('[ColdCaller] Warmup TTS failed', { message: error?.message || error });
          return null;
        })
    ));
  }

  const voiceResponse = new VoiceResponse();
  await appendSpeech(voiceResponse, greetingLine, { variantKey, preset, baseUrl: appBaseUrl });

  const gather = voiceResponse.gather({
    input: 'speech',
    method: 'POST',
    action: introAction,
    speechTimeout: 'auto',
    timeout: 5,
    bargeIn: 'true',
    actionOnEmptyResult: true,
  });

  await appendSpeech(gather, quickIntroLine, { variantKey, preset, baseUrl: appBaseUrl });
  gather.pause({ length: 1 });
  await appendSpeech(gather, introQuestionLine, { variantKey, preset, baseUrl: appBaseUrl });

  voiceResponse.redirect(introAction);

  const twiml = voiceResponse.toString();
  console.info('[ColdCaller] TwiML preview', twiml.slice(0, 400) + (twiml.length > 400 ? '…' : ''));

  const call = await twilioClient.calls.create({
    to: toNumber,
    from: fromNumber,
    twiml,
    machineDetection: 'DetectMessageEnd',
  });

  console.info('[ColdCaller] Twilio call queued', { sid: call.sid, to: maskPhoneNumberForLog(toNumber) });

  return { sid: call.sid, to: toNumber, variant: variantKey };
}

app.post('/api/cold-caller/dial', async (req, res) => {
  try {
    const appBaseUrl = resolveAppBaseUrl(req);
    const result = await queueColdCallerCall({ ...req.body, appBaseUrl });
    return res.json({ sid: result.sid });
  } catch (error) {
    const errorInfo = {
      message: error?.message || 'Unknown error',
      code: error?.code,
      status: error?.status,
      moreInfo: error?.moreInfo,
      details: error?.details,
      responseData: error?.response?.data,
    };
    console.error('[ColdCaller] Dial error', errorInfo);
    if (error?.code === 'invalid_to') {
      return res.status(400).json({ error: 'invalid_to' });
    }
    if (error?.code === 'invalid_handoff') {
      return res.status(400).json({ error: 'invalid_handoff' });
    }
    if (error?.code === 'missing_from_number') {
      return res.status(500).json({ error: 'missing_from_number' });
    }
    const statusCode = Number(error?.status) || 500;
    const payload = { error: 'dial_failed' };
    if (error?.code || error?.moreInfo) {
      payload.twilio = {
        code: error.code,
        moreInfo: error.moreInfo,
        status: error.status,
      };
    }
    return res.status(statusCode).json(payload);
  }
});

app.post('/voice/cold-caller/intro', async (req, res) => {
  try {
    const stateToken = typeof req.query?.state === 'string' ? req.query.state : '';
    const state = decodeCallState(stateToken) || {};
    const appBaseUrl = resolveAppBaseUrl(req);
    const { variant, preset } = resolveVariantPreset(state.variant);
    const speech = normalizeSpeech(req.body?.SpeechResult);
    const callSid = req.body?.CallSid || '';
    const leadNumber = extractLeadNumber(req.body);
    const wantsLinkNow = requestsLink(speech);
    const intent = detectLeadIntent(speech);
    const hasIntent = Boolean(intent);

    if (isNegativeSpeech(speech) || wantsLinkNow) {
      if (leadNumber) {
        await sendColdCallerLink(leadNumber, callSid);
      }
      const response = new VoiceResponse();
      await appendSpeech(response, state.declineLine || 'No worries — I just texted you the link. Have a great day!', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.hangup();
      return res.type('text/xml').send(response.toString());
    }

    if (isAffirmativeSpeech(speech) || hasIntent) {
      const nextState = hasIntent && intent !== state.prefillIntent
        ? { ...state, prefillIntent: intent }
        : state;
      const response = new VoiceResponse();
      await appendSpeech(response, state.positiveAckLine || 'Appreciate it.', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });

      const qualifyState = encodeCallState(nextState);
      const qualifyAction = absoluteUrl(`/voice/cold-caller/qualify?state=${qualifyState}`, appBaseUrl);
      const gather = response.gather({
        input: 'speech',
        method: 'POST',
        action: qualifyAction,
        speechTimeout: 'auto',
        timeout: 6,
        bargeIn: 'true',
        actionOnEmptyResult: true,
      });

      await appendSpeech(gather, state.valueBridgeLine || 'We spotted something worth a quick look.', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      gather.pause({ length: 1 });
      await appendSpeech(gather, state.qualifierQuestion || 'Are you looking to buy, sell, or invest in the next 90 days?', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });

      response.redirect(qualifyAction);
      return res.type('text/xml').send(response.toString());
    }

    const introLines = Array.isArray(state.introLines) ? state.introLines : [];
    const introNoSpeechCount = Number(state.introNoSpeechCount) || 0;

    if (!speech) {
      if (introNoSpeechCount >= 1) {
        if (leadNumber) {
          await sendColdCallerLink(leadNumber, callSid);
        }
        const response = new VoiceResponse();
        await appendSpeech(response, state.smsOfferLine || 'I just texted you a quick link so you can see the options.', {
          variantKey: variant,
          preset,
          baseUrl: appBaseUrl,
        });
        response.hangup();
        return res.type('text/xml').send(response.toString());
      }

      const nextState = { ...state, introNoSpeechCount: introNoSpeechCount + 1 };
      const response = new VoiceResponse();
      const introState = encodeCallState(nextState);
      const introAction = absoluteUrl(`/voice/cold-caller/intro?state=${introState}`, appBaseUrl);
      const gather = response.gather({
        input: 'speech',
        method: 'POST',
        action: introAction,
        speechTimeout: 'auto',
        timeout: 5,
        bargeIn: 'true',
        actionOnEmptyResult: true,
      });

      for (let i = 0; i < introLines.length; i += 1) {
        await appendSpeech(gather, introLines[i], { variantKey: variant, preset, baseUrl: appBaseUrl });
        if (i < introLines.length - 1) {
          gather.pause({ length: 1 });
        }
      }

      response.redirect(introAction);
      return res.type('text/xml').send(response.toString());
    }

    const response = new VoiceResponse();
    const introState = encodeCallState(state);
    const introAction = absoluteUrl(`/voice/cold-caller/intro?state=${introState}`, appBaseUrl);
    const gather = response.gather({
      input: 'speech',
      method: 'POST',
      action: introAction,
      speechTimeout: 'auto',
      timeout: 5,
      bargeIn: 'true',
      actionOnEmptyResult: true,
    });

    for (let i = 0; i < introLines.length; i += 1) {
      await appendSpeech(gather, introLines[i], { variantKey: variant, preset, baseUrl: appBaseUrl });
      if (i < introLines.length - 1) {
        gather.pause({ length: 1 });
      }
    }

    response.redirect(introAction);
    return res.type('text/xml').send(response.toString());
  } catch (error) {
    console.error('[ColdCaller] Intro handler error', {
      message: error?.message || error,
      stack: error?.stack,
    });
    const { variant, preset } = resolveVariantPreset();
    const appBaseUrl = resolveAppBaseUrl(req);
    const fallback = new VoiceResponse();
    await appendSpeech(fallback, 'Thanks for your time. Talk soon!', {
      variantKey: variant,
      preset,
      baseUrl: appBaseUrl,
    });
    fallback.hangup();
    return res.type('text/xml').send(fallback.toString());
  }
});

app.post('/voice/cold-caller/qualify', async (req, res) => {
  try {
    const stateToken = typeof req.query?.state === 'string' ? req.query.state : '';
    const state = decodeCallState(stateToken) || {};
    const appBaseUrl = resolveAppBaseUrl(req);
    const { variant, preset } = resolveVariantPreset(state.variant);
    const prefillIntent = typeof state.prefillIntent === 'string' ? state.prefillIntent : '';
    let speech = normalizeSpeech(req.body?.SpeechResult);
    if (!speech && prefillIntent) {
      speech = prefillIntent;
    }
    const callSid = req.body?.CallSid || '';
    const leadNumber = extractLeadNumber(req.body);
    const wantsLinkNow = requestsLink(speech);

    const qualifyNoSpeechCount = Number(state.qualifyNoSpeechCount) || 0;

    if (!speech) {
      if (qualifyNoSpeechCount >= 1) {
        if (leadNumber) {
          await sendColdCallerLink(leadNumber, callSid);
        }
        const response = new VoiceResponse();
        await appendSpeech(response, state.smsOfferLine || 'I just texted you a quick link so you can see the options.', {
          variantKey: variant,
          preset,
          baseUrl: appBaseUrl,
        });
        response.hangup();
        return res.type('text/xml').send(response.toString());
      }

      const nextState = { ...state, qualifyNoSpeechCount: qualifyNoSpeechCount + 1 };
      const response = new VoiceResponse();
      const qualifyState = encodeCallState(nextState);
      const qualifyAction = absoluteUrl(`/voice/cold-caller/qualify?state=${qualifyState}`, appBaseUrl);
      const gather = response.gather({
        input: 'speech',
        method: 'POST',
        action: qualifyAction,
        speechTimeout: 'auto',
        timeout: 6,
        bargeIn: 'true',
        actionOnEmptyResult: true,
      });
      await appendSpeech(gather, state.qualifierQuestion || 'Are you looking to buy, sell, or invest in the next 90 days?', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.redirect(qualifyAction);
      return res.type('text/xml').send(response.toString());
    }

    if (isNegativeSpeech(speech) && !wantsLinkNow) {
      if (leadNumber) {
        await sendColdCallerLink(leadNumber, callSid);
      }
      const response = new VoiceResponse();
      await appendSpeech(response, state.declineLine || 'No worries — I just texted you the link. Have a great day!', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.hangup();
      return res.type('text/xml').send(response.toString());
    }

    if (leadNumber) {
      await sendColdCallerLink(leadNumber, callSid);
    }

    const intent = detectLeadIntent(speech);
    const intentLineMap = {
      buy: 'Great — we will send over buying options that fit.',
      sell: 'Great — let us talk about getting top dollar for you.',
      invest: 'Awesome — I have a couple investment plays to text you.',
      rent: 'Perfect — I will text over the top rentals right now.',
    };
    const intentLine = intentLineMap[intent] || state.valueBridgeLine || 'Perfect — let me text you the details.';

    const response = new VoiceResponse();
    await appendSpeech(response, intentLine, {
      variantKey: variant,
      preset,
      baseUrl: appBaseUrl,
    });
    await appendSpeech(response, state.smsOfferLine || 'I just texted you a quick link so you can see the options.', {
      variantKey: variant,
      preset,
      baseUrl: appBaseUrl,
    });

    if (state.handoffNumber) {
      const handoffState = encodeCallState(state);
      const handoffAction = absoluteUrl(`/voice/cold-caller/handoff?state=${handoffState}`, appBaseUrl);
      const gather = response.gather({
        input: 'speech',
        method: 'POST',
        action: handoffAction,
        speechTimeout: 'auto',
        timeout: 5,
        bargeIn: 'true',
        actionOnEmptyResult: true,
      });
      await appendSpeech(gather, state.handoffPromptLine || 'Want me to connect you with a teammate now?', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.redirect(handoffAction);
    } else {
      await appendSpeech(response, 'Talk soon!', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.hangup();
    }

    return res.type('text/xml').send(response.toString());
  } catch (error) {
    console.error('[ColdCaller] Qualify handler error', {
      message: error?.message || error,
      stack: error?.stack,
    });
    const { variant, preset } = resolveVariantPreset();
    const appBaseUrl = resolveAppBaseUrl(req);
    const fallback = new VoiceResponse();
    await appendSpeech(fallback, 'Appreciate your time — we will follow up by text.', {
      variantKey: variant,
      preset,
      baseUrl: appBaseUrl,
    });
    fallback.hangup();
    return res.type('text/xml').send(fallback.toString());
  }
});

app.post('/voice/cold-caller/handoff', async (req, res) => {
  try {
    const stateToken = typeof req.query?.state === 'string' ? req.query.state : '';
    const state = decodeCallState(stateToken) || {};
    const appBaseUrl = resolveAppBaseUrl(req);
    const { variant, preset } = resolveVariantPreset(state.variant);
    const speech = normalizeSpeech(req.body?.SpeechResult);
    const callSid = req.body?.CallSid || '';
    const leadNumber = extractLeadNumber(req.body);
    const wantsLinkNow = requestsLink(speech);
    const handoffNumber = state.handoffNumber || '';

    if (!handoffNumber) {
      const response = new VoiceResponse();
    await appendSpeech(response, state.smsOfferLine || 'I will follow up with the details shortly.', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.hangup();
      return res.type('text/xml').send(response.toString());
    }

    if (!speech) {
      const response = new VoiceResponse();
      const handoffState = encodeCallState(state);
      const handoffAction = absoluteUrl(`/voice/cold-caller/handoff?state=${handoffState}`, appBaseUrl);
      const gather = response.gather({
        input: 'speech',
        method: 'POST',
        action: handoffAction,
        speechTimeout: 'auto',
        timeout: 5,
        bargeIn: 'true',
        actionOnEmptyResult: true,
      });
      await appendSpeech(gather, state.handoffPromptLine || 'Want me to connect you with a teammate now?', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.redirect(handoffAction);
      return res.type('text/xml').send(response.toString());
    }

    if (isAffirmativeSpeech(speech)) {
      const response = new VoiceResponse();
      await appendSpeech(response, state.connectLine || 'One moment while I connect you to a teammate.', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      clearCallSmsRecord(callSid);
      response.dial(handoffNumber);
      return res.type('text/xml').send(response.toString());
    }

    if (isNegativeSpeech(speech) || wantsLinkNow) {
      if (leadNumber) {
        await sendColdCallerLink(leadNumber, callSid);
      }
      const response = new VoiceResponse();
      await appendSpeech(response, state.smsOfferLine || 'No worries — I just texted you the link.', {
        variantKey: variant,
        preset,
        baseUrl: appBaseUrl,
      });
      response.hangup();
      return res.type('text/xml').send(response.toString());
    }

    const response = new VoiceResponse();
    const handoffState = encodeCallState(state);
    const handoffAction = absoluteUrl(`/voice/cold-caller/handoff?state=${handoffState}`, appBaseUrl);
    const gather = response.gather({
      input: 'speech',
      method: 'POST',
      action: handoffAction,
      speechTimeout: 'auto',
      timeout: 5,
      bargeIn: 'true',
      actionOnEmptyResult: true,
    });
    await appendSpeech(gather, state.handoffPromptLine || 'Want me to connect you with a teammate now?', {
      variantKey: variant,
      preset,
      baseUrl: appBaseUrl,
    });
    response.redirect(handoffAction);
    return res.type('text/xml').send(response.toString());
  } catch (error) {
    console.error('[ColdCaller] Handoff handler error', {
      message: error?.message || error,
      stack: error?.stack,
    });
    const { variant, preset } = resolveVariantPreset();
    const appBaseUrl = resolveAppBaseUrl(req);
    const fallback = new VoiceResponse();
    await appendSpeech(fallback, 'Appreciate your time — we will follow up by text.', {
      variantKey: variant,
      preset,
      baseUrl: appBaseUrl,
    });
    fallback.hangup();
    return res.type('text/xml').send(fallback.toString());
  }
});

// ---------------------------------------------------------------------
// Stripe Checkout: subscription ($150/mo) + one-time setup ($300)
// Supports promo "DELCO150" via env STRIPE_COUPON_DELCO150
// ---------------------------------------------------------------------
app.post('/api/create-checkout-session', async (req, res) => {
  try {
    const promo = (req.body?.promo || '').trim().toUpperCase();

    const params = {
      mode: 'subscription',
      line_items: [
        { price: process.env.STRIPE_PRICE_SUB, quantity: 1 },   // $150/mo
        { price: process.env.STRIPE_PRICE_SETUP, quantity: 1 }, // $300 once
      ],
      allow_promotion_codes: true,
      success_url: `${process.env.APP_BASE_URL}/thank-you?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${process.env.APP_BASE_URL}/checkout?canceled=1`,
    };

    if (promo === 'DELCO150' && process.env.STRIPE_COUPON_DELCO150) {
      params.discounts = [{ coupon: process.env.STRIPE_COUPON_DELCO150 }];
    }

    const session = await stripe.checkout.sessions.create(params);
    return res.json({ id: session.id });
  } catch (e) {
    console.error('Stripe session error:', e?.raw?.message || e?.message, e);
    return res.status(500).json({ error: 'stripe_error' });
  }
});

app.post('/api/dental/checkout', async (req, res) => {
  try {
    const slug = String(req.body?.plan || '').toLowerCase();
    const plan = DENTAL_PLANS.find(p => p.slug === slug);

    if (!plan) {
      return res.status(400).json({ error: 'invalid_plan' });
    }

    const lineItems = [];

    if (plan.monthlyCents) {
      lineItems.push({
        price_data: {
          currency: 'usd',
          unit_amount: plan.monthlyCents,
          recurring: { interval: 'month' },
          product_data: {
            name: `${plan.name} — Monthly Subscription`,
          },
        },
        quantity: 1,
      });
    }

    if (plan.setupCents) {
      lineItems.push({
        price_data: {
          currency: 'usd',
          unit_amount: plan.setupCents,
          product_data: {
            name: `${plan.name} — Onboarding & Setup`,
          },
        },
        quantity: 1,
      });
    }

    if (!lineItems.length) {
      return res.status(400).json({ error: 'unsupported_plan' });
    }

    const session = await stripe.checkout.sessions.create({
      mode: 'subscription',
      allow_promotion_codes: true,
      line_items: lineItems,
      success_url: `${APP_BASE_URL}/dental/thank-you.html?plan=${plan.slug}&session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${APP_BASE_URL}/dental/checkout.html?plan=${plan.slug}&canceled=1`,
      metadata: {
        productLine: 'dental-ai',
        plan: plan.slug,
      },
    });

    if (session?.url) {
      return res.json({ url: session.url });
    }

    return res.json({ id: session.id });
  } catch (error) {
    console.error('Dental checkout error:', error?.raw?.message || error?.message, error);
    return res.status(500).json({ error: 'stripe_error' });
  }
});

// Xbox 360 store: $5-per-game cart → Stripe Checkout
app.post('/api/store/checkout', async (req, res) => {
  try {
    const { items = [] } = req.body || {};
    if (!Array.isArray(items) || !items.length) {
      return res.status(400).json({ error: 'No items' });
    }

    const line_items = items.map(it => ({
      price_data: {
        currency: 'usd',
        unit_amount: Math.round((Number(it.price) || 5) * 100),
        product_data: {
          name: `${String(it.title || 'Xbox 360 Game').slice(0,120)} (Xbox 360)`,
          metadata: { sku: it.id || '' }
        }
      },
      quantity: 1
    }));

    const base = process.env.PUBLIC_BASE_URL || `${req.protocol}://${req.get('host')}`;
    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      payment_method_types: ['card', 'link'],
      line_items,
      allow_promotion_codes: true,
      success_url: `${base}/thank-you?store=1&session_id={CHECKOUT_SESSION_ID}`,
      cancel_url:  `${base}/x360.html?cancel=1`,
      metadata: { source: 'x360-store' }
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error('stripe store checkout error', err);
    res.status(500).json({ error: 'stripe_unavailable' });
  }
});


// Pretty routes for static pages
app.get('/checkout', (_, res) =>
  res.sendFile(path.join(__dirname, 'public', 'checkout.html'))
);
app.get('/thank-you', (_, res) =>
  res.sendFile(path.join(__dirname, 'public', 'thank-you.html'))
);

app.get('/x360', (_, res) =>
  res.sendFile(path.join(__dirname, 'public', 'x360.html'))
);


// Demo eligibility endpoint (stub). Swap this logic for a real clearinghouse later.
app.post('/api/eligibility/check', async (req, res) => {
  try {
    const { payer, memberId, lastName, dob, zip } = req.body || {};
    // --- STUB LOGIC (for demo only) ---
    // Consider even last digit "eligible" to demo success/fail states.
    const ok = memberId && /\d$/.test(memberId);
    const eligible = ok ? (parseInt(memberId.slice(-1), 10) % 2 === 0) : false;

    return res.json({
      ok: true,
      eligible,
      planName: eligible ? `${(payer||'Dental PPO')}` : null,
      copayEstimate: eligible ? 30 : null, // demo number
      echo: { payer, lastName, dob, zip }   // for your inspection; remove later
    });
  } catch (e) {
    console.error('eligibility error', e);
    return res.status(500).json({ ok:false, error:'server_error' });
  }
});


// ---------------------------------------------------------------------
// 🔥 NEW: OpenAI chat endpoint
// ---------------------------------------------------------------------
// --- Chat endpoint (logs to JSONL & returns reply) ---

app.post('/api/chat', async (req, res) => {
  try {
    const convId = String(req.body?.convId || crypto.randomUUID());
    const incoming = Array.isArray(req.body?.messages) ? req.body.messages : [];

    // recent, trimmed history
    const history = incoming.slice(-10).map(m => ({
      role: m.role === 'assistant' ? 'assistant' : 'user',
      content: String(m.content || '').slice(0, 2000),
    }));

    const systemPersona = `
You are "Mikey from Delco Tech" — a friendly, straight-shooting helper for HVAC & plumbing crews in Delaware County, PA.
Goals: explain briefly, then CLOSE. If the user shows buying intent (e.g., "yes", "sign me up", "ready", "buy", "activate"), respond with one short confirmation and include exactly one clear checkout link:
Checkout: ${(process.env.APP_BASE_URL || '')}/checkout
Also offer the 10-min setup call only if they ask for a call.
Keep replies 1–3 sentences, practical, Delco-casual unless they ask for details. Never say you're AI.`;

    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      temperature: 0.6,
      max_tokens: 300,
      messages: [
        { role: 'system', content: systemPersona },
        ...history
      ],
    });

    const reply = completion.choices?.[0]?.message?.content || 'Got it.';

    // Append a JSON line for analytics / training later
    const logLine = {
      ts: new Date().toISOString(),
      convId,
      ip: req.headers['x-forwarded-for'] || req.ip || '',
      ua: req.headers['user-agent'] || '',
      lastUser: history[history.length - 1]?.content || '',
      reply
    };
    fs.appendFile('chatlogs.jsonl', JSON.stringify(logLine) + '\n', () => {});

    res.json({ reply, convId });
  } catch (err) {
    console.error('Chat error:', err?.message || err);
    res.status(500).json({ error: 'chat_error' });
  }
});


// ---------------------------------------------------------------------
// Real estate script generator (OpenAI-powered)
// ---------------------------------------------------------------------
const buildRealEstateFallback = ({ tone, focus, location, nextAction }) => {
  const ctaMap = {
    checkout: 'I can text you a secure Stripe link to lock in the spot right now.',
    book: 'I can drop my booking calendar in your text so you can pick a time that works.',
    transfer: 'Let me patch in my senior closer so you can dive into the numbers together.'
  };
  return [
    `Opening: Hi, this is the Delco Tech real estate desk checking in about ${focus || 'your plans'} in ${location || 'the area'}.`,
    'Discovery: Confirm timeline, financing, and any hurdles that might slow you down.',
    'Value: Remind them we work free-first with public data, comps, and vetted vendor partners.',
    `CTA: ${ctaMap[nextAction] || ctaMap.checkout}`,
    `Tone: ${tone || 'friendly'} and compliance-first (respect DNC & fair housing).`
  ].join('\n');
};

app.post('/api/real-estate-script', async (req, res) => {
  try {
    const { leadType = 'buyer', tone = 'friendly', location = '', focus = '', nextAction = 'checkout' } = req.body || {};

    if (!process.env.OPENAI_API_KEY) {
      return res.json({
        script: buildRealEstateFallback({ tone, focus, location, nextAction }),
        note: 'OpenAI not configured on this deployment. Showing template copy instead.'
      });
    }

    const nextActionText = {
      checkout: `Offer to text them the secure Stripe checkout at ${(process.env.APP_BASE_URL || APP_BASE_URL)}/checkout so they can lock in the spot without fees.`,
      book: `Drive to the Calendly link ${(process.env.APP_BASE_URL || APP_BASE_URL)}/book for a live consult — mention evening slots too.`,
      transfer: 'Let them know you can transfer them to the live acquisitions floor immediately for deeper numbers.'
    };

    const systemPersona = `
You are "Skye" — an inside sales agent for a real estate investment team. You're sharp, empathetic, and stay compliant with DNC and fair-housing rules.
Keep scripts under 220 words. Use short paragraphs or bullet points.
Mention that we start with free public data sources (FSBO, MLS expired, notices) before any paid leads.
Always end with one clear call-to-action based on the option provided.
Reference secure payments via Stripe Checkout and scheduling via Calendly when relevant.
Speak confidently, but stay human — no AI disclaimers.
`;

    const userPrompt = `Lead type: ${leadType}\nTone: ${tone}\nMarket focus: ${location || 'general market'}\nKey pain point: ${focus || 'not provided'}\nCTA instruction: ${nextActionText[nextAction] || nextActionText.checkout}\n\nCraft a quick cold-call script with sections for Opening, Discovery, Value, and Close. Include an objection-handling tip. Mention that data comes from public/free sources where it fits.`;

    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      temperature: 0.65,
      max_tokens: 350,
      messages: [
        { role: 'system', content: systemPersona },
        { role: 'user', content: userPrompt }
      ],
    });

    const script = completion.choices?.[0]?.message?.content?.trim() || buildRealEstateFallback({ tone, focus, location, nextAction });

    res.json({
      script,
      note: `Generated with OpenAI for a ${tone} ${leadType}.`
    });
  } catch (err) {
    console.error('Real estate script error:', err?.message || err);
    const { leadType = 'buyer', tone = 'friendly', location = '', focus = '', nextAction = 'checkout' } = req.body || {};
    res.status(200).json({
      script: buildRealEstateFallback({ tone, focus, location, nextAction }),
      note: 'OpenAI unavailable right now — fallback copy provided.'
    });
  }
});


// ---------------------------------------------------------------------
// Twilio Voice: forward, then detect missed calls
// ---------------------------------------------------------------------
app.post('/voice', (req, res) => {
  const VoiceResponse = require('twilio').twiml.VoiceResponse;
  const twiml = new VoiceResponse();

  const dial = twiml.dial({ action: '/voice/after', timeout: DIAL_TIMEOUT });
  dial.number(process.env.FORWARD_TO_NUMBER);

  twiml.say('Sorry, we were unable to connect your call. We will text you shortly.');
  res.type('text/xml').send(twiml.toString());
});

app.post('/voice/after', async (req, res) => {
  const callStatus = req.body.DialCallStatus; // 'completed' | 'busy' | 'no-answer' | 'failed'
  const from = req.body.From;

  const VoiceResponse = require('twilio').twiml.VoiceResponse;
  const twiml = new VoiceResponse();
  twiml.hangup();
  res.type('text/xml').send(twiml.toString());

  if (['busy', 'no-answer', 'failed'].includes(callStatus)) {
    setStep(from, 'ask_name');
    await upsertByPhone(from, { status: 'opened' });
    await sendSMS(
      from,
      `Hey, it's ${BUSINESS}. Sorry we missed your call. What's your name? ` +
      `Book anytime: ${CAL_LINK} — Reply STOP to stop, HELP for help.`,
      {
        source: 'missed_call_follow_up',
        callStatus,
      }
    );
  }
});

app.post('/api/admin/send-text', async (req, res) => {
  try {
    const to = normalizePhoneNumber(req.body?.to);
    const rawMessage = typeof req.body?.message === 'string' ? req.body.message.trim() : '';
    if (!to) {
      return res.status(400).json({ error: 'invalid_to' });
    }

    const link = COLD_CALLER_SMS_LINK;
    const baseMessage = rawMessage || 'Quick follow-up from DelcoTech — ready to automate your inbound calls and bookings.';
    const finalMessage = baseMessage.includes(link) ? baseMessage : `${baseMessage}\n${link}`;

    console.info('[Admin] Manual SMS request', { to: maskPhoneNumberForLog(to) });
    await sendSMS(to, finalMessage, {
      source: 'admin_manual_sms',
    });

    return res.json({ ok: true });
  } catch (error) {
    console.error('[Admin] Manual SMS failed', { message: error?.message || error });
    res.status(500).json({ error: 'sms_failed' });
  }
});

// ---------------------------------------------------------------------
// Twilio SMS: name -> need -> Calendly link
// ---------------------------------------------------------------------
app.post('/sms', async (req, res) => {
  const MessagingResponse = require('twilio').twiml.MessagingResponse;
  const twiml = new MessagingResponse();

  const from = req.body.From;
  const body = (req.body.Body || '').trim();
  const s = getState(from);

  if (/^help$/i.test(body)) {
    twiml.message(`Reply STOP to opt-out. To book directly: ${CAL_LINK}`);
    return res.type('text/xml').send(twiml.toString());
  }

  if (!s || !s.step) {
    setStep(from, 'ask_name');
    await upsertByPhone(from, { status: 'opened' });
    twiml.message(`Hey, it's ${BUSINESS}. What's your name?`);
    return res.type('text/xml').send(twiml.toString());
  }

  if (s.step === 'ask_name') {
    setField(from, 'name', body);
    setStep(from, 'ask_need');
    await upsertByPhone(from, { name: body, status: 'qualified' });
    twiml.message(`Nice to meet you, ${body}. What can we help you with?`);
    return res.type('text/xml').send(twiml.toString());
  }

  if (s.step === 'ask_need') {
    setField(from, 'need', body);
    setStep(from, 'book');
    await upsertByPhone(from, { need: body, status: 'qualified' });
    twiml.message(
      `Got it. You can book here: ${CAL_LINK}\n` +
      `If you prefer, reply with a preferred day/time and we’ll confirm by text.`
    );
    return res.type('text/xml').send(twiml.toString());
  }

  await upsertByPhone(from, { status: 'awaiting_booking' });
  twiml.message(`Thanks! We’ll confirm shortly. You can also self-book anytime: ${CAL_LINK}`);
  return res.type('text/xml').send(twiml.toString());
});

// ---------------------------------------------------------------------
// Calendly webhook → mark bookings / cancellations
// ---------------------------------------------------------------------
app.post('/calendly/webhook', async (req, res) => {
  try {
    const event = req.body?.event;
    const payload = req.body?.payload;
    if (!event || !payload) return res.status(400).json({ ok: false });

    if (event === 'invitee.created') {
      const phone = payload?.invitee?.text_reminder_number || '';
      const start = payload?.event?.start_time;
      const end = payload?.event?.end_time;
      const ev = payload?.event?.uri || '';
      if (phone) {
        await upsertByPhone(phone, {
          status: 'booked',
          appt_start: start || '',
          appt_end: end || '',
          calendly_event: ev || ''
        });
      }
    }

    if (event === 'invitee.canceled') {
      const phone = payload?.invitee?.text_reminder_number || '';
      if (phone) await upsertByPhone(phone, { status: 'canceled' });
    }

    return res.json({ ok: true });
  } catch (e) {
    console.error('Calendly webhook error:', e);
    return res.status(500).json({ ok: false });
  }
});

// ---------------------------------------------------------------------
// Review request cron (every 5m, 2h after appt_end)
// ---------------------------------------------------------------------
cron.schedule('*/5 * * * *', async () => {
  try {
    if (!REVIEW_LINK) return;
    const rows = await findAll();
    const header = rows[0] || [];
    const idx = (name) => header.indexOf(name);

    const now = dayjs();

    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      const phone = r[idx('phone')];
      const status = r[idx('status')];
      const apptEnd = r[idx('appt_end')];
      if (!phone || !apptEnd) continue;

      const due = now.isAfter(dayjs(apptEnd).add(2, 'hour'));
      const alreadySent = status && status.includes('review_sent');

      if (status === 'booked' && due && !alreadySent) {
        await sendSMS(
          phone,
          `Thanks for visiting ${BUSINESS}! Mind leaving a quick review? ${REVIEW_LINK}`,
          {
            source: 'review_request_cron',
            apptEnd,
          }
        );
        await upsertByPhone(phone, { status: 'review_sent' });
      }
    }
  } catch (e) {
    console.error('Review cron error:', e.message);
  }
});

// Simple health check (optional)
app.get('/api/health', async (req, res) => {
  try {
    // Optional: ping Stripe to confirm key works
    const ok = !!process.env.STRIPE_SECRET_KEY;
    res.json({ ok, stripe: ok, public: APP_BASE_URL });
  } catch {
    res.json({ ok: false, stripe: false, public: APP_BASE_URL });
  }
});

// Optional lightweight lead log so the front-end "begin_checkout" call doesn't 404
/* ───────────────────────── HIRE CATALOG + PRICING ───────────────────────── */

const HIRE_SERVICES = [
  {
    key: 'discord_server',
    title: 'Discord Server — 24h Setup/Overhaul',
    tagline: 'Bots • Roles • Automations • Clean structure',
    blurb: 'Sleek, organized, bot-enhanced setup to engage members and keep mods in control.',
    tiers: [
      { key:'starter',  label:'Starter',  price:  30 },
      { key:'standard', label:'Standard', price:  40 },
      { key:'advanced', label:'Advanced', price:  50 },
    ],
    addons: [
      { key:'extra_rev',        label:'Additional Revision',       price:25 },
      { key:'db_integration',   label:'Database Integration',      price:25 },
      { key:'ai_faq',           label:'AI FAQ Bot',                price:15 },
      { key:'ai_welcome',       label:'AI Welcome Bot',            price:10 },
      { key:'npc_crystal',      label:'24/7 Crystal NPC Bot',      price:20 },
    ]
  },
  {
    key: 'viewer_prediction',
    title: 'Skill-Based Viewer Prediction Game (PvP Wagers)',
    tagline: 'TikTok/Twitch/YT chat picks • wallets • payouts • OBS overlay',
    blurb: 'Plug-and-play engine for AI-vs-AI shows with odds, streak bonuses, auto-settlement, ledgers.',
    tiers: [
      { key:'starter',  label:'Starter',  price:  499 },
      { key:'standard', label:'Standard', price: 1199 },
      { key:'advanced', label:'Advanced', price: 2499 },
    ],
    addons: [
      { key:'fast',           label:'Fast Delivery',               price:250 },
      { key:'extra_platform', label:'Extra Platform (TikTok/Twitch/YT)', price:150 },
      { key:'stripe_paypal',  label:'Stripe/PayPal Payments',      price:400 },
      { key:'discord_bot',    label:'Discord Bot Integration',     price:300 },
    ]
  },
  {
    key: 'ai_quote_pay_book',
    title: 'AI Quote → Pay → Book (Stripe • Calendly • Twilio)',
    tagline: 'Turn visits into scheduled, paid jobs',
    blurb: 'Asks smart questions, calculates price, collects payment, locks the appointment, logs to Sheets.',
    tiers: [
      { key:'starter',  label:'Starter',  price:  299 },
      { key:'standard', label:'Standard', price:  699 },
      { key:'advanced', label:'Advanced', price: 1199 },
    ],
    addons: [
      { key:'add_platform',    label:'Add a messaging platform',   price:200 },
      { key:'sms_compliance',  label:'SMS Compliant Registration', price:250 },
      { key:'custom_calc',     label:'Custom calculator fields',   price:150 },
    ]
  },
  {
    key: 'discord_arena',
    title: 'Discord AI Arena — PvP Bets & Leaderboards',
    tagline: 'Virtual wagers • roles via Stripe • scheduled fights',
    blurb: 'Challenge/accept bets, auto-resolve, standings in embeds with buttons. Admin tools included.',
    tiers: [
      { key:'starter',  label:'Starter',  price:  299 },
      { key:'standard', label:'Standard', price:  699 },
      { key:'advanced', label:'Advanced', price: 1299 },
    ],
    addons: [
      { key:'extra_server', label:'+1 Discord Server',             price: 99  },
      { key:'paypal',       label:'Add PayPal Checkout',           price: 150 },
      { key:'dashboard',    label:'Admin Web Dashboard',           price: 300 },
    ]
  },
  {
    key: 'crypto_frontend',
    title: 'Crypto Exchange Frontend (Next.js + Charts)',
    tagline: 'Candles + volume • EMA/SMA • crosshair • wagmi/viem opt-in',
    blurb: 'Performance-oriented, mobile-first token market + detail pages with trader-grade UX.',
    tiers: [
      { key:'starter',  label:'Starter',  price:  349 },
      { key:'standard', label:'Standard', price:  799 },
      { key:'advanced', label:'Advanced', price: 1299 },
    ],
    addons: [
      { key:'fast',        label:'Fast Delivery',                 price: 99  },
      { key:'extra_rev',   label:'Additional Revision',           price: 49  },
      { key:'branding',    label:'Branding & Theme Pack',         price: 150 },
      { key:'wallet',      label:'Wallet Layer (wagmi/viem)',     price: 200 },
      { key:'api',         label:'API/Data Integration',          price: 300 },
    ]
  },
  {
    key: 'web_rescue',
    title: 'Rapid Recovery: Site/Email • DNS/SSL/MX',
    tagline: 'Fix downtime • restore email • secure HTTPS',
    blurb: 'Triage + verified fix with proof (validation screenshots & tests).',
    tiers: [
      { key:'starter',  label:'Starter',  price:   60 },
      { key:'standard', label:'Standard', price:  160 },
      { key:'advanced', label:'Advanced', price:  320 },
    ],
    addons: [
      { key:'extra_rev',     label:'Additional Revision',         price: 24.99 },
      { key:'urgent4',       label:'Urgent 4-hour response',      price: 75    },
      { key:'cloudflare',    label:'Migrate DNS to Cloudflare',   price: 120   },
      { key:'email_migrate', label:'Email migration (Google/M365)', price: 250 },
    ]
  },
  {
    key: 'hubspot_email',
    title: 'HubSpot Email Template — Polished & Tested',
    tagline: 'Responsive • modules • bulletproof buttons',
    blurb: 'Hand-coded, table-based HTML with inline styles, retina images, and Litmus-tested tweaks.',
    tiers: [
      { key:'starter',  label:'Starter',  price:  49.99 },
      { key:'standard', label:'Standard', price: 119.99 },
      { key:'advanced', label:'Advanced', price: 349.99 },
    ],
    addons: [
      { key:'fast',       label:'Fast Delivery',          price: 85  },
      { key:'extra_rev',  label:'Additional Revision',    price: 29.99 },
      { key:'editable',   label:'Editable Template',      price: 85  },
      { key:'darkmode',   label:'Dark mode optimization', price: 45  },
      { key:'gif_hero',   label:'Animated GIF hero',      price: 65  },
      { key:'variant',    label:'Extra variant',          price: 75  },
    ]
  }
];

function ddJwt(){
  const now = Math.floor(Date.now()/1000);
  const payload = { iss: DD_DEVELOPER_ID, aud: 'doordash', iat: now, exp: now + 60, kid: DD_KEY_ID };
  const header  = { 'dd-ver': 'DD-JWT-V1' };
  const secret  = Buffer.from(DD_SIGNING_B64, 'base64');
  return jwt.sign(payload, secret, { algorithm: 'HS256', header });
}

async function ddPost(path, body){
  const r = await fetch(`https://openapi.doordash.com${path}`, {
    method:'POST',
    headers:{ 'Authorization': `Bearer ${ddJwt()}`, 'Content-Type':'application/json' },
    body: JSON.stringify(body)
  });
  const json = await r.json().catch(()=> ({}));
  if(!r.ok) throw new Error(`DoorDash ${r.status}: ${JSON.stringify(json)}`);
  return json;
}

function cents(n){ return Math.max(0, Math.round(Number(n||0)*100)); }
function toE164Maybe(usPhone){ const d = String(usPhone||'').replace(/\D/g,''); return d.startsWith('1')? `+${d}` : `+1${d}`; }

app.post('/api/food/quote', async (req,res)=>{
  try{
    const { items=[], address={} } = req.body||{};
    const subtotalCents = items.reduce((s,it)=> s + (Number(it.priceCents)||0)*(Number(it.qty)||0), 0);
    const external_delivery_id = `FOOD-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;

    const pickup_address = `${STORE_ADDR}, ${STORE_CITY}, ${STORE_STATE} ${STORE_ZIP}`;
    const dropoff_address = `${address.addr1}${address.addr2? (', '+address.addr2):''}, ${address.city}, ${address.state} ${address.zip}`;

    const quote = await ddPost('/drive/v2/quotes', {
      external_delivery_id,
      pickup_address,
      pickup_business_name: STORE_NAME,
      pickup_phone_number: STORE_PHONE,
      dropoff_address,
      dropoff_contact_given_name: address.first || '',
      dropoff_contact_family_name: address.last  || '',
      dropoff_phone_number: toE164Maybe(address.phone),
      dropoff_instructions: address.notes || '',
      order_value: subtotalCents,
      items: items.map(it=>({ name: it.name, quantity: it.qty }))
    });

    res.json({ quote });
  }catch(e){
    res.status(400).json({ error:'quote_failed' });
  }
});

app.post('/api/food/checkout', async (req,res)=>{
  try{
    const { items=[], tipCents=0, quote={}, address={} } = req.body||{};
    if(!Array.isArray(items) || !items.length) return res.status(400).json({ error:'no_items' });
    if(!quote?.externalId || !quote?.feeCents) return res.status(400).json({ error:'no_quote' });

    const line_items = [];

    // Food items
    for(const it of items){
      const unit = Math.max(100, Math.round(Number(it.priceCents)||0));
      const qty  = Math.max(1, Number(it.qty)||1);
      line_items.push({
        price_data:{ currency:'usd', unit_amount: unit, product_data:{ name: it.name } },
        quantity: qty
      });
    }

    // Tax (simple: subtotal * TAX_RATE)
    const subtotalCents = items.reduce((s,it)=> s + (Number(it.priceCents)||0)*(Number(it.qty)||0), 0);
    const taxCents = Math.round(subtotalCents * TAX_RATE);
    if (taxCents > 0){
      line_items.push({ price_data:{ currency:'usd', unit_amount: taxCents, product_data:{ name:'Sales Tax' } }, quantity:1 });
    }

    // Delivery + Tip
    line_items.push({ price_data:{ currency:'usd', unit_amount: Math.max(100, Number(quote.feeCents)||0), product_data:{ name:'Delivery (DoorDash)' } }, quantity:1 });
    if (tipCents > 0){
      line_items.push({ price_data:{ currency:'usd', unit_amount: Math.round(Number(tipCents)||0), product_data:{ name:'Dasher Tip' } }, quantity:1 });
    }

    const session = await stripe.checkout.sessions.create({
      mode:'payment',
      payment_method_types:['card','link'],
      line_items,
      success_url:`${APP_BASE_URL}/food-confirm?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url:`${APP_BASE_URL}/food.html?canceled=1`,
      metadata:{
        flow:'food',
        dd_ext_id: quote.externalId,
        dd_fee: String(quote.feeCents||0),
        cust_phone_e164: toE164Maybe(address.phone||''),
        tip_cents: String(tipCents||0),
        drop_addr: `${address.addr1}${address.addr2?(', '+address.addr2):''}, ${address.city}, ${address.state} ${address.zip}`,
        drop_name: `${address.first||''} ${address.last||''}`.trim()
      }
    });

    res.json({ url: session.url });
  }catch(e){
    res.status(500).json({ error:'stripe_error' });
  }
});

// After Stripe redirect, accept the quote (start delivery) and send them to Thank You
app.get('/food-confirm', async (req,res)=>{
  try{
    const sid = String(req.query.session_id||'');
    const sess = await stripe.checkout.sessions.retrieve(sid);
    if (sess.payment_status !== 'paid') return res.redirect('/food.html?canceled=1');

    const ext = sess.metadata?.dd_ext_id;
    const tip = Number(sess.metadata?.tip_cents||0);
    const phone = sess.metadata?.cust_phone_e164 || '';

    // Accept quote (creates the delivery). You can also capture track URL from response.
    const resp = await ddPost(`/drive/v2/quotes/${ext}/accept`, { tip, dropoff_phone_number: phone });

    const track = encodeURIComponent(resp.tracking_url || '');
    return res.redirect(`/thank-you?food=1${track?('&track='+track):''}`);
  }catch(e){
    res.redirect('/thank-you?food=1');
  }
});

function roundMoney(n){ return Math.round(n*100)/100; }
function baseTierPrice(serviceKey, tierKey){
  const svc = HIRE_SERVICES.find(s=>s.key===serviceKey);
  const t = svc?.tiers.find(t=>t.key===tierKey);
  return t ? Number(t.price) : 0;
}
function addonsSum(serviceKey, addonKeys=[]){
  const svc = HIRE_SERVICES.find(s=>s.key===serviceKey);
  const priceByKey = new Map((svc?.addons||[]).map(a=>[a.key, Number(a.price)]));
  return (addonKeys||[]).reduce((s,k)=> s + (priceByKey.get(k)||0), 0);
}
function hireModifiersMultiplier(mod={}) {
  let m = 1.0;
  m *= (mod.urgency==='rush') ? 1.20 : (mod.urgency==='ultra' ? 1.35 : 1.00);
  m *= (mod.complexity==='moderate') ? 1.12 : (mod.complexity==='advanced' ? 1.25 : 1.00);
  return m;
}

// ±10% AI nudge using your existing OpenAI client (safe to skip if no key)
async function aiHireAdjust(anchor, selection, mod){
  if (!process.env.OPENAI_API_KEY) return anchor;
  try{
    const sys = 'You adjust quotes for software/game gigs. Given a base USD price, apply a small rational adjustment within ±10% based on risk and scope. Return ONLY a number.';
    const usr = `Base: ${anchor}. Service: ${selection.serviceKey}, Tier: ${selection.tierKey}, Addons: ${selection.addons?.join(',')||'none'}. Modifiers: ${JSON.stringify(mod)}.`;
    const r = await openai.chat.completions.create({
      model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
      temperature: 0.2,
      messages: [{role:'system',content:sys},{role:'user',content:usr}]
    });
    const txt = (r.choices?.[0]?.message?.content || '').trim();
    const val = Number(String(txt).replace(/[^\d.]/g,''));
    if (!isNaN(val) && val>0) return val;
  }catch{}
  return anchor;
}

function priceEnvelope(anchor){
  const payNow = anchor * 0.95;   // 5% off
  const deposit = Math.max(49, anchor * 0.20);
  return {
    anchor: roundMoney(anchor),
    payNow: roundMoney(payNow),
    deposit: roundMoney(deposit),
    currency:'usd'
  };
}

async function computeHirePrice({ selection=null, cart=null, modifiers={} }){
  let sum = 0;
  if (selection){
    const base = baseTierPrice(selection.serviceKey, selection.tierKey) + addonsSum(selection.serviceKey, selection.addons);
    const withMods = base * hireModifiersMultiplier(modifiers);
    sum += await aiHireAdjust(withMods, selection, modifiers);
  } else if (Array.isArray(cart)){
    for (const it of cart) sum += Number(it.amount)||0; // items already priced
    sum *= hireModifiersMultiplier(modifiers);
    if (cart.length >= 3) sum *= 0.97; // small bundle discount
  }
  return priceEnvelope(sum);
}

// Catalog
app.get('/api/hire/services', (req,res)=> res.json({ services: HIRE_SERVICES }));

// Quote (single selection OR whole cart)
app.post('/api/hire/quote', async (req,res)=>{
  try{
    const { selection=null, cart=null, modifiers={} } = req.body||{};
    const pricing = await computeHirePrice({ selection, cart, modifiers });
    const summary = selection
      ? `${selection.serviceKey} • ${selection.tierKey}${(selection.addons?.length? ' • +' + selection.addons.length + ' add-on(s)':'')}`
      : `Cart (${(cart||[]).length} item${(cart||[]).length===1?'':'s'})`;
    const breakdown = selection ? `Tier + add-ons × urgency/complexity` : `Sum of items × urgency/complexity`;
    res.json({ pricing, summary, breakdown });
  }catch(e){
    res.status(500).json({ error:'hire_quote_failed' });
  }
});

// Multi-item Stripe checkout
app.post('/api/hire/checkout', async (req,res)=>{
  try{
    const { items=[] } = req.body||{};
    if (!process.env.STRIPE_SECRET_KEY) return res.status(400).json({ error:'stripe_not_configured' });
    if (!Array.isArray(items) || !items.length) return res.status(400).json({ error:'no_items' });

    const line_items = items.map(it => ({
      price_data:{
        currency:'usd',
        unit_amount: Math.max(100, Math.round(Number(it.amountCents)||0)),
        product_data:{ name: String(it.label||'Service') }
      },
      quantity:1
    }));

    const session = await stripe.checkout.sessions.create({
      mode:'payment',
      payment_method_types:['card','link'],
      line_items,
      billing_address_collection:'required',
      success_url:`${APP_BASE_URL}/thank-you`,        // uses your pretty route
      cancel_url:`${APP_BASE_URL}/hire.html?canceled=1`,
      metadata:{ source:'hire_shop' }
    });
    res.json({ url: session.url });
  }catch(e){
    res.status(500).json({ error:'stripe_error' });
  }
});
/* ────────────────────────────────────────────────────────────────────── */


app.post('/api/lead', async (req, res) => {
  try {
    // You can persist to DB/Sheets/etc. For now just acknowledge.
    // console.log('lead:', req.body);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: 'lead_store_failed' });
  }
});

app.get('/api/activity-stream', (req, res) => {
  res.set({
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
  });
  if (typeof res.flushHeaders === 'function') {
    res.flushHeaders();
  }

  let closed = false;
  const keepAlive = setInterval(() => {
    if (closed) return;
    try {
      res.write(': keep-alive\n\n');
    } catch (error) {
      console.warn('[Admin] SSE keep-alive failed', error?.message || error);
    }
  }, 15000);

  const sendEvent = (payload) => {
    if (closed) return;
    try {
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    } catch (error) {
      console.warn('[Admin] SSE write failed', error?.message || error);
    }
  };

  const handleAutomationEvent = (payload) => sendEvent(payload);
  automationEmitter.on('automation', handleAutomationEvent);

  sendEvent({ type: 'stream:connected' });

  const close = () => {
    if (closed) return;
    closed = true;
    clearInterval(keepAlive);
    automationEmitter.off('automation', handleAutomationEvent);
    try {
      res.end();
    } catch (error) {
      // ignore
    }
  };

  req.on('close', close);
  req.on('error', close);
  res.on('close', close);
});

app.get('/api/csv-files', async (req, res) => {
  try {
    const items = await scanCsvDirectory(CSV_ROOT_DIR);
    res.json({ root: 'csvFIles', items });
  } catch (error) {
    console.error('[Admin] CSV scan failed', { message: error?.message || error });
    res.status(500).json({ error: 'csv_scan_failed' });
  }
});

app.post('/api/csv-files/preview', async (req, res) => {
  try {
    const relativePath = req.body?.path;
    if (!relativePath) {
      return res.status(400).json({ error: 'missing_path' });
    }

    const dataset = await loadCsvAutomationData(relativePath, {
      maxRows: CSV_PREVIEW_ROW_LIMIT + 1,
    });

    const header = dataset.header.map((value) => tidyCell(value));
    const previewLimit = Math.max(1, CSV_PREVIEW_ROW_LIMIT);
    const previewRows = dataset.dataRows.slice(0, previewLimit);
    const rows = previewRows.map((row) => header.map((_, index) => tidyCell(row[index])));
    const skippedByReason = dataset.skipped.reduce((acc, item) => {
      const reason = item.reason || 'other';
      acc[reason] = (acc[reason] || 0) + 1;
      return acc;
    }, {});

    const leadPreview = dataset.leads.slice(0, 5).map((lead) => ({
      name: lead.name,
      phone: maskPhoneNumberForLog(lead.phone),
      context: lead.context,
    }));

    res.json({
      file: dataset.file,
      header,
      rows,
      totalRows: dataset.dataRows.length,
      validLeads: dataset.leads.length,
      skippedRows: dataset.skipped.length,
      skippedByReason,
      truncated: dataset.dataRows.length > previewLimit,
      columns: dataset.columns.map((col) => ({
        index: col.index,
        label: col.label,
        type: col.type,
        samples: col.samples,
      })),
      leadsPreview: leadPreview,
      pitchAvailable: Boolean(process.env.OPENAI_API_KEY),
      automationLimit: CSV_AUTOMATION_MAX_LEADS,
    });
  } catch (error) {
    const status = error?.code === 'invalid_csv_path' ? 400 : 500;
    console.error('[Admin] CSV preview failed', { message: error?.message || error });
    res.status(status).json({ error: 'csv_preview_failed', code: error?.code || null });
  }
});

app.post('/api/csv-files/pitch', async (req, res) => {
  if (!process.env.OPENAI_API_KEY) {
    return res.status(503).json({ error: 'openai_unavailable' });
  }

  try {
    const relativePath = req.body?.path;
    if (!relativePath) {
      return res.status(400).json({ error: 'missing_path' });
    }

    const dataset = await loadCsvAutomationData(relativePath, {
      maxRows: Math.min(CSV_AUTOMATION_PARSE_LIMIT, 80),
    });

    const sampleRows = dataset.dataRows.slice(0, 10).map((row) => {
      const record = {};
      dataset.header.forEach((col, index) => {
        const key = tidyCell(col) || `Column ${index + 1}`;
        record[key] = tidyCell(row[index]);
      });
      return record;
    });

    const contextSnippets = dataset.leads
      .map((lead) => lead.context)
      .filter(Boolean)
      .slice(0, 8);

    const systemPrompt = 'You are the DelcoTech revenue strategist. Given a lead list description, craft a concise outreach pitch. Respond in JSON with keys headline, valueProps (array of 3), callOpener, callBeats (array of 3), callCloser, smsIntro, smsValue, smsCloser.';
    const userPrompt = [
      `CSV File: ${dataset.file.name}`,
      `Column labels: ${dataset.columns.map((col) => col.label).join(', ') || 'n/a'}`,
      sampleRows.length ? `Sample leads: ${JSON.stringify(sampleRows, null, 2)}` : 'Sample leads: none',
      contextSnippets.length ? `Context clues: ${contextSnippets.join(' | ')}` : 'Context clues: none',
      'Task: return JSON only. Build a headline for this campaign, three sharp value props, a call opener and three beats, and SMS copy that references the list insights. Keep lines under 160 characters and use energetic but professional tone. Do not mention placeholders.'
    ].join('\n');

    const completion = await openai.chat.completions.create({
      model: process.env.OPENAI_MODEL || 'gpt-4o-mini',
      temperature: 0.6,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ],
    });

    const rawContent = completion.choices?.[0]?.message?.content || '';
    const jsonText = rawContent.replace(/```json|```/g, '').trim();
    let parsed;
    try {
      parsed = JSON.parse(jsonText);
    } catch (parseError) {
      console.warn('[Admin] CSV pitch JSON parse failed', { message: parseError?.message || parseError, raw: rawContent });
      parsed = {};
    }

    const pitch = {
      headline: sanitizeLine(parsed?.headline) || `High-intent outreach for ${dataset.file.name.replace(/\.csv$/i, '')}`,
      valueProps: Array.isArray(parsed?.valueProps)
        ? parsed.valueProps.map((item) => sanitizeLine(item)).filter(Boolean).slice(0, 4)
        : [],
      callOpener: sanitizeLine(parsed?.callOpener) || sanitizeLine(parsed?.callScript?.opener) || '',
      callBeats: Array.isArray(parsed?.callBeats)
        ? parsed.callBeats.map((item) => sanitizeLine(item)).filter(Boolean).slice(0, 5)
        : [],
      callCloser: sanitizeLine(parsed?.callCloser) || '',
      smsIntro: sanitizeLine(parsed?.smsIntro) || '',
      smsValue: sanitizeLine(parsed?.smsValue) || '',
      smsCloser: sanitizeLine(parsed?.smsCloser) || '',
      raw: rawContent,
    };

    res.json({ pitch, file: dataset.file });
  } catch (error) {
    const status = error?.code === 'invalid_csv_path' ? 400 : 500;
    console.error('[Admin] CSV pitch failed', { message: error?.message || error });
    res.status(status).json({ error: 'csv_pitch_failed', code: error?.code || null });
  }
});

app.post('/api/csv-files/launch-call', async (req, res) => {
  const body = req.body || {};
  const runId = typeof body.runId === 'string' ? body.runId.slice(0, 80) : null;

  try {
    const { path: relativePath, limit, callConfig = {}, pitch = {} } = body;
    if (!relativePath) {
      return res.status(400).json({ error: 'missing_path' });
    }

    const dataset = await loadCsvAutomationData(relativePath, { maxRows: CSV_AUTOMATION_PARSE_LIMIT });
    if (!dataset.leads.length) {
      return res.status(400).json({ error: 'no_leads' });
    }

    const appBaseUrl = resolveAppBaseUrl(req);
    const datasetName = dataset.file.name.replace(/\.csv$/i, '');
    const desiredLimit = Number(limit) || CSV_AUTOMATION_MAX_LEADS;
    const leadLimit = Math.max(1, Math.min(CSV_AUTOMATION_MAX_LEADS, desiredLimit));
    const selectedLeads = dataset.leads.slice(0, leadLimit);
    if (runId) {
      publishAutomationEvent({
        type: 'automation:start',
        mode: 'call',
        runId,
        dataset: dataset.file.name,
        total: selectedLeads.length,
      });
    }
    const baseScriptLines = String(callConfig?.script || '')
      .split(/\r?\n/)
      .map(sanitizeLine)
      .filter(Boolean);
    const pitchBeats = Array.isArray(pitch?.callBeats)
      ? pitch.callBeats.map(sanitizeLine).filter(Boolean)
      : [];

    const results = [];
    let queued = 0;

    for (const lead of selectedLeads) {
      const personalizedLines = [];
      if (lead.context) {
        personalizedLines.push(`Lead context: ${lead.context}`);
      }
      const scriptLines = [...baseScriptLines];
      pitchBeats.forEach((line) => {
        if (line && !scriptLines.includes(line)) {
          scriptLines.push(line);
        }
      });
      personalizedLines.forEach((line) => {
        if (line && !scriptLines.includes(line)) {
          scriptLines.push(line);
        }
      });

      const introCandidates = [
        sanitizeLine(callConfig?.intro),
        sanitizeLine(pitch?.callOpener),
        lead.context ? `We spotted ${lead.context}.` : '',
      ].filter(Boolean);
      const goalCandidates = [
        sanitizeLine(callConfig?.goal),
        sanitizeLine(pitch?.headline),
        datasetName,
      ].filter(Boolean);

      const eventLead = {
        name: lead.name,
        phone: maskPhoneNumberForLog(lead.phone),
        context: lead.context,
      };
      const attemptIndex = results.length + 1;
      if (runId) {
        publishAutomationEvent({
          type: 'call:attempt',
          mode: 'call',
          runId,
          lead: eventLead,
          index: attemptIndex,
          total: selectedLeads.length,
        });
      }

      try {
        const response = await queueColdCallerCall({
          to: lead.phone,
          leadName: lead.name,
          goal: goalCandidates[0] || datasetName,
          intro: introCandidates[0] || '',
          script: scriptLines.join('\n'),
          voice: callConfig?.voice || 'warm',
          handoffNumber: callConfig?.handoffNumber || '',
          appBaseUrl,
        });
        queued += 1;
        const result = {
          ok: true,
          sid: response.sid,
          lead: eventLead,
        };
        results.push(result);
        if (runId) {
          publishAutomationEvent({
            type: 'call:queued',
            mode: 'call',
            runId,
            lead: eventLead,
            sid: response.sid,
            index: attemptIndex,
            total: selectedLeads.length,
          });
        }
      } catch (error) {
        const failure = {
          ok: false,
          error: {
            message: error?.message || 'Call failed',
            code: error?.code || null,
            status: error?.status || null,
          },
          lead: eventLead,
        };
        results.push(failure);
        if (runId) {
          publishAutomationEvent({
            type: 'call:error',
            mode: 'call',
            runId,
            lead: eventLead,
            error: {
              message: failure.error.message,
              code: failure.error.code,
              status: failure.error.status,
            },
            index: attemptIndex,
            total: selectedLeads.length,
          });
        }
      }
    }

    const attempted = selectedLeads.length;
    const failedCount = attempted - queued;
    const summary = attempted
      ? `Calls queued for ${queued}/${attempted} leads${failedCount ? ` — ${failedCount} failed.` : '.'}`
      : 'No leads were queued.';
    if (runId) {
      publishAutomationEvent({
        type: 'automation:complete',
        mode: 'call',
        runId,
        queued,
        attempted,
        failed: failedCount,
        message: summary,
      });
    }

    res.json({
      ok: true,
      file: dataset.file,
      attempted,
      queued,
      failed: failedCount,
      totalLeads: dataset.leads.length,
      skippedRows: dataset.skipped.length + Math.max(dataset.leads.length - selectedLeads.length, 0),
      limit: CSV_AUTOMATION_MAX_LEADS,
      results,
    });
  } catch (error) {
    const status = error?.code === 'invalid_csv_path' ? 400 : 500;
    console.error('[Admin] CSV bulk call failed', { message: error?.message || error });
    if (runId) {
      publishAutomationEvent({
        type: 'automation:error',
        mode: 'call',
        runId,
        message: error?.message || 'Call automation failed',
        error: {
          code: error?.code || null,
          status,
        },
      });
    }
    res.status(status).json({ error: 'csv_call_failed', code: error?.code || null });
  }
});

app.post('/api/csv-files/send-sms', async (req, res) => {
  const body = req.body || {};
  const runId = typeof body.runId === 'string' ? body.runId.slice(0, 80) : null;

  try {
    const { path: relativePath, limit, message = '', pitch = {} } = body;
    if (!relativePath) {
      return res.status(400).json({ error: 'missing_path' });
    }

    const dataset = await loadCsvAutomationData(relativePath, { maxRows: CSV_AUTOMATION_PARSE_LIMIT });
    if (!dataset.leads.length) {
      return res.status(400).json({ error: 'no_leads' });
    }

    const desiredLimit = Number(limit) || CSV_AUTOMATION_MAX_LEADS;
    const leadLimit = Math.max(1, Math.min(CSV_AUTOMATION_MAX_LEADS, desiredLimit));
    const selectedLeads = dataset.leads.slice(0, leadLimit);
    if (runId) {
      publishAutomationEvent({
        type: 'automation:start',
        mode: 'sms',
        runId,
        dataset: dataset.file.name,
        total: selectedLeads.length,
      });
    }
    const datasetName = dataset.file.name.replace(/\.csv$/i, '');

    const baseMessage = sanitizeLine(message)
      || sanitizeLine(pitch?.smsIntro)
      || `Quick insight for ${datasetName} — let's automate the outreach.`;
    const valueLine = sanitizeLine(pitch?.smsValue) || '';
    let closerLine = sanitizeLine(pitch?.smsCloser) || `Lock a slot: ${COLD_CALLER_SMS_LINK}`;
    if (!closerLine.includes(COLD_CALLER_SMS_LINK)) {
      closerLine = `${closerLine} ${COLD_CALLER_SMS_LINK}`.trim();
    }

    const results = [];
    let sent = 0;

    for (const lead of selectedLeads) {
      const greeting = lead.firstName
        ? `Hi ${lead.firstName}, it's ${BUSINESS} from DelcoTech.`
        : `Hi there, it's ${BUSINESS} from DelcoTech.`;
      const contextLine = lead.context ? `Saw ${lead.context}.` : '';
      const messageParts = [greeting, baseMessage, valueLine, contextLine, closerLine]
        .map((line) => sanitizeLine(line))
        .filter(Boolean);
      const finalMessage = messageParts.join('\n');

      const eventLead = {
        name: lead.name,
        phone: maskPhoneNumberForLog(lead.phone),
        context: lead.context,
      };
      const attemptIndex = results.length + 1;
      if (runId) {
        publishAutomationEvent({
          type: 'sms:attempt',
          mode: 'sms',
          runId,
          lead: eventLead,
          index: attemptIndex,
          total: selectedLeads.length,
        });
      }

      try {
        await sendSMS(lead.phone, finalMessage, {
          source: 'admin_csv_sms',
          dataset: datasetName,
        });
        sent += 1;
        const result = {
          ok: true,
          lead: eventLead,
        };
        results.push(result);
        if (runId) {
          publishAutomationEvent({
            type: 'sms:sent',
            mode: 'sms',
            runId,
            lead: eventLead,
            index: attemptIndex,
            total: selectedLeads.length,
          });
        }
      } catch (error) {
        const failure = {
          ok: false,
          error: {
            message: error?.message || 'SMS failed',
            code: error?.code || null,
            status: error?.status || null,
          },
          lead: eventLead,
        };
        results.push(failure);
        if (runId) {
          publishAutomationEvent({
            type: 'sms:error',
            mode: 'sms',
            runId,
            lead: eventLead,
            error: {
              message: failure.error.message,
              code: failure.error.code,
              status: failure.error.status,
            },
            index: attemptIndex,
            total: selectedLeads.length,
          });
        }
      }
    }

    const attempted = selectedLeads.length;
    const failedCount = attempted - sent;
    const summary = attempted
      ? `SMS sent to ${sent}/${attempted} leads${failedCount ? ` — ${failedCount} failed.` : '.'}`
      : 'No messages were sent.';
    if (runId) {
      publishAutomationEvent({
        type: 'automation:complete',
        mode: 'sms',
        runId,
        sent,
        attempted,
        failed: failedCount,
        message: summary,
      });
    }

    res.json({
      ok: true,
      file: dataset.file,
      attempted,
      sent,
      failed: failedCount,
      totalLeads: dataset.leads.length,
      skippedRows: dataset.skipped.length + Math.max(dataset.leads.length - selectedLeads.length, 0),
      limit: CSV_AUTOMATION_MAX_LEADS,
      results,
    });
  } catch (error) {
    const status = error?.code === 'invalid_csv_path' ? 400 : 500;
    console.error('[Admin] CSV bulk SMS failed', { message: error?.message || error });
    if (runId) {
      publishAutomationEvent({
        type: 'automation:error',
        mode: 'sms',
        runId,
        message: error?.message || 'SMS automation failed',
        error: {
          code: error?.code || null,
          status,
        },
      });
    }
    res.status(status).json({ error: 'csv_sms_failed', code: error?.code || null });
  }
});

/**
 * Dynamic one-time checkout
 * Expects: { amountCents, summary, partnerSlug?, rep?, dealId? }
 * Returns: { url }
 */
app.post('/api/deal/checkout/stripe', async (req, res) => {
  try {
    const { amountCents, summary = '', partnerSlug = '', rep = '', dealId = '' } = req.body || {};

    const amt = Number(amountCents) | 0;
    if (!amt || amt < 100) return res.status(400).json({ error: 'invalid_amount' });

    const name = String(summary).slice(0, 120) || 'Delco Tech — Custom Package';

    const session = await stripe.checkout.sessions.create({
      mode: 'payment',
      payment_method_types: ['card', 'link'],
      allow_promotion_codes: true,
      line_items: [{
        price_data: {
          currency: 'usd',
          product_data: { name },
          unit_amount: amt
        },
        quantity: 1
      }],
      success_url: `${APP_BASE_URL}/deal-thank-you.html?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${APP_BASE_URL}/legal.html#packages`,
      metadata: { partnerSlug, rep, dealId }
    });

    return res.json({ url: session.url });
  } catch (e) {
    // console.error('checkout error', e);
    res.status(500).json({ error: 'checkout_failed' });
  }
});


// Dev: simulate a missed call
app.get('/simulate/missed-call', async (req, res) => {
  const from = req.query.from;
  if (!from) return res.status(400).json({ ok: false, error: 'from required' });

  setStep(from, 'ask_name');
  await upsertByPhone(from, { status: 'opened' });
  await sendSMS(
    from,
    `Hey, it's ${BUSINESS}. Sorry we missed your call. What's your name? ` +
    `Book anytime: ${CAL_LINK} — Reply STOP to stop, HELP for help.`,
    {
      source: 'simulate_missed_call',
    }
  );

  res.json({ ok: true });
});

// Boot
const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  console.log(`Missed-Call Money Saver running on :${PORT}`);

  if (process.env.CALENDLY_TOKEN && process.env.APP_BASE_URL) {
    const cb = `${process.env.APP_BASE_URL}/calendly/webhook`;
    const r = await subscribeCalendlyWebhook(cb).catch(() => null);
    console.log(r?.ok ? 'Calendly webhook subscribed.' : 'Calendly webhook not subscribed (optional).');
  }
});

import 'dotenv/config';
import { GoogleGenerativeAI } from '@google/generative-ai';
import fs from 'fs-extra';
import path from 'path';
import { glob } from 'glob';
const modelName = 'gemini-3-pro-preview';

const COLOR_ENABLED =
  process.stdout.isTTY &&
  !process.env.NO_COLOR &&
  process.env.FORCE_COLOR !== '0';

const ANSI = {
  reset: '\u001b[0m',
  dim: '\u001b[2m',
  bold: '\u001b[1m',
  red: '\u001b[31m',
  green: '\u001b[32m',
  yellow: '\u001b[33m',
  blue: '\u001b[34m',
  magenta: '\u001b[35m',
  cyan: '\u001b[36m',
  gray: '\u001b[90m'
};

function c(text, ...styles) {
  if (!COLOR_ENABLED) return String(text);
  const open = styles.map(s => ANSI[s]).join('');
  return `${open}${text}${ANSI.reset}`;
}

function ts() {
  return new Date().toISOString();
}

function log(...args) {
  console.log(c(`[${ts()}]`, 'dim'), c('INFO', 'cyan'), ...args);
}

function warn(...args) {
  console.warn(c(`[${ts()}]`, 'dim'), c('WARN', 'yellow', 'bold'), ...args);
}

function errorLog(...args) {
  console.error(c(`[${ts()}]`, 'dim'), c('ERR ', 'red', 'bold'), ...args);
}

function approxChars(parts) {
  if (!Array.isArray(parts)) return 0;
  let total = 0;
  for (const p of parts) total += String(p ?? '').length;
  return total;
}

function stripJsonFences(text) {
  const t = String(text ?? '').trim();
  if (t.startsWith('```')) {
    return t.replace(/^```[a-zA-Z]*\s*/m, '').replace(/```$/m, '').trim();
  }
  return t;
}

function tryParseJsonObject(text) {
  const cleaned = stripJsonFences(text);
  try {
    return JSON.parse(cleaned);
  } catch {
    const start = cleaned.indexOf('{');
    const end = cleaned.lastIndexOf('}');
    if (start >= 0 && end > start) {
      const slice = cleaned.slice(start, end + 1);
      return JSON.parse(slice);
    }
    throw new Error('Could not parse JSON');
  }
}

function getErrorCode(err) {
  const code =
    err?.code ??
    err?.cause?.code ??
    err?.cause?.errno ??
    err?.errno ??
    err?.cause?.cause?.code;
  return typeof code === 'string' ? code : undefined;
}

function isRetryableError(err) {
  const status = err?.status ?? err?.cause?.status;
  if (typeof status === 'number') {
    if (status === 408) return true;
    if (status === 429) return true;
    if (status === 503) return true;
    if (status >= 500 && status <= 599) return true;
  }

  const msg = String(err?.message ?? '');
  if (/fetch failed/i.test(msg)) return true;
  if (/socket hang up/i.test(msg)) return true;

  const code = getErrorCode(err);
  if (!code) return false;
  return [
    'ECONNRESET',
    'ECONNREFUSED',
    'EAI_AGAIN',
    'ENOTFOUND',
    'ETIMEDOUT',
    'UND_ERR_CONNECT_TIMEOUT',
    'UND_ERR_HEADERS_TIMEOUT',
    'UND_ERR_BODY_TIMEOUT',
    'UND_ERR_SOCKET'
  ].includes(code);
}

async function generateTextWithRetry(modelToUse, promptParts, attempt = 1) {
  const maxAttempts = Number.parseInt(process.env.RETRY_MAX_ATTEMPTS ?? '', 10) || 30;
  const baseDelay = Number.parseInt(process.env.RETRY_BASE_DELAY_MS ?? '', 10) || 1500;
  const maxDelay = Number.parseInt(process.env.RETRY_MAX_DELAY_MS ?? '', 10) || 120000;

  try {
    const sendSize = approxChars(promptParts);
    const t0 = Date.now();
    log(c(`model:${modelName} send`, 'magenta'), `attempt=${attempt}/${maxAttempts}`, `chars≈${sendSize.toLocaleString()}`);
    const result = await modelToUse.generateContent(promptParts);
    const text = result.response.text();
    const dt = Date.now() - t0;
    log(c(`model:${modelName} recv`, 'green'), `ms=${dt}`, `chars=${String(text ?? '').length.toLocaleString()}`);
    return text;
  } catch (error) {
    if (attempt > maxAttempts) throw error;
    if (!isRetryableError(error)) throw error;

    const exp = baseDelay * Math.pow(2, attempt - 1);
    const delay = Math.min(maxDelay, exp) + Math.floor(Math.random() * 750);
    const status = error?.status ?? error?.cause?.status;
    const code = getErrorCode(error);
    const msg = String(error?.message ?? '').slice(0, 220).replace(/\s+/g, ' ').trim();
    const bits = [`retry`, `attempt=${attempt}/${maxAttempts}`];
    if (status) bits.push(`status=${status}`);
    if (code) bits.push(`code=${code}`);
    if (msg) bits.push(`msg="${msg}"`);
    bits.push(`sleep=${(delay / 1000).toFixed(1)}s`);
    warn(c(bits.join(' | '), 'yellow'));
    await new Promise(resolve => setTimeout(resolve, delay));
    return generateTextWithRetry(modelToUse, promptParts, attempt + 1);
  }
}

function parseArgs(argv) {
  const args = {
    compendiumPath: null,
    compendiumDir: './compendium_versions',
    topicsDir: './structured_topics',
    snapshotsDir: './structured_topics_versions',
    stateFile: null,
    batchSize: 50,
    maxBatches: 1,
    topic: null, // slug or header substring
    startIndex: null, // start message index within a chosen topic
    logModelOutput: false,
    dryRun: false
  };

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const v = argv[i + 1];

    if (a === '--compendium' && v) {
      args.compendiumPath = v;
      i++;
    } else if (a === '--compendium-dir' && v) {
      args.compendiumDir = v;
      i++;
    } else if (a === '--structured-dir' && v) {
      // compat alias
      args.topicsDir = v;
      i++;
    } else if (a === '--topics-dir' && v) {
      args.topicsDir = v;
      i++;
    } else if (a === '--snapshots-dir' && v) {
      args.snapshotsDir = v;
      i++;
    } else if (a === '--state' && v) {
      args.stateFile = v;
      i++;
    } else if (a === '--batch-size' && v) {
      args.batchSize = Math.max(1, Number.parseInt(v, 10) || 50);
      i++;
    } else if (a === '--max-batches' && v) {
      args.maxBatches = Math.max(1, Number.parseInt(v, 10) || 1);
      i++;
    } else if (a === '--topic' && v) {
      args.topic = String(v);
      i++;
    } else if (a === '--start-index' && v) {
      args.startIndex = Math.max(0, Number.parseInt(v, 10) || 0);
      i++;
    } else if (a === '--log-model-output') {
      args.logModelOutput = true;
    } else if (a === '--dry-run') {
      args.dryRun = true;
    }
  }

  return args;
}

async function getLatestCompendiumFile(compendiumDir) {
  const files = await glob(path.join(compendiumDir, 'compendium_*.md').replace(/\\/g, '/'));
  const parsed = files
    .map(p => {
      const base = path.basename(p);
      const m = base.match(/compendium_(\d+)\.md$/);
      return m ? { path: p, version: Number.parseInt(m[1], 10) } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.version - a.version);
  return parsed.length ? parsed[0] : null;
}

function parseEditionNumber(compendiumPath) {
  const base = path.basename(compendiumPath);
  const m = base.match(/compendium_(\d+)\.md$/);
  return m ? Number.parseInt(m[1], 10) : null;
}

function parseCompendiumMessages(markdown) {
  const lines = String(markdown ?? '').split(/\r?\n/);
  const messages = [];

  let currentSection = null;
  let buf = [];
  let bufSection = null;

  // Message blocks are terminated by a Discord-like message ID in brackets at end.
  const idAtLineEnd = /\[(\d{10,})\]\s*$/;

  function flushIfComplete(line) {
    const m = String(line ?? '').match(idAtLineEnd);
    if (!m) return null;

    const messageId = m[1];
    const cleanedLastLine = String(line).replace(idAtLineEnd, '');
    const allLines = buf.length ? [...buf.slice(0, -1), cleanedLastLine] : [cleanedLastLine];
    const raw = allLines.join('\n');

    let date = null;
    let author = null;
    const firstNonReply = allLines.find(l => !String(l).trimStart().startsWith('>')) ?? '';
    const headerMatch = String(firstNonReply).match(/^\[(\d{4}-\d{2}-\d{2})\]\s+\[([^\]]+)\]\s+(.*)$/);
    if (headerMatch) {
      date = headerMatch[1];
      author = headerMatch[2];
    }

    const msg = {
      index: messages.length,
      messageId,
      section: bufSection ?? currentSection,
      sectionHeader: (bufSection ?? currentSection) ? `## ${bufSection ?? currentSection}` : '## Uncategorized',
      date,
      author,
      raw
    };
    messages.push(msg);

    buf = [];
    bufSection = null;
    return msg;
  }

  for (const line of lines) {
    const rawLine = String(line ?? '');
    const trimmedEnd = rawLine.trimEnd();

    if (trimmedEnd.startsWith('## ')) {
      currentSection = trimmedEnd.slice(3).trim();
      continue;
    }

    if (buf.length === 0) bufSection = currentSection;
    buf.push(rawLine);
    flushIfComplete(rawLine);
  }

  return messages;
}

function slugifyTopic(topicHeader) {
  const t = String(topicHeader ?? '').trim();
  const norm = t.replace(/^##\s+/, '').trim().toLowerCase();
  const slug = norm
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
  return slug || 'unknown';
}

function groupMessagesByTopic(messages) {
  const map = new Map();
  const order = [];
  for (const m of messages) {
    const h = m.sectionHeader || '## Uncategorized';
    if (!map.has(h)) {
      map.set(h, []);
      order.push(h);
    }
    map.get(h).push(m);
  }
  return { map, order };
}

function buildStructurePrompt({ topicHeader, topicSlug, edition, batch }) {
  return [
    `# Rola`,
    `Jestes agentem ekstrakcji informacji i redaktorem kompendium.`,
    ``,
    `# Zadanie`,
    `Dostajesz paczke docelowych wiadomosci (TARGET) z tematu kompendium podrozy do Japonii.`,
    `Dodatkowo dostajesz kontekst: 50 poprzednich (CONTEXT_PREV) i 50 nastepnych (CONTEXT_NEXT) wiadomosci.`,
    `Twoim celem jest stworzenie czytelnych wpisow w stylu kompendium: pelne zdania po polsku,`,
    `bez lania wody, ale tez nie w stylu "surowe fakty".`,
    ``,
    `# Krytyczne zasady (NIE LAM)`,
    `- Zwracaj TYLKO jeden poprawny obiekt JSON (bez markdown, bez code fences, bez komentarzy).`,
    `- Niczego nie zmyslaj. Jesli czegos nie da sie podeprzec cytatem, nie dodawaj tego.`,
    `- Tworz wpisy TYLKO na podstawie TARGET (nie dopisuj nowych informacji tylko dlatego, ze sa w kontekscie).`,
    `- Zrodla moga pochodzic z TARGET albo z kontekstu (PREV/NEXT), ale wtedy nadal musza miec messageId.`,
    `- Kazde twierdzenie/porada/ostrzezenie w entry.text musi byc podparte cytatami z wiadomosci.`,
    `- Chcemy styl Wikipedii: przypisy w tekscie jako [1], [2], ...`,
    `- "citationInserts.atChar" to pozycja w entry.text (0-based), gdzie skrypt wstawi [n] (styl Wikipedii).`,
    `- ZAWSZE ustawiaj citationInserts.atChar na KONCU zdania (po kropce/!/?) tak, aby przypisy nie rozbijaly wyrazow.`,
    `- Jesli zdanie ma kilka twierdzen, mozesz wstawic wiele przypisow na koncu tego zdania (wiele sources).`,
    `- entry.text ma byc po polsku.`,
    ``,
    `# Wymog zachowania szczegolow (MINIMALIZUJ STRATY)`,
    `Zalezy nam na maksymalnej wiernosc informacyjnej. Nie streszczaj watkow do jednego zdania, jesli w wiadomosciach jest wiecej detali.`,
    `- Jesli w wiadomosciach sa liczby/kwoty/terminy/dlugosci pobytu/warunki (np. dochod, punkty, CoE, miesiace, lata) -> zachowaj je.`,
    `- Jesli sa porownania (np. jedna wiza vs druga, teoria vs praktyka) -> opisz roznice wprost, w osobnych zdaniach.`,
    `- Jesli sa zastrzezenia / "w teorii" / "w praktyce" / wyjatki -> dodaj je jako osobne zdania.`,
    `- Lepiej zrobic 2-5 zdan albo kilka entries niz zgubic informacje.`,
    `- Gdy watek jest gesty (duzo wiadomosci o jednym temacie), zrob osobny wpis typu "info" + osobny "warning" + osobny "rule" itd.`,
    ``,
    `# Styl (bardziej "Discord", mniej encyklopedia)`,
    `Zalezy nam, zeby to bylo latwe do czytania dla ludzi przyzwyczajonych do Discorda.`,
    `- Pisz po polsku, ale luzno: dopuszczalne sa wtracenia typu "protip", "w skrocie", "IMO", itp.`,
    `- Preferuj krotkie zdania + czasem newline zamiast jednego dlugiego, akademickiego akapitu.`,
    `- Mozesz uzywac nawiasow i potocznych sformulowan, byle nie zmieniac sensu.`,
    `- Jesli cos w zrodle brzmi jak zart/mem (np. Strong Zero) -> zaznacz to jako "raczej mem/nieoficjalne", ale nie moralizuj.`,
    `- Nie musisz "wybielac" calkowicie stylu, ale unikaj wulgaryzmow/slurow w tresci wpisu (sens zachowaj).`,
    ``,
    `# Kontekst`,
    `Temat: "${topicHeader}" (slug: "${topicSlug}")`,
    `Edycja kompendium: ${edition}`,
    ``,
    `# Format JSON (SCHEMAT - TRZYMAJ SIE)`,
    `{`,
    `  "entries": [`,
    `    {`,
    `      "entryId": "string (np. \\"e1\\")",`,
    `      "type": "one of: tip|warning|price|rule|route|recommendation|info|other",`,
    `      "text": "string (pelne zdania; styl kompendium; po polsku)",`,
    `      "citationInserts": [`,
    `        {`,
    `          "atChar": 0,`,
    `          "sources": [`,
    `            {`,
    `              "messageId": "string"`,
    `            }`,
    `          ]`,
    `        }`,
    `      ],`,
    `    }`,
    `  ],`,
    `}`,
    ``,
    `# WIADOMOSCI (WEJSCIE)`,
    `BEGIN_TARGET_MESSAGES_JSON`,
    JSON.stringify(
      batch.target.map(m => ({
        messageId: String(m.messageId),
        sectionHeader: m.sectionHeader,
        date: m.date ?? null,
        author: m.author ?? null,
        raw: m.raw
      })),
      null,
      2
    ),
    `END_TARGET_MESSAGES_JSON`,
    ``,
    `BEGIN_CONTEXT_PREV_MESSAGES_JSON`,
    JSON.stringify(
      batch.prev.map(m => ({
        messageId: String(m.messageId),
        sectionHeader: m.sectionHeader,
        date: m.date ?? null,
        author: m.author ?? null,
        raw: m.raw
      })),
      null,
      2
    ),
    `END_CONTEXT_PREV_MESSAGES_JSON`,
    ``,
    `BEGIN_CONTEXT_NEXT_MESSAGES_JSON`,
    JSON.stringify(
      batch.next.map(m => ({
        messageId: String(m.messageId),
        sectionHeader: m.sectionHeader,
        date: m.date ?? null,
        author: m.author ?? null,
        raw: m.raw
      })),
      null,
      2
    ),
    `END_CONTEXT_NEXT_MESSAGES_JSON`
  ].join('\n');
}

function safeJsonArray(val) {
  return Array.isArray(val) ? val : [];
}

async function readJsonIfExists(p) {
  if (!(await fs.pathExists(p))) return null;
  try {
    return await fs.readJson(p);
  } catch {
    return null;
  }
}

async function writeJson(p, obj) {
  await fs.ensureDir(path.dirname(p));
  await fs.writeFile(p, JSON.stringify(obj, null, 2), 'utf8');
}

async function getLatestSnapshotN(snapshotsDir, edition, topicSlug) {
  const dir = path.join(snapshotsDir, `compendium_${edition}`, topicSlug).replace(/\\/g, '/');
  const files = await glob(path.join(dir, '*.json').replace(/\\/g, '/'));
  const nums = files
    .map(f => path.basename(f).match(/^(\d+)\.json$/))
    .filter(Boolean)
    .map(m => Number.parseInt(m[1], 10))
    .filter(n => Number.isFinite(n));
  return nums.length ? Math.max(...nums) : 0;
}

function nextSnapshotPath(snapshotsDir, edition, topicSlug, currentN) {
  const nextN = (currentN || 0) + 1;
  const padded = String(nextN).padStart(4, '0');
  return path.join(snapshotsDir, `compendium_${edition}`, topicSlug, `${padded}.json`);
}

function validateBatchCitations({ entries, messageById }) {
  let bad = 0;
  for (const e of entries) {
    const text = String(e?.text ?? '');
    const inserts = safeJsonArray(e?.citationInserts);
    for (const ins of inserts) {
      const at = ins?.atChar;
      if (!Number.isInteger(at) || at < 0 || at > text.length) bad++;
      const sources = safeJsonArray(ins?.sources);
      for (const s of sources) {
        const mid = String(s?.messageId ?? '');
        const msg = messageById.get(mid);
        if (!msg) {
          bad++;
          continue;
        }
        void msg;
      }
    }
  }
  return bad;
}

function applyWikipediaFootnotes(entry) {
  const baseText = String(entry?.text ?? '');
  const inserts = safeJsonArray(entry?.citationInserts)
    .map(ins => ({
      atChar: ins?.atChar,
      messageIds: safeJsonArray(ins?.sources)
        .map(s => String(s?.messageId ?? '').trim())
        .filter(Boolean)
    }))
    .filter(ins => Number.isInteger(ins.atChar) && ins.atChar >= 0 && ins.atChar <= baseText.length);

  function moveToSentenceEnd(text, atChar) {
    // Move marker to end of the sentence to avoid splitting words.
    // Sentence ends: '.', '!', '?' (and allow closing quotes/brackets right after).
    const n = text.length;
    const start = Math.max(0, Math.min(n, atChar));

    // If already at end or next char is whitespace/newline and previous is punctuation, keep.
    if (start >= n) return n;

    const tail = text.slice(start);
    const m = tail.match(/[.!?]/);
    if (!m) {
      // Fallback: before newline or end.
      const nl = tail.indexOf('\n');
      return nl === -1 ? n : start + nl;
    }

    let idx = start + (m.index ?? 0) + 1; // insert after punctuation

    // Include immediate closing characters after punctuation.
    while (idx < n && /["')\]]/.test(text[idx])) idx++;
    return idx;
  }

  // Group inserts by atChar, keep stable order.
  const groups = new Map();
  for (const ins of inserts) {
    const key = moveToSentenceEnd(baseText, ins.atChar);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(ins);
  }
  const atChars = Array.from(groups.keys()).sort((a, b) => a - b);

  const footnotes = {};
  const insertMarkers = [];
  let refNumber = 1;

  for (const at of atChars) {
    const insList = groups.get(at) || [];
    // If model provided multiple inserts at same position, they become [n][n+1]...
    const nums = [];
    for (const ins of insList) {
      const n = refNumber++;
      nums.push(n);
      // Store unique messageIds per footnote number.
      const uniq = Array.from(new Set(safeJsonArray(ins.messageIds)));
      footnotes[String(n)] = uniq;
    }
    insertMarkers.push({ atChar: at, marker: nums.map(n => `[${n}]`).join('') });
  }

  // Insert from end to start so indices stay valid.
  let textWith = baseText;
  for (let i = insertMarkers.length - 1; i >= 0; i--) {
    const { atChar, marker } = insertMarkers[i];
    textWith = textWith.slice(0, atChar) + marker + textWith.slice(atChar);
  }

  return {
    entryId: String(entry?.entryId ?? ''),
    type: String(entry?.type ?? 'other'),
    text: textWith,
    footnotes
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (!process.env.GEMINI_API_KEY && !args.dryRun) {
    errorLog('GEMINI_API_KEY is not set (put it in .env).');
    process.exit(1);
  }

  const thinkingBudget =
    Number.parseInt(process.env.STRUCTURE_THINKING_BUDGET ?? '', 10) ||
    Number.parseInt(process.env.THINKING_BUDGET ?? '', 10) ||
    24576;

  const compendiumInfo = args.compendiumPath
    ? { path: args.compendiumPath, version: parseEditionNumber(args.compendiumPath) }
    : await getLatestCompendiumFile(args.compendiumDir);

  if (!compendiumInfo?.path) {
    errorLog(`No compendium found. Looked in ${args.compendiumDir}`);
    process.exit(1);
  }

  const compendiumPath = compendiumInfo.path;
  const edition = compendiumInfo.version ?? parseEditionNumber(compendiumPath) ?? 0;

  const topicsRoot = path.join(args.topicsDir, `compendium_${edition}`);
  const defaultStatePath = args.stateFile ?? path.join(args.topicsDir, `compendium_${edition}_state.json`);

  log(`Compendium: ${path.resolve(compendiumPath)}`);
  log(`Edition: ${edition}`);
  log(`Model: ${modelName}`);
  log(`Thinking budget: ${thinkingBudget} tokens`);
  log(`Topics root: ${path.resolve(topicsRoot)}`);
  log(`Snapshots dir: ${path.resolve(args.snapshotsDir)}`);
  log(`State: ${path.resolve(defaultStatePath)}`);
  log(`batchSize=${args.batchSize} maxBatches=${args.maxBatches} dryRun=${args.dryRun}`);
  if (args.topic) log(`topic filter: ${args.topic}`);

  const md = await fs.readFile(compendiumPath, 'utf8');
  const messages = parseCompendiumMessages(md);
  log(c('Parsed messages', 'green'), `count=${messages.length.toLocaleString()}`);

  const { map: topicMap, order: topicHeadersInOrder } = groupMessagesByTopic(messages);
  const topicOrder = topicHeadersInOrder.map(header => ({ header, slug: slugifyTopic(header) }));

  const loadedState = (await readJsonIfExists(defaultStatePath)) || null;
  const cursors = loadedState?.topicCursors && typeof loadedState.topicCursors === 'object'
    ? { ...loadedState.topicCursors }
    : Object.fromEntries(topicOrder.map(t => [t.slug, 0]));
  const batchCounters = loadedState?.topicBatchCounters && typeof loadedState.topicBatchCounters === 'object'
    ? { ...loadedState.topicBatchCounters }
    : Object.fromEntries(topicOrder.map(t => [t.slug, 0]));

  const genAI = args.dryRun ? null : new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
  const model = args.dryRun
    ? null
    : genAI.getGenerativeModel({
      model: modelName,
      generationConfig: { thinkingConfig: { thinkingBudget } }
    });

  let batchesDone = 0;
  while (batchesDone < args.maxBatches) {
    // Pick the next topic with remaining messages (respect --topic filter if provided).
    let chosen = null;
    for (const t of topicOrder) {
      if (args.topic) {
        const q = String(args.topic).toLowerCase();
        if (!(t.slug.toLowerCase() === q || t.header.toLowerCase().includes(q))) continue;
      }

      const topicMessages = topicMap.get(t.header) || [];
      const cur = Number.isInteger(cursors[t.slug]) ? cursors[t.slug] : 0;
      if (cur < topicMessages.length) {
        chosen = { ...t, topicMessages, cursor: cur };
        break;
      }
    }

    if (!chosen) {
      log(c('Nothing to do', 'yellow'), 'No topic has remaining messages (or topic filter matches none).');
      break;
    }

    const { header: topicHeader, slug: topicSlug, topicMessages } = chosen;
    let start = chosen.cursor;
    if (typeof args.startIndex === 'number') start = Math.min(topicMessages.length, args.startIndex);
    const contextSize = 50;
    const target = topicMessages.slice(start, start + args.batchSize);
    const prev = topicMessages.slice(Math.max(0, start - contextSize), start);
    const next = topicMessages.slice(start + target.length, start + target.length + contextSize);
    const batch = { target, prev, next };

    if (target.length === 0) {
      cursors[topicSlug] = topicMessages.length;
      continue;
    }

    const topicFilePath = path.join(topicsRoot, `${topicSlug}.json`);
    const topicExisting = await readJsonIfExists(topicFilePath);
    const topicJson = topicExisting && typeof topicExisting === 'object'
      ? topicExisting
      : { entries: [] };
    if (!Array.isArray(topicJson.entries)) topicJson.entries = [];

    const batchIndex = Number.isInteger(batchCounters[topicSlug]) ? batchCounters[topicSlug] : 0;
    const batchMessageIds = target.map(m => String(m.messageId));

    log(c('Topic', 'cyan', 'bold'), `${topicHeader} (${topicSlug})`);
    log(c(`Batch ${batchIndex}`, 'cyan', 'bold'), `start=${start} count=${target.length}/${topicMessages.length}`);
    log(c('IDs', 'dim'), `${batchMessageIds[0]} ... ${batchMessageIds[batchMessageIds.length - 1]}`);

    if (args.dryRun) {
      log(c('Dry run', 'yellow'), 'Skipping model call.');
      break;
    }

    let parsed = null;
    let rawModelText = null;
    try {
      const prompt = buildStructurePrompt({ topicHeader, topicSlug, edition, batch });
      rawModelText = await generateTextWithRetry(model, [prompt]);
      parsed = tryParseJsonObject(rawModelText);
    } catch (e) {
      errorLog('Model call / JSON parse failed:', e);
      break;
    }

    const entries = safeJsonArray(parsed?.entries);

    const normalizedEntries = entries.map((e, i) => {
      const localId = typeof e?.entryId === 'string' && e.entryId.trim() ? e.entryId.trim() : `e${i + 1}`;
      return { ...e, entryId: `${batchIndex}:${localId}` };
    });

    // Validate quotes against the exact message.raw content that was provided to the model.
    const allProvided = target.concat(prev, next);
    const messageById = new Map(allProvided.map(m => [String(m.messageId), m]));
    const badCitations = validateBatchCitations({ entries: normalizedEntries, messageById });
    if (badCitations > 0) warn(`Citation validation issues: ${badCitations} (quotes/offsets may be wrong)`);

    // Convert citationInserts -> Wikipedia style [1][2] markers + an embedded "Przypisy:" block.
    const wikipediaEntries = normalizedEntries.map(applyWikipediaFootnotes);

    // Append entries. Topic JSON stays minimal.
    topicJson.entries = topicJson.entries.concat(wikipediaEntries);

    await writeJson(topicFilePath, topicJson);

    const lastN = await getLatestSnapshotN(args.snapshotsDir, edition, topicSlug);
    const snapPath = nextSnapshotPath(args.snapshotsDir, edition, topicSlug, lastN);
    await writeJson(snapPath, topicJson);

    if (args.logModelOutput && rawModelText) {
      const logsDir = path.join(args.topicsDir, '_model_logs', `compendium_${edition}`, topicSlug);
      const logPath = path.join(logsDir, `batch_${String(batchIndex).padStart(4, '0')}.json`);
      await fs.ensureDir(logsDir);
      await fs.writeFile(logPath, rawModelText, 'utf8');
      log(c('Model output log', 'gray'), path.resolve(logPath));
    }

    cursors[topicSlug] = start + target.length;
    batchCounters[topicSlug] = batchIndex + 1;

    await writeJson(defaultStatePath, {
      schemaVersion: 2,
      source: { compendiumPath: path.resolve(compendiumPath), edition },
      topicOrder,
      topicCursors: cursors,
      topicBatchCounters: batchCounters,
      lastRun: {
        topicHeader,
        topicSlug,
        batchIndex,
        messageStartIndex: start,
        messageCount: target.length,
        entries: wikipediaEntries.length,
        badCitations
      },
      updatedAt: ts()
    });

    log(c('Saved topic', 'green', 'bold'), path.resolve(topicFilePath));
    log(c('Snapshot', 'green'), path.resolve(snapPath));

    batchesDone++;

    // If --start-index was used as a one-off override, clear it for subsequent batches.
    args.startIndex = null;
  }

  log('Done');
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});


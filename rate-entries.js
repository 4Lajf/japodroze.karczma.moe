#!/usr/bin/env node
/**
 * Rates all entries in structured_topics/compendium_931 JSON files
 * based on interestingness, uniqueness, and non-obviousness (0-1000).
 *
 * Usage: node rate-entries.js [--resume] [--batch-size 100] [--dry-run]
 *
 * Saves progress to structured_topics/compendium_931_ratings_state.json
 * so interrupted runs can be resumed with --resume.
 */

import 'dotenv/config';
import { GoogleGenerativeAI } from '@google/generative-ai';
import fs from 'fs-extra';
import path from 'path';
import { fileURLToPath } from 'url';
import { glob } from 'glob';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const BASE = path.join(__dirname, 'structured_topics', 'compendium_931');
const STATE_FILE = path.join(__dirname, 'structured_topics', 'compendium_931_ratings_state.json');
const MODEL_NAME = 'gemini-3-flash-preview';

// --- Color helpers ---
const COLOR_ENABLED =
  process.stdout.isTTY && !process.env.NO_COLOR && process.env.FORCE_COLOR !== '0';
const ANSI = {
  reset: '\u001b[0m', dim: '\u001b[2m', bold: '\u001b[1m',
  red: '\u001b[31m', green: '\u001b[32m', yellow: '\u001b[33m',
  blue: '\u001b[34m', magenta: '\u001b[35m', cyan: '\u001b[36m', gray: '\u001b[90m'
};
function c(text, ...styles) {
  if (!COLOR_ENABLED) return String(text);
  return `${styles.map(s => ANSI[s] ?? '').join('')}${text}${ANSI.reset}`;
}
function ts() { return new Date().toISOString(); }
function log(...args) { console.log(c(`[${ts()}]`, 'dim'), c('INFO', 'cyan'), ...args); }
function warn(...args) { console.warn(c(`[${ts()}]`, 'dim'), c('WARN', 'yellow', 'bold'), ...args); }
function errorLog(...args) { console.error(c(`[${ts()}]`, 'dim'), c('ERR ', 'red', 'bold'), ...args); }

// --- JSON helpers ---
function stripJsonFences(text) {
  const t = String(text ?? '').trim();
  if (t.startsWith('```')) {
    return t.replace(/^```[a-zA-Z]*\s*/m, '').replace(/```\s*$/m, '').trim();
  }
  return t;
}

function tryParseJsonArray(text) {
  const cleaned = stripJsonFences(text);
  try {
    const val = JSON.parse(cleaned);
    if (Array.isArray(val)) return val;
    throw new Error('not an array');
  } catch {
    const start = cleaned.indexOf('[');
    const end = cleaned.lastIndexOf(']');
    if (start >= 0 && end > start) {
      return JSON.parse(cleaned.slice(start, end + 1));
    }
    throw new Error('Could not parse JSON array from model response');
  }
}

// --- Retry logic ---
function getErrorCode(err) {
  const code = err?.code ?? err?.cause?.code ?? err?.cause?.errno ?? err?.errno;
  return typeof code === 'string' ? code : undefined;
}

function isRetryableError(err) {
  const status = err?.status ?? err?.cause?.status;
  if (typeof status === 'number') {
    if (status === 408 || status === 429 || status === 503) return true;
    if (status >= 500 && status <= 599) return true;
  }
  const msg = String(err?.message ?? '');
  if (/fetch failed/i.test(msg) || /socket hang up/i.test(msg)) return true;
  const code = getErrorCode(err);
  if (!code) return false;
  return [
    'ECONNRESET', 'ECONNREFUSED', 'EAI_AGAIN', 'ENOTFOUND', 'ETIMEDOUT',
    'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT',
    'UND_ERR_BODY_TIMEOUT', 'UND_ERR_SOCKET'
  ].includes(code);
}

async function generateWithRetry(model, prompt, attempt = 1) {
  const maxAttempts = 30;
  const baseDelay = 1500;
  const maxDelay = 120000;
  try {
    const t0 = Date.now();
    log(c(`model:${MODEL_NAME} send`, 'magenta'), `attempt=${attempt}/${maxAttempts}`, `chars≈${prompt.length.toLocaleString()}`);
    const result = await model.generateContent(prompt);
    const text = result.response.text();
    log(c(`model:${MODEL_NAME} recv`, 'green'), `ms=${Date.now() - t0}`, `chars=${text.length.toLocaleString()}`);
    return text;
  } catch (error) {
    if (attempt > maxAttempts || !isRetryableError(error)) throw error;
    const delay = Math.min(maxDelay, baseDelay * Math.pow(2, attempt - 1)) + Math.floor(Math.random() * 750);
    const status = error?.status ?? error?.cause?.status;
    const bits = [`retry`, `attempt=${attempt}/${maxAttempts}`];
    if (status) bits.push(`status=${status}`);
    bits.push(`sleep=${(delay / 1000).toFixed(1)}s`);
    warn(c(bits.join(' | '), 'yellow'));
    await new Promise(r => setTimeout(r, delay));
    return generateWithRetry(model, prompt, attempt + 1);
  }
}

// --- Prompt builder ---
function buildRatingPrompt(items) {
  return [
    `# Rola`,
    `Jesteś ekspertem oceniającym wartość informacyjną wpisów z kompendium podróży do Japonii.`,
    ``,
    `# Zadanie`,
    `Oceń każdy wpis na podstawie trzech kryteriów:`,
    `- **Ciekawość**: czy wpis jest fascynujący, odkrywczy, niespodziewany`,
    `- **Unikalność**: czy informacja jest rzadka, niszowa, trudna do znalezienia gdzie indziej`,
    `- **Niebanalność**: czy to coś więcej niż ogólniki — konkretne, praktyczne, nieoczywiste`,
    ``,
    `# Skala ocen — ABSOLUTNA (niezależna od innych wpisów w tym batchu)`,
    `Całe liczby od 0 do 1000:`,
    `- 0–100: banalne ogólniki, rzeczy znane każdemu turyście ("w Japonii jeżdżą pociągi")`,
    `- 100–300: standardowe porady turystyczne, użyteczne, ale powszechnie znane`,
    `- 300–500: konkretna, przydatna rada, powyżej przeciętnej wiedzy turystycznej`,
    `- 500–700: ciekawa, mniej oczywista informacja, wartościowa dla doświadczonego podróżnika`,
    `- 700–900: niszowa, zaskakująca informacja — coś czego trudno się dowiedzieć`,
    `- 900–1000: wyjątkowy gem — bardzo konkretny, trudny do znalezienia, wysoka unikalna wartość`,
    ``,
    `# Zasady`,
    `- Oceniaj ABSOLUTNIE — nie porównuj wpisów między sobą w batchu`,
    `- Zwróć TYLKO poprawny JSON array, bez markdown, bez code fences, bez komentarzy`,
    `- Każdy wpis z wejścia musi być w odpowiedzi`,
    ``,
    `# Format odpowiedzi (JSON array):`,
    `[{"id": "...", "rating": 0}, ...]`,
    ``,
    `# Wpisy do ocenienia:`,
    JSON.stringify(items, null, 2)
  ].join('\n');
}

// --- Arg parsing ---
function parseArgs(argv) {
  const args = { resume: false, batchSize: 100, dryRun: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--resume') args.resume = true;
    else if (argv[i] === '--dry-run') args.dryRun = true;
    else if (argv[i] === '--batch-size' && argv[i + 1]) {
      args.batchSize = Math.max(1, parseInt(argv[i + 1], 10) || 100);
      i++;
    }
  }
  return args;
}

// --- Main ---
async function main() {
  const args = parseArgs(process.argv.slice(2));

  if (!process.env.GEMINI_API_KEY && !args.dryRun) {
    errorLog('GEMINI_API_KEY is not set (put it in .env).');
    process.exit(1);
  }

  const genAI = args.dryRun ? null : new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
  const model = args.dryRun ? null : genAI.getGenerativeModel({ model: MODEL_NAME });

  // Load all JSON files
  const files = (await glob(path.join(BASE, '*.json').replace(/\\/g, '/'))).sort();
  log(`Found ${files.length} JSON files in ${BASE}`);

  // Load all entries with metadata
  const allEntries = [];
  const fileData = new Map();

  for (const filePath of files) {
    const data = await fs.readJson(filePath);
    fileData.set(filePath, data);
    const entries = Array.isArray(data.entries) ? data.entries : [];
    const fileName = path.basename(filePath);
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      allEntries.push({
        id: String(allEntries.length),
        filePath,
        fileName,
        entryIndex: i,
        entryId: entry.entryId,
        text: String(entry.text ?? '')
      });
    }
    log(`  ${fileName}: ${entries.length} entries`);
  }

  log(c(`Total entries: ${allEntries.length.toLocaleString()}`, 'bold'));

  // Load saved ratings from state file (for --resume)
  const savedRatings = new Map();
  if (args.resume && await fs.pathExists(STATE_FILE)) {
    try {
      const state = await fs.readJson(STATE_FILE);
      const saved = state?.ratings ?? {};
      for (const [id, rating] of Object.entries(saved)) {
        savedRatings.set(String(id), Number(rating));
      }
      log(c(`Resumed: ${savedRatings.size} ratings already saved`, 'yellow'));
    } catch (e) {
      warn('Could not read state file:', e.message);
    }
  }

  const totalBatches = Math.ceil(allEntries.length / args.batchSize);

  // Process in batches
  for (let batchStart = 0; batchStart < allEntries.length; batchStart += args.batchSize) {
    const batch = allEntries.slice(batchStart, batchStart + args.batchSize);
    const batchNum = Math.floor(batchStart / args.batchSize) + 1;

    // Check if all items in this batch already have ratings (resume mode)
    const alreadyDone = batch.every(e => savedRatings.has(e.id));
    if (alreadyDone) {
      log(c(`Batch ${batchNum}/${totalBatches}`, 'gray'), `skipped (already rated)`);
      continue;
    }

    log(c(`Batch ${batchNum}/${totalBatches}`, 'cyan', 'bold'),
      `entries ${batchStart + 1}–${batchStart + batch.length} / ${allEntries.length}`);

    if (args.dryRun) {
      log(c('Dry run', 'yellow'), 'skipping model call');
      continue;
    }

    const items = batch.map(e => ({ id: e.id, entryId: e.entryId, text: e.text }));
    const prompt = buildRatingPrompt(items);

    let parsed;
    {
      const maxParseAttempts = 5;
      let parseAttempt = 0;
      while (true) {
        parseAttempt++;
        try {
          const raw = await generateWithRetry(model, prompt);
          parsed = tryParseJsonArray(raw);
          break;
        } catch (err) {
          const isParseError = err instanceof SyntaxError || /json/i.test(err.message) || /token/i.test(err.message);
          if (isParseError && parseAttempt < maxParseAttempts) {
            warn(c(`Batch ${batchNum}: JSON parse error, retrying (${parseAttempt}/${maxParseAttempts})...`, 'yellow'), err.message.slice(0, 120));
            await new Promise(r => setTimeout(r, 1500 * parseAttempt));
            continue;
          }
          errorLog(`Batch ${batchNum} failed after ${parseAttempt} attempt(s):`, err.message);
          warn('Saving progress and stopping. Re-run with --resume to continue.');
          process.exit(1);
        }
      }
    }

    // Build id->entry map for this batch
    const batchById = new Map(batch.map(e => [e.id, e]));

    let matched = 0;
    const touchedFiles = new Set();
    for (const item of parsed) {
      const id = String(item?.id ?? '');
      const raw = Number(item?.rating);
      if (!Number.isFinite(raw)) continue;
      const entry = batchById.get(id);
      if (!entry) continue;
      const rating = Math.round(Math.max(0, Math.min(1000, raw)));
      savedRatings.set(id, rating);
      fileData.get(entry.filePath).entries[entry.entryIndex].rating = rating;
      touchedFiles.add(entry.filePath);
      matched++;
    }

    log(`Batch ${batchNum}: ${matched}/${batch.length} ratings received`);
    if (matched < batch.length) {
      warn(`Missing ${batch.length - matched} ratings in this batch`);
    }

    // Write affected files immediately after each batch
    for (const filePath of touchedFiles) {
      const data = fileData.get(filePath);
      const entries = Array.isArray(data.entries) ? data.entries : [];
      const rated = entries.filter(e => typeof e.rating === 'number').length;
      log(c('  saving', 'blue'), `${path.basename(filePath)} — ${rated}/${entries.length} entries rated`);
      await fs.writeJson(filePath, data, { spaces: 2 });
    }

    // Save progress to state file after each batch
    await fs.ensureDir(path.dirname(STATE_FILE));
    await fs.writeJson(STATE_FILE, {
      model: MODEL_NAME,
      batchSize: args.batchSize,
      totalEntries: allEntries.length,
      ratedSoFar: savedRatings.size,
      ratings: Object.fromEntries(savedRatings),
      updatedAt: ts()
    }, { spaces: 2 });
  }

  if (args.dryRun) {
    log(c('Dry run complete — no changes written.', 'yellow'));
    return;
  }

  const totalRated = [...savedRatings.values()].length;
  log(c(`Done! ${totalRated}/${allEntries.length} entries rated.`, 'green', 'bold'));
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});

import 'dotenv/config';
import { GoogleGenerativeAI } from '@google/generative-ai';
import fs from 'fs-extra';
import { glob } from 'glob';
import path from 'path';

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

const MODEL_NAME =
  process.env.MODEL_NAME ||
  process.env.WRITE_MODEL_NAME ||
  'gemini-2.5-flash';
const COMPENDIUM_DIR = './compendium_versions';
const INPUT_PATTERNS = [
  './KARCZMA_JAPAN/slim/**/*.json',
  './NODODON_JAPAN/slim/**/*.json',
  './NODODON_JAPAN_THREAD/slim/**/*.json'
];

const generateSystemPrompt = (currentJsonFilePath) => `
# Role: Japan Travel Compendium Curator & Insertion Planner

# Objective
You will receive **raw JSON chat logs** (slim format) from: \`${currentJsonFilePath}\`

Your job: (1) FILTER messages worth keeping, (2) ASSIGN each kept message to a category. Output operations only for messages you keep. Use \`position: "append"\` for all (we merge into compendium server-side).

# Filter Rules (KEEP if useful to a stranger visiting Japan)
- **KEEP:** Concrete place/venue/shop names, event info, prices, routes, comparisons, reservations, warnings/pitfalls.
- **KEEP:** General protips, travel hacks, life hacks (e.g., "add places to Apple Maps", "how to use Suica").
- **KEEP:** Otaku: Shops (Mandarake, Surugaya), Arcades (Taito, Round1), Events (Comiket, WonFes), Raves (Mogra), Themed Cafes.
- **REJECT:** Personal anecdotes, emotions, "UF", battery talk, "I met X", gifts, photo requests, "I arrived at X time", generic chatter, memes, one-word reactions.

# Available Categories (H2 headers)
- \`## Transport & Logistics\`
- \`## Connectivity\`
- \`## Otaku: Shopping & Goods\`
- \`## Otaku: Arcades & Rhythm Games\`
- \`## Otaku: Events, Raves & Idols\`
- \`## Otaku: Themed Cafes\`
- \`## Food & Dining (Jedzenie)\`
- \`## Accommodation (Noclegi)\`
- \`## Sightseeing\`
- \`## Shopping (General/Fashion)\`
- \`## Money & Budget\`
- \`## Seasonal\`
- \`## Cultural Etiquette & Tips\`

# Output Format (STRICT)
Return ONE valid JSON object (no markdown, no code fences):
{
  "operations": [
    {
      "messageId": "string",
      "category": "## Category Name",
      "position": "append"
    }
  ],
  "notes": "optional brief comment"
}

Always use \`"position": "append"\`. Categories are created automatically if missing.
If nothing is worth keeping: \`{"operations":[],"notes":"..."}\`
`;

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const THINKING_BUDGET = Number.parseInt(process.env.THINKING_BUDGET ?? '', 10) || 24576;

const model = genAI.getGenerativeModel({
  model: MODEL_NAME,
  generationConfig: {
    thinkingConfig: {
      thinkingBudget: THINKING_BUDGET
    }
  }
});

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

async function getSortedInputFiles() {
  let allFiles = [];
  for (const pattern of INPUT_PATTERNS) {
    const files = await glob(pattern);
    allFiles = allFiles.concat(files);
  }
  return allFiles.sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }));
}

async function getLatestVersion() {
  await fs.ensureDir(COMPENDIUM_DIR);
  const files = await fs.readdir(COMPENDIUM_DIR);

  const versionFiles = files
    .filter(f => f.startsWith('compendium_') && f.endsWith('.md'))
    .map(f => {
      const match = f.match(/compendium_(\d+)\.md/);
      return match ? { name: f, version: parseInt(match[1], 10) } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.version - a.version);

  return versionFiles.length > 0 ? versionFiles[0] : null;
}

function getNextVersionFilename(currentVersion) {
  const nextNum = (currentVersion ? currentVersion.version : 0) + 1;
  const paddedNum = String(nextNum).padStart(3, '0');
  return path.join(COMPENDIUM_DIR, `compendium_${paddedNum}.md`);
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

async function generateTextWithRetry(modelToUse, promptParts, attempt = 1) {
  const maxAttempts = Number.parseInt(process.env.RETRY_MAX_ATTEMPTS ?? '', 10) || 30;
  const baseDelay = Number.parseInt(process.env.RETRY_BASE_DELAY_MS ?? '', 10) || 1500;
  const maxDelay = Number.parseInt(process.env.RETRY_MAX_DELAY_MS ?? '', 10) || 120000;

  try {
    const modelLabel = `model:${MODEL_NAME}`;
    const sendSize = approxChars(promptParts);
    const t0 = Date.now();
    log(c(`${modelLabel} send`, 'magenta'), `attempt=${attempt}/${maxAttempts}`, `chars≈${sendSize.toLocaleString()}`);
    const result = await modelToUse.generateContent(promptParts);
    const text = result.response.text();
    const dt = Date.now() - t0;
    log(c(`${modelLabel} recv`, 'green'), `ms=${dt}`, `chars=${String(text ?? '').length.toLocaleString()}`);
    return text;
  } catch (error) {
    if (attempt > maxAttempts) {
      throw error;
    }

    if (isRetryableError(error)) {
      const exp = baseDelay * Math.pow(2, attempt - 1);
      const delay = Math.min(maxDelay, exp) + Math.floor(Math.random() * 750);
      const status = error?.status ?? error?.cause?.status;
      const code = getErrorCode(error);
      const modelLabel = `model:${MODEL_NAME}`;
      const msg = String(error?.message ?? '').slice(0, 200).replace(/\s+/g, ' ').trim();
      const bits = [`${modelLabel} retry`, `attempt=${attempt}/${maxAttempts}`];
      if (status) bits.push(`status=${status}`);
      if (code) bits.push(`code=${code}`);
      if (msg) bits.push(`msg="${msg}"`);
      bits.push(`sleep=${(delay / 1000).toFixed(1)}s`);
      warn(c(bits.join(' | '), 'yellow'));
      await new Promise(resolve => setTimeout(resolve, delay));
      return generateTextWithRetry(modelToUse, promptParts, attempt + 1);
    }
    throw error;
  }
}

function buildMessageIndex(slimData) {
  const idx = new Map();
  if (Array.isArray(slimData?.messages)) {
    for (const m of slimData.messages) {
      if (m.id) idx.set(String(m.id), m);
    }
  }
  return idx;
}

function formatMessageBlock(msg, msgIndex) {
  const id = String(msg?.id ?? '');
  const ts = msg?.timestamp;
  const dateStr = ts ? ts.split('T')[0] : 'UNKNOWN-DATE';
  const author = msg?.author ? `[${msg.author}] ` : '';
  const content = String(msg?.content ?? '').trim();

  let block = `[${dateStr}] ${author}${content}[${id}]`;

  if (msg?.replyTo && msgIndex.has(String(msg.replyTo))) {
    const replyMsg = msgIndex.get(String(msg.replyTo));
    const snippet = String(replyMsg?.content ?? '').slice(0, 60).trim();
    block = `> In reply to: "${snippet}"\n${block}`;
  } else if (msg?.replyTo) {
    block = `> (Reply to ID: ${msg.replyTo})\n${block}`;
  }

  return block;
}

function applyOperations(markdownLines, operations, msgIndex) {
  const categoryIndex = new Map();
  const messageIdToLine = new Map();

  for (let i = 0; i < markdownLines.length; i++) {
    const line = markdownLines[i];
    if (line.startsWith('## ')) {
      categoryIndex.set(line.trim(), i);
    }
    const idMatch = line.match(/\[(\d+)\]$/);
    if (idMatch) {
      messageIdToLine.set(idMatch[1], i);
    }
  }

  const newLines = [...markdownLines];
  let insertedCount = 0;

  for (const op of operations) {
    const msgId = String(op.messageId);
    if (!msgIndex.has(msgId)) {
      warn(`Message ID ${msgId} not found in input data, skipping`);
      continue;
    }

    if (messageIdToLine.has(msgId)) {
      log(`Message ID ${msgId} already exists in compendium, skipping`);
      continue;
    }

    const msg = msgIndex.get(msgId);
    const block = formatMessageBlock(msg, msgIndex);
    const category = op.category?.trim();

    let catLine = categoryIndex.get(category);

    if (catLine === undefined) {
      newLines.push('', category, '');
      catLine = newLines.length - 2;
      categoryIndex.set(category, catLine);
      log(`Created category: ${category}`);
    }

    let insertLine = catLine + 1;

    if (typeof op.position === 'string') {
      if (op.position === 'prepend') {
        insertLine = catLine + 1;
      } else if (op.position === 'append') {
        let nextCat = newLines.length;
        for (let j = catLine + 1; j < newLines.length; j++) {
          if (newLines[j].startsWith('## ')) {
            nextCat = j;
            break;
          }
        }
        insertLine = nextCat;
      }
    } else if (op.position?.afterId) {
      const refLine = messageIdToLine.get(String(op.position.afterId));
      if (refLine !== undefined) {
        insertLine = refLine + 1;
      } else {
        warn(`afterId ${op.position.afterId} not found, defaulting to append`);
        let nextCat = newLines.length;
        for (let j = catLine + 1; j < newLines.length; j++) {
          if (newLines[j].startsWith('## ')) {
            nextCat = j;
            break;
          }
        }
        insertLine = nextCat;
      }
    } else if (op.position?.beforeId) {
      const refLine = messageIdToLine.get(String(op.position.beforeId));
      if (refLine !== undefined) {
        insertLine = refLine;
      } else {
        warn(`beforeId ${op.position.beforeId} not found, defaulting to prepend`);
        insertLine = catLine + 1;
      }
    }

    newLines.splice(insertLine, 0, block);
    messageIdToLine.set(msgId, insertLine);
    insertedCount++;

    for (const [cat, idx] of categoryIndex.entries()) {
      if (idx >= insertLine) categoryIndex.set(cat, idx + 1);
    }
    for (const [id, ln] of messageIdToLine.entries()) {
      if (ln >= insertLine && id !== msgId) messageIdToLine.set(id, ln + 1);
    }
  }

  log(c('Applied operations', 'green'), `inserted=${insertedCount}/${operations.length}`);
  return newLines.join('\n');
}

async function main() {
  if (!process.env.GEMINI_API_KEY) {
    errorLog("Error: GEMINI_API_KEY is not set in .env file.");
    process.exit(1);
  }

  log(`Model: ${MODEL_NAME}`);
  log(`Thinking budget: ${THINKING_BUDGET} tokens`);

  const inputFiles = await getSortedInputFiles();
  log(`Found ${inputFiles.length} input files.`);

  if (inputFiles.length === 0) return;

  let latestVersion = await getLatestVersion();
  let processedCount = latestVersion ? latestVersion.version : 0;

  const filesToProcess = inputFiles.slice(processedCount);
  log(`Resuming from index ${processedCount}. Processing ${filesToProcess.length} files.`);

  for (let i = 0; i < filesToProcess.length; i++) {
    const file = filesToProcess[i];
    const fileIndex = processedCount + i;

    const cleanPath = path.relative('.', file).replace(/\\/g, '/');

    log(c(`File ${fileIndex + 1}/${inputFiles.length}`, 'cyan', 'bold'), cleanPath);

    let contextContent = "";
    if (latestVersion) {
      contextContent = await fs.readFile(path.join(COMPENDIUM_DIR, latestVersion.name), 'utf-8');
      if (contextContent.length > 3000000) warn("WARNING: Context > 3M chars.");
    }

    const jsonContent = await fs.readFile(file, 'utf-8');
    log(c('Input JSON', 'blue'), `chars=${jsonContent.length.toLocaleString()}`);

    let slimData = null;
    try {
      slimData = JSON.parse(jsonContent);
      const msgs = Array.isArray(slimData?.messages) ? slimData.messages : null;
      if (!slimData || !msgs || msgs.length === 0) {
        warn("Empty JSON, skipping API call, bumping version.");
        const nextPath = getNextVersionFilename(latestVersion);
        await fs.outputFile(nextPath, contextContent);
        latestVersion = { name: path.basename(nextPath), version: (latestVersion?.version || 0) + 1 };
        log(c('Saved', 'green', 'bold'), nextPath);
        continue;
      }
    } catch (e) {
      warn("Invalid JSON, attempting to process as raw text...");
    }

    const dynamicPrompt = generateSystemPrompt(cleanPath);

    const promptParts = [
      dynamicPrompt,
      "### RAW CHAT LOGS (JSON):",
      jsonContent
    ];

    let ops = [];
    try {
      log(c('Model', 'magenta', 'bold'), "start");
      const responseText = await generateTextWithRetry(model, promptParts);
      const parsed = tryParseJsonObject(responseText);
      if (parsed && Array.isArray(parsed.operations)) {
        ops = parsed.operations;
      }
    } catch (err) {
      errorLog(`Model step failed on file ${file}:`, err);
      continue;
    }

    const inputTotal = slimData?.messages?.length ?? 0;
    const kept = ops.length;
    const discarded = Math.max(0, inputTotal - kept);
    log(c('Model', 'magenta', 'bold'), "done", `keep=${kept}`, `discarded=${discarded}`, `total=${inputTotal}`);

    if (ops.length === 0) {
      log("No operations to apply, bumping version with unchanged content.");
      const nextPath = getNextVersionFilename(latestVersion);
      await fs.outputFile(nextPath, contextContent);
      latestVersion = { name: path.basename(nextPath), version: (latestVersion?.version || 0) + 1 };
      log(c('Saved', 'green', 'bold'), nextPath);
      continue;
    }

    const msgIndex = buildMessageIndex(slimData);
    const markdownLines = contextContent.split('\n');
    const newContent = applyOperations(markdownLines, ops, msgIndex);

    const nextVersionPath = getNextVersionFilename(latestVersion);
    await fs.outputFile(nextVersionPath, newContent);
    log(c('Saved', 'green', 'bold'), nextVersionPath, `chars=${newContent.length.toLocaleString()}`);

    latestVersion = {
      name: path.basename(nextVersionPath),
      version: (latestVersion ? latestVersion.version : 0) + 1
    };
  }

  log("Processing complete");
}

main().catch(console.error);

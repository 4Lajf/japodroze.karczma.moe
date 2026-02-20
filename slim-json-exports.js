import fs from 'fs-extra';
import { glob } from 'glob';
import path from 'path';

function ts() {
  return new Date().toISOString();
}

function log(...args) {
  console.log(`[${ts()}]`, ...args);
}

function parseArgs(argv) {
  const args = {
    overwrite: false,
    dryRun: false,
    keepEmptyContent: false,
    keepHeader: false
  };

  for (const a of argv) {
    if (a === '--overwrite') args.overwrite = true;
    else if (a === '--dry-run') args.dryRun = true;
    else if (a === '--keep-empty-content') args.keepEmptyContent = true;
    else if (a === '--keep-header') args.keepHeader = true;
  }
  return args;
}

const INPUT_PATTERNS = [
  './KARCZMA_JAPAN/split/**/*.json',
  './NODODON_JAPAN/split/**/*.json',
  './NODODON_JAPAN_THREAD/split/**/*.json'
];

function toPosix(p) {
  return String(p).replace(/\\/g, '/');
}

function pickAuthor(msg) {
  const name = msg?.author?.name;
  const v = typeof name === 'string' && name.trim() ? name.trim() : '';
  return v || null;
}

function slimMessage(msg, opts) {
  const content = typeof msg?.content === 'string' ? msg.content : '';
  const contentTrim = content.trim();
  if (!opts.keepEmptyContent && contentTrim.length === 0) return null;

  return {
    id: msg?.id ?? null,
    timestamp: msg?.timestamp ?? null,
    author: pickAuthor(msg),
    content: content,
    replyTo: msg?.reference?.messageId ?? null
  };
}

function makeOutputPath(inputPath) {
  const rel = toPosix(path.relative('.', inputPath));
  if (rel.includes('/split/')) return path.resolve(rel.replace('/split/', '/slim/'));
  return path.resolve(rel.replace(/^\.\//, ''));
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));

  let files = [];
  for (const pattern of INPUT_PATTERNS) {
    const found = await glob(pattern);
    files = files.concat(found);
  }
  files = files
    .map(toPosix)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: 'base' }));

  if (files.length === 0) {
    log('No input JSON files found.');
    return;
  }

  log(`Found ${files.length} JSON file(s) to slim.`);
  log(`Options: overwrite=${opts.overwrite} dryRun=${opts.dryRun} keepEmptyContent=${opts.keepEmptyContent} keepHeader=${opts.keepHeader}`);

  let totalInMsgs = 0;
  let totalOutMsgs = 0;
  let skipped = 0;

  for (let i = 0; i < files.length; i++) {
    const inPath = files[i];
    const outPath = makeOutputPath(inPath);

    if (!opts.overwrite && (await fs.pathExists(outPath))) {
      skipped++;
      continue;
    }

    const data = await fs.readJson(inPath);
    const inMessages = Array.isArray(data?.messages) ? data.messages : [];

    const outMessages = [];
    for (const m of inMessages) {
      const sm = slimMessage(m, opts);
      if (sm) outMessages.push(sm);
    }

    totalInMsgs += inMessages.length;
    totalOutMsgs += outMessages.length;

    const outObj = opts.keepHeader
      ? {
          guild: data?.guild ? { id: data.guild.id ?? null, name: data.guild.name ?? null } : undefined,
          channel: data?.channel ? { id: data.channel.id ?? null, name: data.channel.name ?? null, category: data.channel.category ?? null } : undefined,
          exportedAt: data?.exportedAt ?? null,
          messages: outMessages
        }
      : { messages: outMessages };

    const pct = inMessages.length ? ((outMessages.length / inMessages.length) * 100).toFixed(1) : '0.0';
    log(`(${i + 1}/${files.length}) ${toPosix(inPath)} -> ${toPosix(outPath)} msgs ${outMessages.length}/${inMessages.length} (${pct}%)`);

    if (!opts.dryRun) {
      await fs.ensureDir(path.dirname(outPath));
      await fs.writeFile(outPath, JSON.stringify(outObj), 'utf8');
    }
  }

  log(`Done. inputMsgs=${totalInMsgs} outputMsgs=${totalOutMsgs} skippedFiles=${skipped}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});


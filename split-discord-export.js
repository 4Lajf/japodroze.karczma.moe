#!/usr/bin/env node
/**
 * Streams Discord export JSON files from NODODON_JAPAN_THREAD, NODODON_JAPAN, KARCZMA_JAPAN
 * and splits them into files at 1000 messages each.
 * Fixes attachment/avatar/icon paths to point to the correct _Files folder locations.
 */

import fs from 'fs';
import path from 'path';
import Chain from 'stream-chain';
import streamJson from 'stream-json';
import Pick from 'stream-json/filters/Pick.js';
import StreamValues from 'stream-json/streamers/StreamValues.js';

const chain = Chain.chain ?? Chain.make;
const { parser } = streamJson;
const pick = Pick.pick ?? Pick.make;
const streamValues = StreamValues.streamValues ?? StreamValues.make;

import { fileURLToPath } from 'url';
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(__dirname);
const SOURCE_DIRS = ['NODODON_JAPAN_THREAD', 'NODODON_JAPAN', 'KARCZMA_JAPAN'];
const MESSAGES_PER_FILE = 100;

/**
 * Recursively fix paths in an object. Paths like "{jsonName}_Files\\file.png"
 * become "{relativePath}/{jsonName}_Files/file.png" (relative to output split folder).
 */
function fixPathsInObject(obj, jsonBasename) {
  if (obj === null || obj === undefined) return obj;

  if (typeof obj === 'string') {
    const filesMatch = obj.match(/^(.+\.json)_Files[\\/](.+)$/);
    if (filesMatch) {
      const filename = filesMatch[2];
      const filesFolder = `${jsonBasename}_Files`;
      // Split files are in {sourceDir}/split/, so ../ goes to sourceDir where _Files lives
      return path.join('..', filesFolder, filename).replace(/\\/g, '/');
    }
    return obj;
  }

  if (Array.isArray(obj)) {
    return obj.map((item) => fixPathsInObject(item, jsonBasename));
  }

  if (typeof obj === 'object') {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
      result[key] = fixPathsInObject(value, jsonBasename);
    }
    return result;
  }

  return obj;
}

function parseHeader(jsonPath) {
  const chunk = fs.readFileSync(jsonPath, { encoding: 'utf8', flag: 'r' });
  const msgIdx = chunk.indexOf('"messages"');
  if (msgIdx === -1) throw new Error('Could not find messages array');
  const bracketIdx = chunk.indexOf('[', msgIdx);
  if (bracketIdx === -1) throw new Error('Could not find messages array start');
  const beforeMessages = chunk.substring(0, msgIdx).trimEnd();
  const commaIdx = beforeMessages.lastIndexOf(',');
  const headerStr = commaIdx > 0 ? beforeMessages.substring(0, commaIdx) + '}' : beforeMessages + '}';
  return JSON.parse(headerStr);
}

function writePart(header, messages, splitDir, jsonBasename, partIndex) {
  const partName = `${jsonBasename}_part${String(partIndex).padStart(3, '0')}.json`;
  const outputPath = path.join(splitDir, partName);
  const output = { ...header, messages };
  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2), 'utf8');
}

/**
 * Process a single JSON file: stream messages, split into chunks, fix paths, write output.
 */
function processFile(jsonPath, sourceDirName) {
  const jsonBasename = path.basename(jsonPath, '.json');
  const splitDir = path.join(ROOT_DIR, sourceDirName, 'split');
  fs.mkdirSync(splitDir, { recursive: true });

  const header = parseHeader(jsonPath);
  if (header.guild?.iconUrl) {
    header.guild.iconUrl = fixPathsInObject(header.guild.iconUrl, jsonBasename);
  }

  let messageBuffer = [];
  let partIndex = 1;
  let totalMessages = 0;

  return new Promise((resolve, reject) => {
    const pipeline = chain([
      fs.createReadStream(jsonPath),
      parser(),
      pick({ filter: /^messages\.\d+$/ }),
      streamValues(),
    ]);

    pipeline
      .on('data', (data) => {
        const value = data.value ?? data;
        if (value && typeof value === 'object') {
          const msg = fixPathsInObject(value, jsonBasename);
          messageBuffer.push(msg);
          totalMessages++;

          if (messageBuffer.length >= MESSAGES_PER_FILE) {
            writePart(header, messageBuffer, splitDir, jsonBasename, partIndex);
            partIndex++;
            messageBuffer = [];
          }
        }
      })
      .on('end', () => {
        if (messageBuffer.length > 0) {
          writePart(header, messageBuffer, splitDir, jsonBasename, partIndex);
        }
        console.log(`  ${jsonBasename}: ${totalMessages} messages → ${partIndex} part(s)`);
        resolve();
      })
      .on('error', reject);
  });
}

async function main() {
  console.log('Splitting Discord export JSON files (1000 messages per file)...\n');

  for (const dirName of SOURCE_DIRS) {
    const dirPath = path.join(ROOT_DIR, dirName);
    if (!fs.existsSync(dirPath)) {
      console.warn(`Directory not found: ${dirPath}`);
      continue;
    }

    const files = fs
      .readdirSync(dirPath)
      .filter((f) => f.endsWith('.json') && !f.includes('_Files') && !f.includes('_part'));
    if (files.length === 0) {
      console.log(`${dirName}: No JSON files found`);
      continue;
    }

    console.log(`${dirName}:`);
    for (const file of files) {
      const jsonPath = path.join(dirPath, file);
      try {
        await processFile(jsonPath, dirName);
      } catch (err) {
        console.error(`  Error processing ${file}:`, err.message);
      }
    }
  }

  console.log('\nDone.');
}

main().catch(console.error);

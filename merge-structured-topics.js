#!/usr/bin/env node
/**
 * Merges JSON topic files in groups and renumbers entryIds to be continuous.
 * Usage: node merge-structured-topics.js
 */

import { readFileSync, writeFileSync, unlinkSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const BASE = join(__dirname, 'structured_topics', 'compendium_931');

const GROUPS = [
  {
    name: 'A',
    files: ['accommodation-noclegi.json', 'accommodation.json'],
    output: 'accommodation.json',
  },
  {
    name: 'B',
    files: [
      'general-protips-travel-hacks-life-hacks.json',
      'general-protips-travel-hacks.json',
      'general-protips.json',
    ],
    output: 'general-protips.json',
  },
  {
    name: 'C',
    files: ['shopping-general-fashion.json', 'shopping-general.json'],
    output: 'shopping-general.json',
  },
  {
    name: 'D',
    files: ['otaku-themed-cafes.json', 'themed-cafes.json'],
    output: 'themed-cafes.json',
  },
];

function loadJson(path) {
  const raw = readFileSync(path, 'utf-8');
  const data = JSON.parse(raw);
  return Array.isArray(data.entries) ? data.entries : [];
}

function mergeGroup(group) {
  const allEntries = [];
  for (const file of group.files) {
    const path = join(BASE, file);
    try {
      const entries = loadJson(path);
      allEntries.push(...entries);
      console.log(`  + ${file}: ${entries.length} entries`);
    } catch (err) {
      console.error(`  ! ${file}: ${err.message}`);
    }
  }

  // Renumber entryIds continuously (0:e1, 0:e2, 0:e3, ...)
  const merged = allEntries.map((entry, i) => ({
    ...entry,
    entryId: `0:e${i + 1}`,
  }));

  return merged;
}

function run() {
  console.log('Merging structured topic files...\n');

  for (const group of GROUPS) {
    console.log(`Group ${group.name}:`);
    const merged = mergeGroup(group);
    const outputPath = join(BASE, group.output);
    const output = { entries: merged };
    writeFileSync(outputPath, JSON.stringify(output, null, 2), 'utf-8');
    console.log(`  -> ${group.output}: ${merged.length} entries (IDs 0:e1..0:e${merged.length})`);

    for (const file of group.files) {
      if (file !== group.output) {
        unlinkSync(join(BASE, file));
        console.log(`  - deleted ${file}`);
      }
    }
    console.log('');
  }

  console.log('Done.');
}

run();

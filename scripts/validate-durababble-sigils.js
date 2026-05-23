#!/usr/bin/env node
/*
 * Validates that all [DURABABBLE-*] formal-model sigils appear in the Ruby
 * implementation/specs and that Ruby-side sigils exist in the Alloy model.
 */

const fs = require("fs");
const path = require("path");

const TAG_REGEX = /\[DURABABBLE-[A-Z0-9-]+\]/g;
const MODEL_DIRS = ["formal"];
const RUBY_DIRS = ["lib", "test"];

function findRoot(start) {
  let dir = start;
  while (!fs.existsSync(path.join(dir, "durababble.gemspec"))) {
    const parent = path.dirname(dir);
    if (parent === dir) {
      throw new Error("Could not find repository root (missing durababble.gemspec)");
    }
    dir = parent;
  }
  return dir;
}

function findFiles(root, dirs, predicate, results = []) {
  for (const dir of dirs) {
    const fullDir = path.join(root, dir);
    if (!fs.existsSync(fullDir)) continue;
    walk(fullDir, predicate, results);
  }
  return results;
}

function walk(dir, predicate, results) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walk(fullPath, predicate, results);
    } else if (entry.isFile() && predicate(fullPath)) {
      results.push(fullPath);
    }
  }
}

function extract(files, root) {
  const tags = new Set();
  const locations = new Map();

  for (const file of files) {
    const text = fs.readFileSync(file, "utf8");
    const matches = text.match(TAG_REGEX) || [];
    for (const tag of matches) {
      tags.add(tag);
      if (!locations.has(tag)) locations.set(tag, []);
      locations.get(tag).push(path.relative(root, file));
    }
  }

  return { tags, locations };
}

function difference(left, right) {
  return [...left].filter((tag) => !right.has(tag)).sort();
}

function printTagList(title, tags, locations) {
  console.log(title);
  for (const tag of tags) {
    console.log(`  ${tag} (${(locations.get(tag) || []).join(", ")})`);
  }
}

function printMatched(tags, modelLocations, rubyLocations) {
  console.log("Matched tags:");
  for (const tag of tags) {
    console.log(`  ${tag}`);
    console.log(`    Alloy: ${(modelLocations.get(tag) || []).join(", ")}`);
    console.log(`    Ruby:  ${(rubyLocations.get(tag) || []).join(", ")}`);
  }
}

function main() {
  const root = findRoot(process.cwd());
  const modelFiles = findFiles(root, MODEL_DIRS, (file) => file.endsWith(".als"));
  const rubyFiles = findFiles(root, RUBY_DIRS, (file) => file.endsWith(".rb"));

  if (modelFiles.length === 0) {
    console.error("No Alloy model files found under formal/");
    process.exit(1);
  }
  if (rubyFiles.length === 0) {
    console.error("No Ruby implementation/spec files found under lib/ or test/");
    process.exit(1);
  }

  const model = extract(modelFiles, root);
  const ruby = extract(rubyFiles, root);
  const onlyInModel = difference(model.tags, ruby.tags);
  const onlyInRuby = difference(ruby.tags, model.tags);
  const inBoth = [...model.tags].filter((tag) => ruby.tags.has(tag)).sort();

  console.log("Durababble sigil validation");
  console.log("============================");
  console.log(`Alloy files: ${modelFiles.length}`);
  console.log(`Ruby files:  ${rubyFiles.length}`);
  console.log(`Matched tags: ${inBoth.length}`);
  console.log(`Only in Alloy: ${onlyInModel.length}`);
  console.log(`Only in Ruby:  ${onlyInRuby.length}`);
  console.log();

  let failed = false;
  if (onlyInModel.length > 0) {
    failed = true;
    printTagList("Tags in Alloy but missing from Ruby:", onlyInModel, model.locations);
    console.log();
  }

  if (onlyInRuby.length > 0) {
    failed = true;
    printTagList("Tags in Ruby but missing from Alloy:", onlyInRuby, ruby.locations);
    console.log();
  }

  if (process.argv.includes("--verbose") || process.argv.includes("-v")) {
    printMatched(inBoth, model.locations, ruby.locations);
  }

  process.exit(failed ? 1 : 0);
}

main();

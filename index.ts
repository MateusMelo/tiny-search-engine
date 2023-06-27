import { createReadStream } from "node:fs";
import { Parser, parse } from "csv-parse";
import { argv, exit } from "node:process";

const INDEX: Map<number, string> = new Map(); // TODO: Persists at disk
const INVERTED_INDEX: Map<string, number[]> = new Map(); // TODO: Persists at disk
const DATASET_PATH = `./dataset/large-dataset.csv`; // TODO: Get dataset path from process.argv

interface SearchResult {
  id: number,
  text: string
}

function normalize(value: string): string[] {
  const terms: string[] = value.split(" ");
  const normalizedTerms: string[] = [];
  for (let i = 0; i < terms.length; i++) {
    const term: string = terms[i].replace(/[^a-z0-9]/gi, "");
    if (term) {
      normalizedTerms.push(term.toLowerCase());
    }
  }

  return normalizedTerms;
}

function search(text: string): SearchResult[] {
  let matchs: number[] = [];
  for (const token of normalize(text)) {
    if (INVERTED_INDEX.has(token)) {
      const ids: number[] = INVERTED_INDEX.get(token) || [];
      if (matchs.length === 0) {
        matchs.push(...ids);
      } else {
        matchs = matchs.filter(value => ids.includes(value));
      }
    }
  }

  return matchs.map(id => ({ id, text: INDEX.get(id) || "" }));;
}

async function buildIndex(): Promise<void> {
  const parser: Parser = createReadStream(DATASET_PATH).pipe(
    parse({
      delimiter: ";",
      escape: "\\",
    })
  );

  let lines: number = 0;
  for await (const record of parser) {
    if (lines > 0) {
      const [id, text] = record;
      const parsedId: number = Number.parseInt(id);
      INDEX.set(parsedId, text);
      for (const token of normalize(text)) {
        if (INVERTED_INDEX.has(token)) {
          const ids: number[] = INVERTED_INDEX.get(token) || [];
          if (ids.indexOf(parsedId) === -1) {
            ids.push(parsedId);
          }
        } else {
          INVERTED_INDEX.set(token, [parsedId]);
        }
      }
    }
    lines++;
  }
}

(async () => {
  if (argv.length <= 2) {
    console.log("ERROR: Missing search params\n");

    console.log("Usage: ");
    console.log('npm start <query>');
    console.log('<query>    The query filter.');

    exit(69);
  }

  const query = argv[2];

  console.log("[Index] Building...");
  console.time("[Index] Took");
  await buildIndex(); // TODO: Load index from disk instead of build at runtime
  console.log("[Index] Finished.");
  console.timeEnd("[Index] Took");

  console.log("[Search] Searching...");
  console.time("[Search] Took");
  
  const results: SearchResult[] = search(query);

  console.log("[Search] Results: ");
  console.log(results);

  console.timeEnd("[Search] Took");
})();

import { createReadStream } from "node:fs";
import { Parser, parse } from "csv-parse";

const INDEX: Map<number, string> = new Map(); // TODO: Persists at disk
const INVERTED_INDEX: Map<string, number[]> = new Map(); // TODO: Persists at disk
const DATASET_PATH = `./dataset/small-dataset.csv`; // TODO: Get dataset path from process.argv

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

function search(text: string): number[] {
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

  console.log("[Search] Results: ");
  for (const id of matchs) {
    console.log(`${id} - ${INDEX.get(id)}`);
  }
  return matchs;
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
  console.log("[Index] Building...");
  console.time("[Index] Took");
  await buildIndex(); // TODO: Load index from disk instead of build at runtime
  console.log("[Index] Finished.");
  console.timeEnd("[Index] Took");

  console.log("[Search] Searching...");
  console.time("[Search] Took");
  
  search("generation is a process"); // TODO: Get query from process.argv

  console.timeEnd("[Search] Took");
})();

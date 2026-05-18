import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import i18n from "../../i18n";
import {
  currentLang,
  compareExpansionCandidates,
  createExpansionSearch,
  createSavedExpansionSearch,
  generateDecisionMemo,
  getExpansionCandidateMemo,
  getExpansionCandidates,
  getExpansionRecommendationReport,
  getExpansionSearch,
  getSavedExpansionSearch,
  listSavedExpansionSearches,
  updateSavedExpansionSearch,
} from "./expansionAdvisor";
import type { ExpansionBrief } from "./expansionAdvisor";

/* ── currentLang() — locale resolution ── */

describe("currentLang — normalizes i18n.language to the two backend values", () => {
  const original = i18n.language;
  const setLang = (value: string | undefined) => {
    (i18n as unknown as { language?: string }).language = value;
  };
  afterEach(() => setLang(original));

  it.each([
    ["en", "en"],
    ["en-US", "en"],
    ["ar", "ar"],
    ["ar-SA", "ar"],
    ["de", "en"],
  ])("maps %s -> %s", (input, expected) => {
    setLang(input);
    expect(currentLang()).toBe(expected);
  });

  it("falls back to en for null/undefined locales", () => {
    setLang(undefined);
    expect(currentLang()).toBe("en");
  });
});

/* ── Handler lang plumbing — 11 handlers ── */

function mockJsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  }) as unknown as Response;
}

const brief: ExpansionBrief = {
  brand_name: "X",
  category: "QSR",
  service_model: "qsr",
  min_area_m2: 100,
  max_area_m2: 300,
  target_districts: [],
  existing_branches: [],
  limit: 15,
};

type HandlerCase = {
  name: string;
  /** "query" handlers carry lang in the URL; "body" handlers in the JSON body. */
  kind: "query" | "body";
  run: () => Promise<unknown>;
};

// Re-derived from live source: 6 GET (lang via query param) + 5 POST/PATCH
// (lang via JSON body). The two GETs without a backend lang param
// (/districts, /branch-suggestions) are intentionally excluded.
const HANDLERS: HandlerCase[] = [
  { name: "getExpansionSearch", kind: "query", run: () => getExpansionSearch("s1") },
  { name: "getExpansionCandidates", kind: "query", run: () => getExpansionCandidates("s1") },
  { name: "getExpansionCandidateMemo", kind: "query", run: () => getExpansionCandidateMemo("c1") },
  { name: "getExpansionRecommendationReport", kind: "query", run: () => getExpansionRecommendationReport("s1") },
  { name: "listSavedExpansionSearches", kind: "query", run: () => listSavedExpansionSearches() },
  { name: "getSavedExpansionSearch", kind: "query", run: () => getSavedExpansionSearch("sv1") },
  { name: "createExpansionSearch", kind: "body", run: () => createExpansionSearch(brief) },
  { name: "compareExpansionCandidates", kind: "body", run: () => compareExpansionCandidates("s1", ["c1", "c2"]) },
  { name: "createSavedExpansionSearch", kind: "body", run: () => createSavedExpansionSearch({ search_id: "s1", title: "T", status: "draft" }) },
  { name: "updateSavedExpansionSearch", kind: "body", run: () => updateSavedExpansionSearch("sv1", { title: "T2" }) },
  // generateDecisionMemo is called without an explicit lang so the default
  // (currentLang()) is exercised.
  { name: "generateDecisionMemo", kind: "body", run: () => generateDecisionMemo({ id: "c1" }, { brand_name: "X" }) },
];

describe("Expansion Advisor handlers thread the active locale per request", () => {
  const original = i18n.language;
  let fetchMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchMock = vi.fn().mockResolvedValue(mockJsonResponse({ memo: {} }));
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    (i18n as unknown as { language?: string }).language = original;
  });

  for (const lang of ["en", "ar"] as const) {
    describe(`under lang=${lang}`, () => {
      for (const handler of HANDLERS) {
        it(`${handler.name} sends lang=${lang}`, async () => {
          (i18n as unknown as { language?: string }).language = lang;
          await handler.run();
          const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
          if (handler.kind === "query") {
            expect(String(url)).toContain(`lang=${lang}`);
          } else {
            expect(init?.body).toBeTruthy();
            const body = JSON.parse(String(init.body));
            expect(body.lang).toBe(lang);
          }
        });
      }
    });
  }
});

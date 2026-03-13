/**
 * SERP Clustering Pipeline
 *
 * Clusters keywords by shared ranking URLs (SERP overlap).
 * All clustering signals are SERP-based only — no semantic/lexical similarity.
 *
 * Pipeline:
 *   parseCsv → buildKeywordSerps → computePairSimilarities → buildClusters
 *   → buildContentPlanRows + buildClusterMemberDiagnostics → CSV exports
 *
 * IMPORTANT — Transitive clustering risk:
 *   Connected-components (Union-Find) clustering means A~B and B~C implies A,B,C
 *   are in one cluster even if A and C share zero URLs. This is a known property,
 *   not a bug. Use weakMembers diagnostics and the pruneWeakMembers option to
 *   detect and optionally remove transitive-contaminated members.
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type MonthlyVolume = { month: string; volume: number };

export type RawSerpRow = {
  keyword: string;
  position: number;
  url: string;
  volume: number;
  /** Raw string value of the "Volume historical" column, e.g. "2025-02:60500, 2025-03:49500". */
  volumeHistoricalRaw: string;
  rowNumber: number;
};

export type ValidationIssueLevel = 'warning' | 'error';

export type ValidationIssue = {
  level: ValidationIssueLevel;
  code:
    | 'MISSING_COLUMN'
    | 'INVALID_POSITION'
    | 'INVALID_VOLUME'
    | 'EMPTY_KEYWORD'
    | 'EMPTY_URL'
    | 'DUPLICATE_ROW'
    | 'INCONSISTENT_VOLUME'
    | 'POSITION_OUT_OF_RANGE'
    | 'FILTERED_ROW';
  rowNumber?: number;
  message: string;
};

export type ParsedDataset = {
  rows: RawSerpRow[];
  issues: ValidationIssue[];
};

export type KeywordSerp = {
  keyword: string;
  volume: number;
  /** Ordered list of normalized URLs by position (may contain duplicates if two
   * positions normalize to the same URL after normalization). */
  urls: string[];
  /** Deduplicated set of normalized URLs — used for display and client URL detection. */
  normalizedUrlSet: Set<string>;
  /** Maps normalized URL → best (lowest) position in this SERP. */
  orderedUrlPositions: Map<string, number>;
  /** Domain-level URL set — used for clustering overlap comparison.
   *  Collapses all pages from the same domain into one entry, preventing
   *  false negatives when a domain ranks with multiple different URLs. */
  clusteringUrlSet: Set<string>;
  /** Domain → best (lowest) position across all URLs from that domain in this SERP.
   *  Used for weighted Jaccard scoring at the domain level. */
  clusteringPositions: Map<string, number>;
  /** Raw row count before position filtering. */
  originalRowCount: number;
  /** Count of unique normalized URLs kept (= normalizedUrlSet.size). */
  keptRowCount: number;
  /** Parsed monthly search volume history from the "Volume historical" column.
   *  Sorted chronologically. Empty array when the column is absent or empty. */
  volumeHistorical: MonthlyVolume[];
};

export type KeywordPairSimilarity = {
  keywordA: string;
  keywordB: string;
  overlapCount: number;
  jaccardScore: number;
  overlapRatioA: number;
  overlapRatioB: number;
  weightedOverlapScore: number;
  weightedJaccardScore: number;
};

export type ClusterMemberDiagnosticStatus = 'isolated' | 'weak' | 'strong';

export type ClusterMemberDiagnostic = {
  clusterId: string;
  keyword: string;
  volume: number;
  mainKeyword: string;
  /** For the main keyword itself: equals its own normalizedUrlSet.size.
   *  For secondary keywords: number of shared URLs with the main keyword. */
  overlapCountWithMain: number;
  similarityRatioWithMain: number;
  weightedSimilarityWithMain: number;
  status: ClusterMemberDiagnosticStatus;
};

export type Cluster = {
  clusterId: string;
  keywords: string[];
  mainKeyword: string;
  mainKeywordVolume: number;
  secondaryKeywords: string[];
  clusterSize: number;
  averagePairOverlap: number | null;
  averagePairJaccard: number | null;
  minimumOverlapToMainKeyword: number;
  minimumSimilarityRatioToMainKeyword: number;
  weakMembers: string[];
};

export type ContentPlanRow = {
  /** 'OPTIMISATION' if the client domain appears in any cluster SERP; 'CREATION' otherwise.
   *  Empty string when no clientDomain was provided. */
  todoSeo: 'OPTIMISATION' | 'CREATION' | '';
  /**
   * Most representative client URL in the cluster (frequency → avg position → on main SERP).
   * Empty when no clientDomain is provided or no client URL found in any cluster SERP.
   */
  clientUrl: string;
  clusterId: string;
  intentMainKeyword: string;
  mainKeywordVolume: number;
  /** Intent type inferred from the top 3 competitor URLs using editorial/product path patterns. */
  intentType: 'BLOG' | 'PRODUCT' | 'MIXED' | '—';
  topKeywordsCell: string;
  topKeywordVolumesCell: string;
  averageKeywordVolume: number;
  /** Average volume of the top keywords in the cluster (up to 6). */
  indicativeClusterVolume: number;
  competitorUrlsCell: string;
  competitorCoverageCell: string;
  clusterSize: number;
  confidenceStatus: 'High Confidence' | 'Review' | 'Low Cohesion';
  weakMemberCount: number;
  /** Monthly volume history for the main keyword of this cluster.
   *  Sourced from the "Volume historical" Monitorank column. Empty when unavailable. */
  mainKeywordHistorical: MonthlyVolume[];
};

export type BuildOptions = {
  overlapThreshold?: number;
  minPosition?: number;
  maxPosition?: number;
  collapseWww?: boolean;
  forceHttps?: boolean;
  dropTrackingParams?: boolean;
  stripHash?: boolean;
  weakMainOverlapThreshold?: number;
  weakMainSimilarityThreshold?: number;
  /**
   * When true, weak cluster members are removed from content plan rows and
   * re-inserted as standalone single-keyword rows (isolated intent candidates).
   * Clustering data (Cluster type) is unaffected — diagnostics still show the
   * original connected-components result.
   */
  pruneWeakMembers?: boolean;
  /**
   * Client domain used to classify clusters as OPTIMISATION vs CREATION.
   * Accepts any of: "client.com", "www.client.com", "https://www.client.com".
   * Normalized internally via normalizeDomain().
   */
  clientDomain?: string;
  /**
   * When true, clusters classified as Low Cohesion receive one additional
   * stricter Union-Find pass at (overlapThreshold + 1) over their own members.
   * The resulting sub-clusters replace the original. No iterative loop —
   * exactly one refine pass per low-cohesion cluster.
   */
  autoRefineWeakClusters?: boolean;
};

export type CsvExportOptions = {
  /** Prepend UTF-8 BOM (U+FEFF). Improves Google Sheets import compatibility
   *  when opening CSV files directly (not via "Import" dialog). Default: false. */
  addBom?: boolean;
};

const DEFAULT_OPTIONS: Required<BuildOptions> = {
  overlapThreshold: 4,
  minPosition: 1,
  maxPosition: 10,
  collapseWww: true,
  forceHttps: true,
  dropTrackingParams: true,
  stripHash: true,
  weakMainOverlapThreshold: 2,
  weakMainSimilarityThreshold: 0.3,
  pruneWeakMembers: false,
  clientDomain: '',
  autoRefineWeakClusters: false,
};

// ---------------------------------------------------------------------------
// URL tracking param lists
// ---------------------------------------------------------------------------

const TRACKING_PARAM_PREFIXES = ['utm_'];

/**
 * Known advertising/analytics tracking parameters that are safe to strip.
 *
 * NOTE: "source" was intentionally removed from this list. It is a common
 * functional parameter in many CMS systems (e.g. ?source=rss, ?source=api)
 * and stripping it could cause two distinct pages to normalize to the same URL,
 * creating false SERP overlap. SERP URLs from Monitorank are almost never
 * tracking-parameter-heavy, so this is a low-risk removal.
 *
 * NOTE: "ref" is kept because in SERP contexts it is almost always a tracking
 * referral param (not a functional routing param), but this can be disabled
 * by setting dropTrackingParams: false and handling normalization upstream.
 */
const TRACKING_PARAM_NAMES = new Set([
  'gclid',
  'fbclid',
  'msclkid',
  'igshid',
  'mc_cid',
  'mc_eid',
  'ref',
  'ref_src',
]);

// ---------------------------------------------------------------------------
// Shared CSV escape utility
// ---------------------------------------------------------------------------

/**
 * Escape a single CSV cell value per RFC 4180.
 * Wraps in double quotes if the value contains a comma, double-quote, or newline.
 * Internal double-quotes are doubled ("").
 */
function escapeCsvCell(value: string | number): string {
  const text = String(value);
  const escaped = text.replace(/"/g, '""');
  if (/[",\n]/.test(escaped)) {
    return `"${escaped}"`;
  }
  return escaped;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** RFC 4180 CSV field splitter. Supports any single-character delimiter. */
function csvSplitLine(line: string, delimiter = ','): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === delimiter && !inQuotes) {
      result.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  result.push(current);
  return result;
}

/** Infer delimiter from the first line by counting `;` vs `,` occurrences. */
function detectDelimiter(firstLine: string): string {
  const semicolons = (firstLine.match(/;/g) ?? []).length;
  const commas = (firstLine.match(/,/g) ?? []).length;
  return semicolons > commas ? ';' : ',';
}

/** Matches Monitorank's dynamic datetime position-column header, e.g. "2026-02-26 15:57:02". */
const DATETIME_HEADER_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}$/;

function getPositionWeight(position: number): number {
  return 1 / position;
}

/**
 * Canonical key for an unordered keyword pair used as a Map key.
 * Uses U+0000 (null byte) as separator — guaranteed not to appear in
 * valid keyword text, preventing the split ambiguity that "||" would have.
 */
function toSortedPairKey(a: string, b: string): string {
  return a < b ? `${a}\x00${b}` : `${b}\x00${a}`;
}

function getSimilarityRatio(similarity: KeywordPairSimilarity): number {
  return Math.max(similarity.overlapRatioA, similarity.overlapRatioB);
}

function joinLines(values: Array<string | number>): string {
  return values.map((value) => String(value)).join('\n');
}

function roundToTwo(value: number): number {
  return Math.round(value * 100) / 100;
}

/**
 * Parse a Monitorank "Volume historical" cell value.
 * Expected format: "2025-02:60500, 2025-03:49500, 2025-04:60500"
 * Returns a chronologically sorted array of { month, volume } entries.
 * Malformed or empty items are silently dropped.
 */
function parseVolumeHistorical(raw: string): MonthlyVolume[] {
  if (!raw.trim()) return [];
  return raw
    .split(',')
    .map((entry) => {
      const colonIdx = entry.indexOf(':');
      if (colonIdx === -1) return null;
      const month = entry.slice(0, colonIdx).trim();
      const volume = Number(entry.slice(colonIdx + 1).trim());
      if (!month || !Number.isFinite(volume)) return null;
      return { month, volume };
    })
    .filter((item): item is MonthlyVolume => item !== null)
    .sort((a, b) => a.month.localeCompare(b.month));
}

function buildSimilarityLookup(
  similarities: KeywordPairSimilarity[],
): Map<string, KeywordPairSimilarity> {
  const lookup = new Map<string, KeywordPairSimilarity>();
  for (const similarity of similarities) {
    lookup.set(toSortedPairKey(similarity.keywordA, similarity.keywordB), similarity);
  }
  return lookup;
}

// ---------------------------------------------------------------------------
// CSV Parser
// ---------------------------------------------------------------------------

/**
 * Parse a Monitorank-style CSV export.
 *
 * LIMITATION: This parser splits on newlines before field-level parsing.
 * Input fields containing embedded newlines (RFC 4180 multi-line cells)
 * will be misread. This is acceptable for Monitorank exports where no field
 * (keyword, position, URL, volume) would legitimately span multiple lines.
 */
export function parseCsv(input: string): ParsedDataset {
  const issues: ValidationIssue[] = [];

  const trimmed = input.replace(/^\uFEFF/, '');
  const lines = trimmed
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  if (lines.length === 0) {
    return {
      rows: [],
      issues: [
        {
          level: 'error',
          code: 'MISSING_COLUMN',
          message: 'The CSV file is empty.',
        },
      ],
    };
  }

  // Auto-detect delimiter (supports Monitorank ";" exports and generic "," CSVs)
  const delimiter = detectDelimiter(lines[0]);
  const header = csvSplitLine(lines[0], delimiter).map((value) => value.trim().toLowerCase());

  const columnIndex = new Map<string, number>();
  header.forEach((name, index) => columnIndex.set(name, index));

  // Alias: Monitorank uses "Keywords" (plural) instead of "keyword"
  if (!columnIndex.has('keyword') && columnIndex.has('keywords')) {
    columnIndex.set('keyword', columnIndex.get('keywords')!);
  }

  // Detect dynamic datetime position column (e.g. "2026-02-26 15:57:02")
  if (!columnIndex.has('position')) {
    for (const [name, idx] of columnIndex.entries()) {
      if (DATETIME_HEADER_RE.test(name)) {
        columnIndex.set('position', idx);
        break;
      }
    }
  }

  const requiredColumns = ['keyword', 'position', 'url', 'volume'];
  for (const requiredColumn of requiredColumns) {
    if (!columnIndex.has(requiredColumn)) {
      issues.push({
        level: 'error',
        code: 'MISSING_COLUMN',
        message: `Missing required column: ${requiredColumn}`,
      });
    }
  }

  if (issues.some((issue) => issue.level === 'error')) {
    return { rows: [], issues };
  }

  // "Position type" column index — used to filter to organic results only
  const positionTypeColIndex = columnIndex.get('position type') ?? -1;
  // "Volume historical" column — optional, present in full Monitorank exports
  const volumeHistoricalColIndex = columnIndex.get('volume historical') ?? -1;

  const rows: RawSerpRow[] = [];
  const duplicateTracker = new Set<string>();
  let filteredRowCount = 0;

  for (let i = 1; i < lines.length; i += 1) {
    const rowNumber = i + 1;
    const values = csvSplitLine(lines[i], delimiter);

    // Filter: keep only "Natural results" rows (organic rankings)
    if (positionTypeColIndex >= 0) {
      const posType = (values[positionTypeColIndex] ?? '').trim().toLowerCase();
      if (posType !== 'natural results') {
        filteredRowCount += 1;
        continue;
      }
    }

    const keyword = (values[columnIndex.get('keyword')!] ?? '').trim();
    const positionRaw = (values[columnIndex.get('position')!] ?? '').trim();
    const url = (values[columnIndex.get('url')!] ?? '').trim();
    const volumeRaw = (values[columnIndex.get('volume')!] ?? '').trim();
    const volumeHistoricalRaw =
      volumeHistoricalColIndex >= 0
        ? (values[volumeHistoricalColIndex] ?? '').trim()
        : '';

    if (!keyword) {
      issues.push({
        level: 'error',
        code: 'EMPTY_KEYWORD',
        rowNumber,
        message: `Row ${rowNumber}: keyword is empty.`,
      });
      continue;
    }

    if (!url) {
      issues.push({
        level: 'error',
        code: 'EMPTY_URL',
        rowNumber,
        message: `Row ${rowNumber}: url is empty.`,
      });
      continue;
    }

    const position = Number(positionRaw);
    if (!Number.isFinite(position)) {
      issues.push({
        level: 'error',
        code: 'INVALID_POSITION',
        rowNumber,
        message: `Row ${rowNumber}: position must be numeric.`,
      });
      continue;
    }

    const volume = Number(volumeRaw);
    if (!Number.isFinite(volume)) {
      issues.push({
        level: 'error',
        code: 'INVALID_VOLUME',
        rowNumber,
        message: `Row ${rowNumber}: volume must be numeric.`,
      });
      continue;
    }

    const duplicateKey = `${keyword}__${position}__${url}__${volume}`;
    if (duplicateTracker.has(duplicateKey)) {
      issues.push({
        level: 'warning',
        code: 'DUPLICATE_ROW',
        rowNumber,
        message: `Row ${rowNumber}: duplicate row detected and ignored.`,
      });
      continue;
    }

    duplicateTracker.add(duplicateKey);
    rows.push({ keyword, position, url, volume, volumeHistoricalRaw, rowNumber });
  }

  if (filteredRowCount > 0) {
    issues.push({
      level: 'warning',
      code: 'FILTERED_ROW',
      message: `${filteredRowCount} rows skipped — Position type ≠ "Natural results".`,
    });
  }

  return { rows, issues };
}

// ---------------------------------------------------------------------------
// URL Normalization
// ---------------------------------------------------------------------------

export function normalizeUrl(rawUrl: string, options: BuildOptions = {}): string {
  const config = { ...DEFAULT_OPTIONS, ...options };

  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    try {
      parsed = new URL(`https://${rawUrl}`);
    } catch {
      return rawUrl.trim().toLowerCase();
    }
  }

  const protocol = config.forceHttps ? 'https:' : parsed.protocol.toLowerCase();

  let hostname = parsed.hostname.toLowerCase();
  if (config.collapseWww && hostname.startsWith('www.')) {
    hostname = hostname.slice(4);
  }

  const pathname =
    parsed.pathname.replace(/\/+$/g, '').replace(/\/+/g, '/') || '/';

  const searchParams = new URLSearchParams(parsed.search);
  if (config.dropTrackingParams) {
    for (const key of [...searchParams.keys()]) {
      const lower = key.toLowerCase();
      if (
        TRACKING_PARAM_NAMES.has(lower) ||
        TRACKING_PARAM_PREFIXES.some((prefix) => lower.startsWith(prefix))
      ) {
        searchParams.delete(key);
      }
    }
  }

  const sortedSearch = [...searchParams.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
    .join('&');

  const hash = config.stripHash ? '' : parsed.hash;
  const search = sortedSearch ? `?${sortedSearch}` : '';
  const port = parsed.port ? `:${parsed.port}` : '';

  return `${protocol}//${hostname}${port}${pathname}${search}${hash}`;
}

// ---------------------------------------------------------------------------
// Keyword SERP builder
// ---------------------------------------------------------------------------

export function buildKeywordSerps(
  rows: RawSerpRow[],
  options: BuildOptions = {},
): { serps: KeywordSerp[]; issues: ValidationIssue[] } {
  const config = { ...DEFAULT_OPTIONS, ...options };
  const issues: ValidationIssue[] = [];
  const grouped = new Map<string, RawSerpRow[]>();

  for (const row of rows) {
    if (row.position < config.minPosition || row.position > config.maxPosition) {
      issues.push({
        level: 'warning',
        code: 'POSITION_OUT_OF_RANGE',
        rowNumber: row.rowNumber,
        message: `Row ${row.rowNumber}: position ${row.position} ignored (outside ${config.minPosition}-${config.maxPosition}).`,
      });
      continue;
    }

    const list = grouped.get(row.keyword) ?? [];
    list.push(row);
    grouped.set(row.keyword, list);
  }

  const serps: KeywordSerp[] = [];

  for (const [keyword, keywordRows] of grouped.entries()) {
    const volumeSet = new Set(keywordRows.map((row) => row.volume));
    if (volumeSet.size > 1) {
      issues.push({
        level: 'warning',
        code: 'INCONSISTENT_VOLUME',
        message: `Keyword "${keyword}" has inconsistent volume values: ${[...volumeSet].join(', ')}. Using the maximum value.`,
      });
    }
    const selectedVolume = Math.max(...volumeSet);

    const byPosition = new Map<number, string>();
    keywordRows
      .slice()
      .sort((a, b) => a.position - b.position)
      .forEach((row) => {
        const normalized = normalizeUrl(row.url, config);
        if (!byPosition.has(row.position)) {
          byPosition.set(row.position, normalized);
        }
      });

    const positionEntries = [...byPosition.entries()].sort((a, b) => a[0] - b[0]);
    const urls = positionEntries.map((entry) => entry[1]);

    const orderedUrlPositions = new Map<string, number>();
    for (const [position, normalizedUrl] of positionEntries) {
      if (!orderedUrlPositions.has(normalizedUrl)) {
        orderedUrlPositions.set(normalizedUrl, position);
      }
    }

    const normalizedUrlSet = new Set(urls);

    // Domain-level clustering: collapse each URL to its bare hostname so that
    // two pages from the same domain count as one shared signal.
    const clusteringPositions = new Map<string, number>();
    for (const [normalizedUrl, position] of orderedUrlPositions.entries()) {
      const domain = normalizeDomain(normalizedUrl);
      if (!clusteringPositions.has(domain) || position < clusteringPositions.get(domain)!) {
        clusteringPositions.set(domain, position);
      }
    }
    const clusteringUrlSet = new Set(clusteringPositions.keys());

    // Historical volume: take the first non-empty value across rows for this keyword
    const historicalRaw =
      keywordRows.find((r) => r.volumeHistoricalRaw)?.volumeHistoricalRaw ?? '';
    const volumeHistorical = parseVolumeHistorical(historicalRaw);

    serps.push({
      keyword,
      volume: selectedVolume,
      urls,
      normalizedUrlSet,
      orderedUrlPositions,
      clusteringUrlSet,
      clusteringPositions,
      originalRowCount: keywordRows.length,
      keptRowCount: normalizedUrlSet.size,
      volumeHistorical,
    });
  }

  serps.sort((a, b) => a.keyword.localeCompare(b.keyword));

  return { serps, issues };
}

// ---------------------------------------------------------------------------
// Pair similarity scoring
// ---------------------------------------------------------------------------

export function scoreKeywordPair(
  a: KeywordSerp,
  b: KeywordSerp,
): KeywordPairSimilarity {
  let overlapCount = 0;
  let weightedIntersection = 0;
  let weightSumA = 0;
  let weightSumB = 0;

  // Weights are summed at the URL level (orderedUrlPositions).
  // These weighted metrics are diagnostic only — they do NOT drive clustering.
  for (const [, position] of a.orderedUrlPositions.entries()) {
    weightSumA += getPositionWeight(position);
  }
  for (const [, position] of b.orderedUrlPositions.entries()) {
    weightSumB += getPositionWeight(position);
  }

  const smaller = a.normalizedUrlSet.size <= b.normalizedUrlSet.size ? a : b;
  const larger = smaller === a ? b : a;

  for (const url of smaller.normalizedUrlSet) {
    if (!larger.normalizedUrlSet.has(url)) {
      continue;
    }
    overlapCount += 1;
    const positionA = a.orderedUrlPositions.get(url)!;
    const positionB = b.orderedUrlPositions.get(url)!;
    // Weighted intersection: min(w_A, w_B) per shared URL — diagnostic only.
    weightedIntersection += Math.min(
      getPositionWeight(positionA),
      getPositionWeight(positionB),
    );
  }

  const unionCount = a.normalizedUrlSet.size + b.normalizedUrlSet.size - overlapCount;
  const weightedUnion = weightSumA + weightSumB - weightedIntersection;

  return {
    keywordA: a.keyword,
    keywordB: b.keyword,
    overlapCount,
    jaccardScore: unionCount > 0 ? overlapCount / unionCount : 0,
    overlapRatioA:
      a.normalizedUrlSet.size > 0 ? overlapCount / a.normalizedUrlSet.size : 0,
    overlapRatioB:
      b.normalizedUrlSet.size > 0 ? overlapCount / b.normalizedUrlSet.size : 0,
    weightedOverlapScore: weightedIntersection,
    weightedJaccardScore: weightedUnion > 0 ? weightedIntersection / weightedUnion : 0,
  };
}

export function computePairSimilarities(
  serps: KeywordSerp[],
): KeywordPairSimilarity[] {
  const urlToKeywords = new Map<string, string[]>();
  const serpByKeyword = new Map<string, KeywordSerp>();

  for (const serp of serps) {
    serpByKeyword.set(serp.keyword, serp);
    for (const url of serp.normalizedUrlSet) {
      const keywords = urlToKeywords.get(url) ?? [];
      keywords.push(serp.keyword);
      urlToKeywords.set(url, keywords);
    }
  }

  const candidatePairs = new Set<string>();
  for (const keywords of urlToKeywords.values()) {
    keywords.sort((a, b) => a.localeCompare(b));
    for (let i = 0; i < keywords.length; i += 1) {
      for (let j = i + 1; j < keywords.length; j += 1) {
        candidatePairs.add(toSortedPairKey(keywords[i], keywords[j]));
      }
    }
  }

  const similarities: KeywordPairSimilarity[] = [];
  for (const pairKey of candidatePairs) {
    // FIX: split on \x00 (null byte) instead of "||" to avoid ambiguity
    const separatorIndex = pairKey.indexOf('\x00');
    const keywordA = pairKey.slice(0, separatorIndex);
    const keywordB = pairKey.slice(separatorIndex + 1);

    const serpA = serpByKeyword.get(keywordA);
    const serpB = serpByKeyword.get(keywordB);
    if (!serpA || !serpB) {
      continue;
    }

    similarities.push(scoreKeywordPair(serpA, serpB));
  }

  similarities.sort((a, b) => {
    if (b.overlapCount !== a.overlapCount) return b.overlapCount - a.overlapCount;
    if (b.jaccardScore !== a.jaccardScore) return b.jaccardScore - a.jaccardScore;
    if (a.keywordA !== b.keywordA) return a.keywordA.localeCompare(b.keywordA);
    return a.keywordB.localeCompare(b.keywordB);
  });

  return similarities;
}

// ---------------------------------------------------------------------------
// Union-Find
// ---------------------------------------------------------------------------

class UnionFind {
  private readonly parent = new Map<string, string>();
  private readonly rank = new Map<string, number>();

  makeSet(value: string): void {
    if (!this.parent.has(value)) {
      this.parent.set(value, value);
      this.rank.set(value, 0);
    }
  }

  find(value: string): string {
    const parent = this.parent.get(value);
    if (!parent) {
      this.makeSet(value);
      return value;
    }
    if (parent === value) return value;
    const root = this.find(parent);
    this.parent.set(value, root); // path compression
    return root;
  }

  union(a: string, b: string): void {
    const rootA = this.find(a);
    const rootB = this.find(b);
    if (rootA === rootB) return;

    const rankA = this.rank.get(rootA) ?? 0;
    const rankB = this.rank.get(rootB) ?? 0;
    if (rankA < rankB) {
      this.parent.set(rootA, rootB);
    } else if (rankA > rankB) {
      this.parent.set(rootB, rootA);
    } else {
      this.parent.set(rootB, rootA);
      this.rank.set(rootA, rankA + 1);
    }
  }
}

// ---------------------------------------------------------------------------
// Cluster builder helpers
// ---------------------------------------------------------------------------

/**
 * Build a Cluster value object from an arbitrary set of keywords.
 * Used by buildClusters (main pass) and the auto-refine pass.
 * Does NOT assign a clusterId — callers assign that after sorting.
 */
function buildClusterFromKeywords(
  keywords: string[],
  keywordToSerp: Map<string, KeywordSerp>,
  similarityLookup: Map<string, KeywordPairSimilarity>,
  config: Required<BuildOptions>,
): Omit<Cluster, 'clusterId'> {
  const sorted = [...keywords].sort((a, b) => {
    const serpA = keywordToSerp.get(a)!;
    const serpB = keywordToSerp.get(b)!;
    if (serpB.volume !== serpA.volume) return serpB.volume - serpA.volume;
    return serpA.keyword.localeCompare(serpB.keyword);
  });

  const mainKeywordSerp = keywordToSerp.get(sorted[0])!;
  const secondaryKeywords = sorted.slice(1);

  let pairCount = 0;
  let overlapSum = 0;
  let jaccardSum = 0;

  for (let i = 0; i < sorted.length; i += 1) {
    for (let j = i + 1; j < sorted.length; j += 1) {
      const sim = similarityLookup.get(toSortedPairKey(sorted[i], sorted[j]));
      if (!sim) continue;
      pairCount += 1;
      overlapSum += sim.overlapCount;
      jaccardSum += sim.jaccardScore;
    }
  }

  let minimumOverlapToMainKeyword =
    secondaryKeywords.length > 0 ? Number.POSITIVE_INFINITY : 0;
  let minimumSimilarityRatioToMainKeyword =
    secondaryKeywords.length > 0 ? Number.POSITIVE_INFINITY : 0;
  const weakMembers: string[] = [];

  for (const keyword of secondaryKeywords) {
    const sim = similarityLookup.get(
      toSortedPairKey(mainKeywordSerp.keyword, keyword),
    );
    const overlapToMain = sim?.overlapCount ?? 0;
    const simRatioToMain = sim ? getSimilarityRatio(sim) : 0;

    minimumOverlapToMainKeyword = Math.min(minimumOverlapToMainKeyword, overlapToMain);
    minimumSimilarityRatioToMainKeyword = Math.min(
      minimumSimilarityRatioToMainKeyword,
      simRatioToMain,
    );

    if (
      overlapToMain < config.weakMainOverlapThreshold ||
      simRatioToMain < config.weakMainSimilarityThreshold
    ) {
      weakMembers.push(keyword);
    }
  }

  return {
    keywords: sorted,
    mainKeyword: mainKeywordSerp.keyword,
    mainKeywordVolume: mainKeywordSerp.volume,
    secondaryKeywords,
    clusterSize: sorted.length,
    averagePairOverlap: pairCount > 0 ? overlapSum / pairCount : null,
    averagePairJaccard: pairCount > 0 ? jaccardSum / pairCount : null,
    minimumOverlapToMainKeyword:
      minimumOverlapToMainKeyword === Number.POSITIVE_INFINITY
        ? 0
        : minimumOverlapToMainKeyword,
    minimumSimilarityRatioToMainKeyword:
      minimumSimilarityRatioToMainKeyword === Number.POSITIVE_INFINITY
        ? 0
        : minimumSimilarityRatioToMainKeyword,
    weakMembers,
  };
}

/**
 * Returns true if the cluster meets the Low Cohesion criteria.
 * Mirrors the same thresholds used in deriveConfidenceStatus().
 * Single-keyword clusters are never considered low cohesion (nothing to split).
 */
function isLowCohesion(cluster: Cluster, config: Required<BuildOptions>): boolean {
  if (cluster.clusterSize <= 1) return false;
  const weakRatio = cluster.weakMembers.length / cluster.clusterSize;
  const avgOverlap = cluster.averagePairOverlap ?? 0;
  return (
    weakRatio > 0.34 ||
    cluster.minimumOverlapToMainKeyword < Math.max(1, config.weakMainOverlapThreshold - 1) ||
    avgOverlap < Math.max(1, config.overlapThreshold - 1)
  );
}

/** Sort + ID-stamp a flat cluster array. Mutates sort order in place. */
function sortAndAssignIds(clusters: Array<Omit<Cluster, 'clusterId'>>): Cluster[] {
  clusters.sort((a, b) => {
    if (b.mainKeywordVolume !== a.mainKeywordVolume)
      return b.mainKeywordVolume - a.mainKeywordVolume;
    if (b.clusterSize !== a.clusterSize) return b.clusterSize - a.clusterSize;
    return a.mainKeyword.localeCompare(b.mainKeyword);
  });
  return clusters.map((cluster, index) => ({
    ...cluster,
    clusterId: `CL-${String(index + 1).padStart(4, '0')}`,
  }));
}

// ---------------------------------------------------------------------------
// Cluster builder
// ---------------------------------------------------------------------------

export function buildClusters(
  serps: KeywordSerp[],
  similarities: KeywordPairSimilarity[],
  options: BuildOptions = {},
): Cluster[] {
  const config = { ...DEFAULT_OPTIONS, ...options };
  const unionFind = new UnionFind();
  const keywordToSerp = new Map(serps.map((serp) => [serp.keyword, serp]));
  const similarityLookup = buildSimilarityLookup(similarities);

  for (const serp of serps) {
    unionFind.makeSet(serp.keyword);
  }

  for (const similarity of similarities) {
    if (similarity.overlapCount >= config.overlapThreshold) {
      unionFind.union(similarity.keywordA, similarity.keywordB);
    }
  }

  const groupedKeywords = new Map<string, string[]>();
  for (const serp of serps) {
    const root = unionFind.find(serp.keyword);
    const list = groupedKeywords.get(root) ?? [];
    list.push(serp.keyword);
    groupedKeywords.set(root, list);
  }

  // Build initial cluster objects
  const rawClusters: Array<Omit<Cluster, 'clusterId'>> = [
    ...groupedKeywords.values(),
  ].map((keywords) =>
    buildClusterFromKeywords(keywords, keywordToSerp, similarityLookup, config),
  );

  const initialClusters = sortAndAssignIds(rawClusters);

  // Optional one-pass auto-refine: re-cluster Low Cohesion clusters at threshold+1
  if (!config.autoRefineWeakClusters) {
    return initialClusters;
  }

  const higherThreshold = config.overlapThreshold + 1;
  const refined: Array<Omit<Cluster, 'clusterId'>> = [];

  for (const cluster of initialClusters) {
    if (!isLowCohesion(cluster, config)) {
      refined.push(cluster);
      continue;
    }

    // Sub-cluster this cluster's keywords at the stricter threshold
    const subUF = new UnionFind();
    for (const kw of cluster.keywords) {
      subUF.makeSet(kw);
    }

    const clusterKwSet = new Set(cluster.keywords);
    for (const sim of similarities) {
      if (
        clusterKwSet.has(sim.keywordA) &&
        clusterKwSet.has(sim.keywordB) &&
        sim.overlapCount >= higherThreshold
      ) {
        subUF.union(sim.keywordA, sim.keywordB);
      }
    }

    const subGroups = new Map<string, string[]>();
    for (const kw of cluster.keywords) {
      const root = subUF.find(kw);
      const group = subGroups.get(root) ?? [];
      group.push(kw);
      subGroups.set(root, group);
    }

    for (const subKeywords of subGroups.values()) {
      refined.push(
        buildClusterFromKeywords(subKeywords, keywordToSerp, similarityLookup, config),
      );
    }
  }

  return sortAndAssignIds(refined);
}

// ---------------------------------------------------------------------------
// Cluster member diagnostics
// ---------------------------------------------------------------------------

export function buildClusterMemberDiagnostics(
  clusters: Cluster[],
  serps: KeywordSerp[],
  similarities: KeywordPairSimilarity[],
  options: BuildOptions = {},
): ClusterMemberDiagnostic[] {
  const config = { ...DEFAULT_OPTIONS, ...options };
  const similarityLookup = buildSimilarityLookup(similarities);
  const serpByKeyword = new Map(serps.map((serp) => [serp.keyword, serp]));
  const diagnostics: ClusterMemberDiagnostic[] = [];

  for (const cluster of clusters) {
    for (const keyword of cluster.keywords) {
      const serp = serpByKeyword.get(keyword);
      if (!serp) continue;

      if (keyword === cluster.mainKeyword) {
        diagnostics.push({
          clusterId: cluster.clusterId,
          keyword,
          volume: serp.volume,
          mainKeyword: cluster.mainKeyword,
          // FIX: use normalizedUrlSet.size (unique URLs used in scoring),
          // not urls.length (raw position slots that may include duplicates
          // when two positions normalize to the same URL).
          overlapCountWithMain: serp.normalizedUrlSet.size,
          similarityRatioWithMain: 1,
          weightedSimilarityWithMain: 1,
          status: cluster.clusterSize === 1 ? 'isolated' : 'strong',
        });
        continue;
      }

      const similarity = similarityLookup.get(
        toSortedPairKey(keyword, cluster.mainKeyword),
      );
      const overlapCountWithMain = similarity?.overlapCount ?? 0;
      const similarityRatioWithMain = similarity ? getSimilarityRatio(similarity) : 0;
      const weightedSimilarityWithMain = similarity?.weightedJaccardScore ?? 0;

      let status: ClusterMemberDiagnosticStatus = 'strong';
      if (cluster.clusterSize === 1) {
        status = 'isolated';
      } else if (
        overlapCountWithMain < config.weakMainOverlapThreshold ||
        similarityRatioWithMain < config.weakMainSimilarityThreshold
      ) {
        status = 'weak';
      }

      diagnostics.push({
        clusterId: cluster.clusterId,
        keyword,
        volume: serp.volume,
        mainKeyword: cluster.mainKeyword,
        overlapCountWithMain,
        similarityRatioWithMain,
        weightedSimilarityWithMain,
        status,
      });
    }
  }

  diagnostics.sort((a, b) => {
    if (a.clusterId !== b.clusterId) return a.clusterId.localeCompare(b.clusterId);
    if (b.volume !== a.volume) return b.volume - a.volume;
    return a.keyword.localeCompare(b.keyword);
  });

  return diagnostics;
}

// ---------------------------------------------------------------------------
// Domain normalization + client detection
// ---------------------------------------------------------------------------

/**
 * Normalize a user-supplied client domain to a bare hostname.
 * Accepts any of: "client.com", "www.client.com", "https://www.client.com/path".
 * Returns lowercase bare hostname, e.g. "client.com". Returns "" on empty input.
 */
export function normalizeDomain(input: string): string {
  const trimmed = input.trim().toLowerCase();
  if (!trimmed) return '';
  const withoutProtocol = trimmed.replace(/^https?:\/\//, '');
  const withoutWww = withoutProtocol.replace(/^www\./, '');
  // Drop path, query, and hash
  return withoutWww.split('/')[0].split('?')[0].split('#')[0];
}

/**
 * Returns true if a normalized URL belongs to clientDomain (exact match or any subdomain).
 * clientDomain must already be normalized (no protocol, no www, no path).
 */
function urlBelongsToClient(normalizedUrl: string, clientDomain: string): boolean {
  try {
    const hostname = new URL(normalizedUrl).hostname.toLowerCase().replace(/^www\./, '');
    return hostname === clientDomain || hostname.endsWith(`.${clientDomain}`);
  } catch {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Client URL selection
// ---------------------------------------------------------------------------

/**
 * Find the most representative client URL across a set of keyword SERPs.
 *
 * Ranking:
 *   1. Frequency — how many keyword SERPs contain this client URL (desc)
 *   2. Average position across those appearances (asc — closer to top is better)
 *   3. Presence on the main keyword SERP (present = preferred)
 *
 * Returns empty string when clientDomainNormalized is empty or no match found.
 */
function findBestClientUrl(
  activeKeywords: string[],
  mainKeyword: string,
  serpByKeyword: Map<string, KeywordSerp>,
  clientDomainNormalized: string,
): string {
  if (!clientDomainNormalized) return '';

  const frequency = new Map<string, number>();
  const positionSum = new Map<string, number>();
  const positionCount = new Map<string, number>();

  for (const kw of activeKeywords) {
    const serp = serpByKeyword.get(kw);
    if (!serp) continue;
    for (const [url, position] of serp.orderedUrlPositions.entries()) {
      if (!urlBelongsToClient(url, clientDomainNormalized)) continue;
      frequency.set(url, (frequency.get(url) ?? 0) + 1);
      positionSum.set(url, (positionSum.get(url) ?? 0) + position);
      positionCount.set(url, (positionCount.get(url) ?? 0) + 1);
    }
  }

  if (frequency.size === 0) return '';

  // Determine which client URLs appear on the main keyword SERP (tie-break 3)
  const onMainSet = new Set<string>();
  const mainSerp = serpByKeyword.get(mainKeyword);
  if (mainSerp) {
    for (const [url] of mainSerp.orderedUrlPositions.entries()) {
      if (urlBelongsToClient(url, clientDomainNormalized)) {
        onMainSet.add(url);
      }
    }
  }

  const ranked = [...frequency.keys()]
    .map((url) => ({
      url,
      frequency: frequency.get(url) ?? 0,
      avgPosition: (positionSum.get(url) ?? 0) / (positionCount.get(url) ?? 1),
      onMain: onMainSet.has(url) ? 0 : 1, // 0 sorts before 1 (prefer on-main)
    }))
    .sort((a, b) => {
      if (b.frequency !== a.frequency) return b.frequency - a.frequency;
      if (a.avgPosition !== b.avgPosition) return a.avgPosition - b.avgPosition;
      return a.onMain - b.onMain;
    });

  return ranked[0]?.url ?? '';
}

// ---------------------------------------------------------------------------
// Intent type detection
// ---------------------------------------------------------------------------

const EDITORIAL_PATTERNS = [
  /\/blog(\/|$)/i,
  /\/news(\/|$)/i,
  /\/guides?(\/|$)/i,
  /\/articles?(\/|$)/i,
  /\/insights?(\/|$)/i,
  /\/academy(\/|$)/i,
  /\/learn(\/|$)/i,
  /\/resources?(\/|$)/i,
  /\/magazine(\/|$)/i,
  /\/actualit[eé]s?(\/|$)/i,
  /\/wiki(\/|$)/i,
  /\/help(\/|$)/i,
  /\/faqs?(\/|$)/i,
  /\/tutorials?(\/|$)/i,
  /\/tips?(\/|$)/i,
  /\/howto(\/|$)/i,
  /\/how-to(\/|$)/i,
  /\/glossary(\/|$)/i,
  /\/definitions?(\/|$)/i,
  /\/case-studi(es|y)(\/|$)/i,
  /\/ebooks?(\/|$)/i,
  /\/whitepapers?(\/|$)/i,
  /\/white-papers?(\/|$)/i,
  /\/formations?(\/|$)/i,
  /\/conseils?(\/|$)/i,
  /\/dossiers?(\/|$)/i,
  /\/podcasts?(\/|$)/i,
  /\/webinars?(\/|$)/i,
  // French question patterns
  /\/comment-/i,
  /\/pourquoi-/i,
  /\/quest-ce-que/i,
  /\/qu-est-ce-que/i,
  // English question-style URL slugs
  /\/how-to-/i,
  /\/what-is-/i,
  /\/what-are-/i,
  /\/why-\w/i,
];

const PRODUCT_PATTERNS = [
  /\/products?(\/|$)/i,
  /\/services?(\/|$)/i,
  /\/solutions?(\/|$)/i,
  /\/platform(\/|$)/i,
  /\/software(\/|$)/i,
  /\/tools?(\/|$)/i,
  /\/pricing(\/|$)/i,
  /\/demo(\/|$)/i,
  /\/features?(\/|$)/i,
  /\/shop(\/|$)/i,
  /\/store(\/|$)/i,
  /\/boutique(\/|$)/i,
  /\/tarifs?(\/|$)/i,
  /\/abonnements?(\/|$)/i,
  /\/subscribe(\/|$)/i,
  /\/free-trial(\/|$)/i,
  /\/collections?(\/|$)/i,
  /\/catalog(?:ue)?(\/|$)/i,
  /\/buy(\/|$)/i,
  /\/acheter(\/|$)/i,
];

/**
 * Secondary intent signals derived from keyword text (treated like page-title heuristics).
 * Applied only when URL-pattern signals produce no match.
 */
const EDITORIAL_TITLE_PATTERNS = [
  // French interrogatives
  /\bcomment\b/i,
  /\bpourquoi\b/i,
  /\bqu['\u2019]est[- ]ce que\b/i,
  /\bqu['\u2019]est[- ]ce qu['\u2019]/i,
  /\bc['\u2019]est quoi\b/i,
  // English interrogatives / how-to signals
  /^how\b/i,
  /\bhow to\b/i,
  /^why\b/i,
  /^what\b/i,
  /^when\b/i,
  /^where\b/i,
  /\bwhat is\b/i,
  /\bwhat are\b/i,
  // Editorial / informational noun signals
  /\bguide\b/i,
  /\btutorial\b/i,
  /\bglossary\b/i,
  /\bdéfinition\b/i,
  /\bexplication\b/i,
  /\bconseils?\b/i,
  /\bastuces?\b/i,
  /\btips?\b/i,
  // Literal question
  /\?$/,
];

const PRODUCT_TITLE_PATTERNS = [
  // Pricing / commercial terms
  /\bprix\b/i,
  /\btarif\b/i,
  /\bdevis\b/i,
  /\bprice\b/i,
  /\bpricing\b/i,
  // Purchase intent
  /\bacheter\b/i,
  /\bbuy\b/i,
  /\bcommande\b/i,
  // Product / platform signals
  /\bsoftware\b/i,
  /\blogiciel\b/i,
  /\boutill?s?\b/i,
  /\bplatform\b/i,
  /\bdemo\b/i,
  // Subscription
  /\bsouscription\b/i,
  /\bsubscri(be|ption)\b/i,
  /\bessai gratuit\b/i,
  /\bfree[- ]trial\b/i,
];

/**
 * Classify the intent of a cluster from its top 3 competitor URLs.
 * When URL patterns find no signal, falls back to keyword-text heuristics.
 * Returns 'BLOG', 'PRODUCT', 'MIXED', or '—' (no pattern matched).
 */
function detectIntentType(
  competitorUrls: string[],
  clusterKeywords: string[] = [],
): 'BLOG' | 'PRODUCT' | 'MIXED' | '—' {
  const top3 = competitorUrls.slice(0, 3);
  let editorial = 0;
  let product = 0;

  // Primary signal: URL path patterns
  for (const url of top3) {
    if (EDITORIAL_PATTERNS.some((re) => re.test(url))) editorial++;
    if (PRODUCT_PATTERNS.some((re) => re.test(url))) product++;
  }

  // Secondary signal: keyword-text heuristics (only when URL patterns are silent)
  if (editorial === 0 && product === 0 && clusterKeywords.length > 0) {
    for (const kw of clusterKeywords) {
      if (EDITORIAL_TITLE_PATTERNS.some((re) => re.test(kw))) editorial++;
      if (PRODUCT_TITLE_PATTERNS.some((re) => re.test(kw))) product++;
    }
  }

  if (editorial > 0 && product > 0) return 'MIXED';
  if (editorial > 0) return 'BLOG';
  if (product > 0) return 'PRODUCT';
  return '—';
}

// ---------------------------------------------------------------------------
// Content plan row builder
// ---------------------------------------------------------------------------

export function buildContentPlanRows(
  clusters: Cluster[],
  serps: KeywordSerp[],
  options: BuildOptions = {},
): ContentPlanRow[] {
  const config = { ...DEFAULT_OPTIONS, ...options };
  const serpByKeyword = new Map(serps.map((serp) => [serp.keyword, serp]));

  const buildRepresentativeCompetitorUrls = (
    cluster: Cluster,
    activeKeywords: string[],
  ): { urls: string[]; coverage: string[] } => {
    const frequency = new Map<string, number>();
    const prominence = new Map<string, number>();
    const mainKeywordSerp = serpByKeyword.get(cluster.mainKeyword);
    const mainKeywordPositions =
      mainKeywordSerp?.orderedUrlPositions ?? new Map<string, number>();

    for (const keyword of activeKeywords) {
      const serp = serpByKeyword.get(keyword);
      if (!serp) continue;
      for (const [url, position] of serp.orderedUrlPositions.entries()) {
        frequency.set(url, (frequency.get(url) ?? 0) + 1);
        prominence.set(url, (prominence.get(url) ?? 0) + getPositionWeight(position));
      }
    }

    const clusterSizeForCoverage = activeKeywords.length;

    const rankedUrls = [...frequency.keys()]
      .map((url) => ({
        url,
        count: frequency.get(url) ?? 0,
        prominenceScore: prominence.get(url) ?? 0,
        mainKeywordPosition:
          mainKeywordPositions.get(url) ?? Number.POSITIVE_INFINITY,
      }))
      .sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        if (b.prominenceScore !== a.prominenceScore)
          return b.prominenceScore - a.prominenceScore;
        if (a.mainKeywordPosition !== b.mainKeywordPosition)
          return a.mainKeywordPosition - b.mainKeywordPosition;
        return a.url.localeCompare(b.url);
      })
      .slice(0, 3);

    return {
      urls: rankedUrls.map((entry) => entry.url),
      coverage: rankedUrls.map(
        (entry) => `${entry.count}/${clusterSizeForCoverage}`,
      ),
    };
  };

  const deriveConfidenceStatus = (
    cluster: Cluster,
    activeKeywordCount: number,
    activeWeakMemberCount: number,
  ): 'High Confidence' | 'Review' | 'Low Cohesion' => {
    if (activeKeywordCount === 1) return 'Review';

    const weakRatio =
      activeKeywordCount > 0 ? activeWeakMemberCount / activeKeywordCount : 0;
    const avgOverlap = cluster.averagePairOverlap ?? 0;

    if (
      activeWeakMemberCount === 0 &&
      cluster.minimumOverlapToMainKeyword >= config.overlapThreshold &&
      avgOverlap >= config.overlapThreshold
    ) {
      return 'High Confidence';
    }

    if (
      weakRatio > 0.34 ||
      cluster.minimumOverlapToMainKeyword <
        Math.max(1, config.weakMainOverlapThreshold - 1) ||
      avgOverlap < Math.max(1, config.overlapThreshold - 1)
    ) {
      return 'Low Cohesion';
    }

    return 'Review';
  };

  const clientDomainNormalized = normalizeDomain(config.clientDomain ?? '');

  const rows: ContentPlanRow[] = [];

  for (const cluster of clusters) {
    // When pruneWeakMembers is enabled, remove weak members from this cluster's
    // content plan row. Pruned keywords become standalone rows below.
    const weakSet = new Set(cluster.weakMembers);
    const activeKeywords = config.pruneWeakMembers
      ? cluster.keywords.filter((kw) => !weakSet.has(kw))
      : cluster.keywords;

    const activeWeakMemberCount = config.pruneWeakMembers
      ? 0
      : cluster.weakMembers.length;

    const rankedKeywords = activeKeywords
      .map((keyword) => serpByKeyword.get(keyword))
      .filter((serp): serp is KeywordSerp => Boolean(serp))
      .sort((a, b) => {
        if (b.volume !== a.volume) return b.volume - a.volume;
        return a.keyword.localeCompare(b.keyword);
      });

    const topKeywords = rankedKeywords.slice(0, 6);
    const totalVolume = rankedKeywords.reduce((sum, serp) => sum + serp.volume, 0);
    const averageKeywordVolume =
      rankedKeywords.length > 0 ? totalVolume / rankedKeywords.length : 0;
    const topVolumeSum = topKeywords.reduce((sum, serp) => sum + serp.volume, 0);
    const clusterVolume =
      topKeywords.length > 0 ? Math.round(topVolumeSum / topKeywords.length) : 0;

    const representative = buildRepresentativeCompetitorUrls(cluster, activeKeywords);
    const intentType = detectIntentType(representative.urls, activeKeywords);

    // ToDo SEO: OPTIMISATION if client domain found in any keyword's SERP, else CREATION
    let todoSeo: ContentPlanRow['todoSeo'] = '';
    if (clientDomainNormalized) {
      const hasClientUrl = activeKeywords.some((kw) => {
        const serp = serpByKeyword.get(kw);
        return serp
          ? [...serp.normalizedUrlSet].some((url) =>
              urlBelongsToClient(url, clientDomainNormalized),
            )
          : false;
      });
      todoSeo = hasClientUrl ? 'OPTIMISATION' : 'CREATION';
    }

    const clientUrl = findBestClientUrl(
      activeKeywords,
      cluster.mainKeyword,
      serpByKeyword,
      clientDomainNormalized,
    );

    rows.push({
      todoSeo,
      clientUrl,
      clusterId: cluster.clusterId,
      intentMainKeyword: cluster.mainKeyword,
      mainKeywordVolume: cluster.mainKeywordVolume,
      intentType,
      topKeywordsCell: joinLines(topKeywords.map((serp) => serp.keyword)),
      topKeywordVolumesCell: joinLines(topKeywords.map((serp) => serp.volume)),
      averageKeywordVolume: roundToTwo(averageKeywordVolume),
      indicativeClusterVolume: clusterVolume,
      competitorUrlsCell: joinLines(representative.urls),
      competitorCoverageCell: joinLines(representative.coverage),
      clusterSize: activeKeywords.length,
      confidenceStatus: deriveConfidenceStatus(
        cluster,
        activeKeywords.length,
        activeWeakMemberCount,
      ),
      weakMemberCount: activeWeakMemberCount,
      mainKeywordHistorical: serpByKeyword.get(cluster.mainKeyword)?.volumeHistorical ?? [],
    });

    // Create standalone rows for pruned weak members
    if (config.pruneWeakMembers) {
      for (const weakKeyword of cluster.weakMembers) {
        const weakSerp = serpByKeyword.get(weakKeyword);
        if (!weakSerp) continue;
        const weakRepresentative = buildRepresentativeCompetitorUrls(cluster, [
          weakKeyword,
        ]);
        let weakTodoSeo: ContentPlanRow['todoSeo'] = '';
        if (clientDomainNormalized) {
          const weakSerp2 = serpByKeyword.get(weakKeyword);
          const hasClientUrl2 = weakSerp2
            ? [...weakSerp2.normalizedUrlSet].some((url) =>
                urlBelongsToClient(url, clientDomainNormalized),
              )
            : false;
          weakTodoSeo = hasClientUrl2 ? 'OPTIMISATION' : 'CREATION';
        }
        const weakClientUrl = findBestClientUrl(
          [weakKeyword],
          weakKeyword,
          serpByKeyword,
          clientDomainNormalized,
        );
        rows.push({
          todoSeo: weakTodoSeo,
          clientUrl: weakClientUrl,
          clusterId: `${cluster.clusterId}-W`,
          intentMainKeyword: weakKeyword,
          mainKeywordVolume: weakSerp.volume,
          intentType: detectIntentType(weakRepresentative.urls, [weakKeyword]),
          topKeywordsCell: weakKeyword,
          topKeywordVolumesCell: String(weakSerp.volume),
          averageKeywordVolume: weakSerp.volume,
          indicativeClusterVolume: weakSerp.volume,
          competitorUrlsCell: joinLines(weakRepresentative.urls),
          competitorCoverageCell: joinLines(weakRepresentative.coverage),
          clusterSize: 1,
          confidenceStatus: 'Review',
          weakMemberCount: 0,
          mainKeywordHistorical: serpByKeyword.get(weakKeyword)?.volumeHistorical ?? [],
        });
      }
    }
  }

  return rows;
}

// ---------------------------------------------------------------------------
// CSV exports
// ---------------------------------------------------------------------------

export function toContentPlanCsv(
  rows: ContentPlanRow[],
  exportOptions: CsvExportOptions = {},
): string {
  // Competitor Coverage, Cluster Size, and Weak Member Count are intentionally
  // omitted from the client-facing export — they are available in the
  // Diagnostics CSV for internal analysis.
  const header = [
    'ToDo SEO',
    'Client URL',
    'Cluster ID',
    'Intent / Main Keyword',
    'Main Keyword Volume',
    'Intent Type',
    'Top Keywords',
    'Top Keyword Volumes',
    'Average Keyword Volume',
    'Cluster Volume (Avg of Top Keywords)',
    'Competitor URLs',
  ];

  const lines = rows.map((row) =>
    [
      row.todoSeo,
      row.clientUrl,
      row.clusterId,
      row.intentMainKeyword,
      row.mainKeywordVolume,
      row.intentType,
      row.topKeywordsCell,
      row.topKeywordVolumesCell,
      row.averageKeywordVolume,
      row.indicativeClusterVolume,
      row.competitorUrlsCell,
    ]
      .map(escapeCsvCell)
      .join(','),
  );

  const csv = [header.map(escapeCsvCell).join(','), ...lines].join('\n');
  return exportOptions.addBom ? `\uFEFF${csv}` : csv;
}

export function toClusterDiagnosticsCsv(
  rows: ClusterMemberDiagnostic[],
  exportOptions: CsvExportOptions = {},
): string {
  const header = [
    'Cluster ID',
    'Keyword',
    'Volume',
    'Main Keyword',
    'Overlap Count With Main Keyword',
    'Similarity Ratio With Main Keyword',
    'Weighted Similarity With Main Keyword',
    'Status',
  ];

  const lines = rows.map((row) =>
    [
      row.clusterId,
      row.keyword,
      row.volume,
      row.mainKeyword,
      row.overlapCountWithMain,
      row.similarityRatioWithMain,
      row.weightedSimilarityWithMain,
      row.status,
    ]
      .map(escapeCsvCell)
      .join(','),
  );

  const csv = [header.map(escapeCsvCell).join(','), ...lines].join('\n');
  return exportOptions.addBom ? `\uFEFF${csv}` : csv;
}

// ---------------------------------------------------------------------------
// Top-level pipeline runner
// ---------------------------------------------------------------------------

export function runSerpClusteringPipeline(
  csvText: string,
  options: BuildOptions = {},
  exportOptions: CsvExportOptions = {},
) {
  const parsed = parseCsv(csvText);
  const built = buildKeywordSerps(parsed.rows, options);
  const similarities = computePairSimilarities(built.serps);
  const clusters = buildClusters(built.serps, similarities, options);
  const contentPlan = buildContentPlanRows(clusters, built.serps, options);
  const clusterDiagnostics = buildClusterMemberDiagnostics(
    clusters,
    built.serps,
    similarities,
    options,
  );

  return {
    issues: [...parsed.issues, ...built.issues],
    serps: built.serps,
    similarities,
    clusters,
    contentPlan,
    clusterDiagnostics,
    contentPlanCsv: toContentPlanCsv(contentPlan, exportOptions),
    clusterDiagnosticsCsv: toClusterDiagnosticsCsv(clusterDiagnostics, exportOptions),
  };
}

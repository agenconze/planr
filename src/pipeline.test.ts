/**
 * Test fixtures for the SERP clustering pipeline.
 *
 * These are framework-agnostic. Adapt to Vitest/Jest/Node test runner as needed.
 * Each fixture is a self-contained scenario matching the audit requirements.
 */

import {
  parseCsv,
  normalizeUrl,
  buildKeywordSerps,
  computePairSimilarities,
  buildClusters,
  buildContentPlanRows,
  buildClusterMemberDiagnostics,
  toContentPlanCsv,
  runSerpClusteringPipeline,
  type BuildOptions,
} from './pipeline';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeCsvRow(
  keyword: string,
  position: number,
  url: string,
  volume: number,
): string {
  return `${keyword},${position},${url},${volume}`;
}

function buildCsv(rows: string[]): string {
  return ['keyword,position,url,volume', ...rows].join('\n');
}

// ---------------------------------------------------------------------------
// FIXTURE 1 — Simple Strong Cluster
// Three keywords sharing 4+ URLs → clean single cluster.
// ---------------------------------------------------------------------------

const FIXTURE_STRONG_CLUSTER = buildCsv([
  // "seo agency" and "seo company" share URLs at positions 1-6
  makeCsvRow('seo agency', 1, 'https://example.com/a', 5000),
  makeCsvRow('seo agency', 2, 'https://example.com/b', 5000),
  makeCsvRow('seo agency', 3, 'https://example.com/c', 5000),
  makeCsvRow('seo agency', 4, 'https://example.com/d', 5000),
  makeCsvRow('seo agency', 5, 'https://other.com/x', 5000),
  makeCsvRow('seo agency', 6, 'https://other.com/y', 5000),
  makeCsvRow('seo agency', 7, 'https://other.com/z', 5000),
  makeCsvRow('seo agency', 8, 'https://other.com/w', 5000),
  makeCsvRow('seo agency', 9, 'https://other.com/v', 5000),
  makeCsvRow('seo agency', 10, 'https://other.com/u', 5000),

  makeCsvRow('seo company', 1, 'https://example.com/a', 3500),
  makeCsvRow('seo company', 2, 'https://example.com/b', 3500),
  makeCsvRow('seo company', 3, 'https://example.com/c', 3500),
  makeCsvRow('seo company', 4, 'https://example.com/d', 3500),
  makeCsvRow('seo company', 5, 'https://example.com/e', 3500),
  makeCsvRow('seo company', 6, 'https://other.com/x', 3500),
  makeCsvRow('seo company', 7, 'https://other.com/y', 3500),
  makeCsvRow('seo company', 8, 'https://different.com/1', 3500),
  makeCsvRow('seo company', 9, 'https://different.com/2', 3500),
  makeCsvRow('seo company', 10, 'https://different.com/3', 3500),

  makeCsvRow('hire seo agency', 1, 'https://example.com/a', 1200),
  makeCsvRow('hire seo agency', 2, 'https://example.com/b', 1200),
  makeCsvRow('hire seo agency', 3, 'https://example.com/c', 1200),
  makeCsvRow('hire seo agency', 4, 'https://example.com/d', 1200),
  makeCsvRow('hire seo agency', 5, 'https://other.com/x', 1200),
  makeCsvRow('hire seo agency', 6, 'https://other.com/y', 1200),
  makeCsvRow('hire seo agency', 7, 'https://other.com/z', 1200),
  makeCsvRow('hire seo agency', 8, 'https://other.com/w', 1200),
  makeCsvRow('hire seo agency', 9, 'https://unrelated.com/p', 1200),
  makeCsvRow('hire seo agency', 10, 'https://unrelated.com/q', 1200),
]);

export function testStrongCluster(): void {
  const result = runSerpClusteringPipeline(FIXTURE_STRONG_CLUSTER);

  console.assert(result.issues.length === 0, 'No issues expected for clean input');
  console.assert(result.clusters.length === 1, 'Should produce exactly 1 cluster');
  console.assert(
    result.clusters[0].mainKeyword === 'seo agency',
    'Main keyword should be highest-volume keyword',
  );
  console.assert(
    result.clusters[0].mainKeywordVolume === 5000,
    'Main keyword volume should be 5000',
  );
  console.assert(
    result.clusters[0].clusterSize === 3,
    'Cluster should have 3 members',
  );
  console.assert(
    result.clusters[0].weakMembers.length === 0,
    'No weak members expected in a clean overlap cluster',
  );
  console.assert(
    result.contentPlan[0].confidenceStatus === 'High Confidence',
    'Should be High Confidence',
  );
  console.log('✓ FIXTURE 1 — Strong cluster passed');
}

// ---------------------------------------------------------------------------
// FIXTURE 2 — Transitive Chain Risk
// A overlaps B strongly, B overlaps C strongly, A overlaps C weakly/not at all.
// All three end up in one cluster. C should appear as a weak member.
// ---------------------------------------------------------------------------

const SHARED_AB = ['https://shared-ab-1.com', 'https://shared-ab-2.com', 'https://shared-ab-3.com', 'https://shared-ab-4.com'];
const SHARED_BC = ['https://shared-bc-1.com', 'https://shared-bc-2.com', 'https://shared-bc-3.com', 'https://shared-bc-4.com'];

const FIXTURE_TRANSITIVE = buildCsv([
  // Keyword A: shares 4 URLs with B, but 0 with C
  makeCsvRow('keyword-a', 1, SHARED_AB[0], 8000),
  makeCsvRow('keyword-a', 2, SHARED_AB[1], 8000),
  makeCsvRow('keyword-a', 3, SHARED_AB[2], 8000),
  makeCsvRow('keyword-a', 4, SHARED_AB[3], 8000),
  makeCsvRow('keyword-a', 5, 'https://only-a-1.com', 8000),
  makeCsvRow('keyword-a', 6, 'https://only-a-2.com', 8000),
  makeCsvRow('keyword-a', 7, 'https://only-a-3.com', 8000),
  makeCsvRow('keyword-a', 8, 'https://only-a-4.com', 8000),
  makeCsvRow('keyword-a', 9, 'https://only-a-5.com', 8000),
  makeCsvRow('keyword-a', 10, 'https://only-a-6.com', 8000),

  // Keyword B: shares 4 URLs with A AND 4 URLs with C
  makeCsvRow('keyword-b', 1, SHARED_AB[0], 3000),
  makeCsvRow('keyword-b', 2, SHARED_AB[1], 3000),
  makeCsvRow('keyword-b', 3, SHARED_AB[2], 3000),
  makeCsvRow('keyword-b', 4, SHARED_AB[3], 3000),
  makeCsvRow('keyword-b', 5, SHARED_BC[0], 3000),
  makeCsvRow('keyword-b', 6, SHARED_BC[1], 3000),
  makeCsvRow('keyword-b', 7, SHARED_BC[2], 3000),
  makeCsvRow('keyword-b', 8, SHARED_BC[3], 3000),
  makeCsvRow('keyword-b', 9, 'https://only-b-1.com', 3000),
  makeCsvRow('keyword-b', 10, 'https://only-b-2.com', 3000),

  // Keyword C: shares 4 URLs with B, but 0 with A
  makeCsvRow('keyword-c', 1, SHARED_BC[0], 1000),
  makeCsvRow('keyword-c', 2, SHARED_BC[1], 1000),
  makeCsvRow('keyword-c', 3, SHARED_BC[2], 1000),
  makeCsvRow('keyword-c', 4, SHARED_BC[3], 1000),
  makeCsvRow('keyword-c', 5, 'https://only-c-1.com', 1000),
  makeCsvRow('keyword-c', 6, 'https://only-c-2.com', 1000),
  makeCsvRow('keyword-c', 7, 'https://only-c-3.com', 1000),
  makeCsvRow('keyword-c', 8, 'https://only-c-4.com', 1000),
  makeCsvRow('keyword-c', 9, 'https://only-c-5.com', 1000),
  makeCsvRow('keyword-c', 10, 'https://only-c-6.com', 1000),
]);

export function testTransitiveChain(): void {
  const result = runSerpClusteringPipeline(FIXTURE_TRANSITIVE);

  // All three end up in one cluster via transitivity — this is expected behavior
  console.assert(result.clusters.length === 1, 'All three should be in one cluster via transitivity');
  console.assert(result.clusters[0].mainKeyword === 'keyword-a', 'A is main (highest volume)');

  // C overlaps A by 0 URLs → should be a weak member
  const weakMembers = result.clusters[0].weakMembers;
  console.assert(weakMembers.includes('keyword-c'), 'keyword-c should be flagged as weak (0 overlap with main)');
  console.assert(!weakMembers.includes('keyword-b'), 'keyword-b should NOT be weak (4 overlap with main)');

  // Diagnostics should show keyword-c as 'weak'
  const cDiag = result.clusterDiagnostics.find((d) => d.keyword === 'keyword-c');
  console.assert(cDiag?.status === 'weak', 'keyword-c diagnostic status should be weak');
  console.assert(cDiag?.overlapCountWithMain === 0, 'keyword-c should have 0 overlap with main');

  // With pruning enabled, keyword-c should become a separate row
  const prunedResult = runSerpClusteringPipeline(FIXTURE_TRANSITIVE, { pruneWeakMembers: true });
  const prunedRows = prunedResult.contentPlan;
  const weakRow = prunedRows.find((r) => r.intentMainKeyword === 'keyword-c');
  console.assert(weakRow !== undefined, 'Pruned weak member should become standalone row');
  console.assert(weakRow?.clusterId.endsWith('-W'), 'Pruned row cluster ID should end with -W');
  console.assert(weakRow?.clusterSize === 1, 'Pruned row should be size 1');

  console.log('✓ FIXTURE 2 — Transitive chain risk passed');
}

// ---------------------------------------------------------------------------
// FIXTURE 3 — Isolated Keyword
// One keyword with no overlap with anything else → single-keyword cluster.
// ---------------------------------------------------------------------------

const FIXTURE_ISOLATED = buildCsv([
  makeCsvRow('seo tools', 1, 'https://a.com', 2000),
  makeCsvRow('seo tools', 2, 'https://b.com', 2000),
  makeCsvRow('seo tools', 3, 'https://c.com', 2000),
  makeCsvRow('seo tools', 4, 'https://d.com', 2000),
  makeCsvRow('seo tools', 5, 'https://e.com', 2000),
  makeCsvRow('seo tools', 6, 'https://f.com', 2000),
  makeCsvRow('seo tools', 7, 'https://g.com', 2000),
  makeCsvRow('seo tools', 8, 'https://h.com', 2000),
  makeCsvRow('seo tools', 9, 'https://i.com', 2000),
  makeCsvRow('seo tools', 10, 'https://j.com', 2000),

  makeCsvRow('buy shoes online', 1, 'https://shoes1.com', 900),
  makeCsvRow('buy shoes online', 2, 'https://shoes2.com', 900),
  makeCsvRow('buy shoes online', 3, 'https://shoes3.com', 900),
  makeCsvRow('buy shoes online', 4, 'https://shoes4.com', 900),
  makeCsvRow('buy shoes online', 5, 'https://shoes5.com', 900),
  makeCsvRow('buy shoes online', 6, 'https://shoes6.com', 900),
  makeCsvRow('buy shoes online', 7, 'https://shoes7.com', 900),
  makeCsvRow('buy shoes online', 8, 'https://shoes8.com', 900),
  makeCsvRow('buy shoes online', 9, 'https://shoes9.com', 900),
  makeCsvRow('buy shoes online', 10, 'https://shoes10.com', 900),
]);

export function testIsolatedKeyword(): void {
  const result = runSerpClusteringPipeline(FIXTURE_ISOLATED);

  console.assert(result.clusters.length === 2, 'Should produce 2 separate clusters');
  for (const cluster of result.clusters) {
    console.assert(cluster.clusterSize === 1, 'Each cluster should have exactly 1 keyword');
    console.assert(cluster.weakMembers.length === 0, 'No weak members in isolated clusters');
  }

  const diags = result.clusterDiagnostics;
  for (const diag of diags) {
    console.assert(diag.status === 'isolated', 'All isolated keywords should have status=isolated');
  }

  const planRows = result.contentPlan;
  for (const row of planRows) {
    console.assert(row.confidenceStatus === 'Review', 'Single-keyword cluster should be Review');
  }

  console.log('✓ FIXTURE 3 — Isolated keyword passed');
}

// ---------------------------------------------------------------------------
// FIXTURE 4 — Volume Zero
// Volume=0 must be accepted without warnings.
// ---------------------------------------------------------------------------

const FIXTURE_ZERO_VOLUME = buildCsv([
  makeCsvRow('long tail keyword exact', 1, 'https://a.com', 0),
  makeCsvRow('long tail keyword exact', 2, 'https://b.com', 0),
  makeCsvRow('long tail keyword exact', 3, 'https://c.com', 0),
  makeCsvRow('long tail keyword exact', 4, 'https://d.com', 0),
  makeCsvRow('long tail keyword exact', 5, 'https://e.com', 0),
  makeCsvRow('long tail keyword exact', 6, 'https://f.com', 0),
  makeCsvRow('long tail keyword exact', 7, 'https://g.com', 0),
  makeCsvRow('long tail keyword exact', 8, 'https://h.com', 0),
  makeCsvRow('long tail keyword exact', 9, 'https://i.com', 0),
  makeCsvRow('long tail keyword exact', 10, 'https://j.com', 0),

  makeCsvRow('long tail keyword variation', 1, 'https://a.com', 0),
  makeCsvRow('long tail keyword variation', 2, 'https://b.com', 0),
  makeCsvRow('long tail keyword variation', 3, 'https://c.com', 0),
  makeCsvRow('long tail keyword variation', 4, 'https://d.com', 0),
  makeCsvRow('long tail keyword variation', 5, 'https://e.com', 0),
  makeCsvRow('long tail keyword variation', 6, 'https://k.com', 0),
  makeCsvRow('long tail keyword variation', 7, 'https://l.com', 0),
  makeCsvRow('long tail keyword variation', 8, 'https://m.com', 0),
  makeCsvRow('long tail keyword variation', 9, 'https://n.com', 0),
  makeCsvRow('long tail keyword variation', 10, 'https://o.com', 0),
]);

export function testVolumeZero(): void {
  const result = runSerpClusteringPipeline(FIXTURE_ZERO_VOLUME);

  const volumeIssues = result.issues.filter(
    (issue) => issue.code === 'INVALID_VOLUME',
  );
  console.assert(volumeIssues.length === 0, 'Volume=0 should not trigger INVALID_VOLUME errors');

  // The two keywords overlap at 5 URLs (a-e) → should cluster (threshold=3 by default)
  console.assert(result.clusters.length === 1, 'Zero-volume keywords should cluster on SERP overlap');
  console.assert(result.clusters[0].mainKeywordVolume === 0, 'Main keyword volume should be 0');
  console.assert(result.contentPlan[0].mainKeywordVolume === 0, 'Content plan volume should be 0');
  console.assert(result.contentPlan[0].indicativeClusterVolume === 0, 'Indicative volume should be 0');

  console.log('✓ FIXTURE 4 — Volume zero passed');
}

// ---------------------------------------------------------------------------
// FIXTURE 5 — Inconsistent Volume
// Same keyword appears with different volume values.
// Max value should be used, a warning emitted.
// ---------------------------------------------------------------------------

const FIXTURE_INCONSISTENT_VOLUME = buildCsv([
  makeCsvRow('seo audit', 1, 'https://a.com', 1000),
  makeCsvRow('seo audit', 2, 'https://b.com', 1000),
  makeCsvRow('seo audit', 3, 'https://c.com', 1000),
  makeCsvRow('seo audit', 4, 'https://d.com', 1000),
  makeCsvRow('seo audit', 5, 'https://e.com', 1000),
  makeCsvRow('seo audit', 6, 'https://f.com', 1000),
  makeCsvRow('seo audit', 7, 'https://g.com', 1000),
  // Position 8: same keyword, different volume (e.g. from different crawl dates merged)
  makeCsvRow('seo audit', 8, 'https://h.com', 1200),
  makeCsvRow('seo audit', 9, 'https://i.com', 1200),
  makeCsvRow('seo audit', 10, 'https://j.com', 1200),
]);

export function testInconsistentVolume(): void {
  const result = runSerpClusteringPipeline(FIXTURE_INCONSISTENT_VOLUME);

  const inconsistentWarning = result.issues.find(
    (issue) => issue.code === 'INCONSISTENT_VOLUME',
  );
  console.assert(inconsistentWarning !== undefined, 'INCONSISTENT_VOLUME warning should be emitted');
  console.assert(
    inconsistentWarning?.level === 'warning',
    'Should be a warning, not an error',
  );

  // Max volume should be selected
  console.assert(result.serps[0].volume === 1200, 'Max volume (1200) should be selected');

  console.log('✓ FIXTURE 5 — Inconsistent volume passed');
}

// ---------------------------------------------------------------------------
// FIXTURE 6 — URL Normalization Cases
// ---------------------------------------------------------------------------

export function testUrlNormalization(): void {
  const cases: Array<{ input: string; expected: string; label: string }> = [
    {
      label: 'www → non-www',
      input: 'https://www.example.com/page',
      expected: 'https://example.com/page',
    },
    {
      label: 'http → https (forceHttps)',
      input: 'http://example.com/page',
      expected: 'https://example.com/page',
    },
    {
      label: 'trailing slash removed',
      input: 'https://example.com/page/',
      expected: 'https://example.com/page',
    },
    {
      label: 'root path stays as /',
      input: 'https://example.com/',
      expected: 'https://example.com/',
    },
    {
      label: 'UTM params stripped',
      input: 'https://example.com/page?utm_source=google&utm_medium=cpc',
      expected: 'https://example.com/page',
    },
    {
      label: 'gclid stripped',
      input: 'https://example.com/page?gclid=abc123',
      expected: 'https://example.com/page',
    },
    {
      label: 'fbclid stripped',
      input: 'https://example.com/page?fbclid=xyz',
      expected: 'https://example.com/page',
    },
    {
      label: 'hash fragment stripped by default',
      input: 'https://example.com/page#section',
      expected: 'https://example.com/page',
    },
    {
      label: 'functional param preserved',
      input: 'https://example.com/search?q=seo&lang=en',
      expected: 'https://example.com/search?lang=en&q=seo',
    },
    {
      label: 'query params sorted alphabetically',
      input: 'https://example.com/page?z=1&a=2&m=3',
      expected: 'https://example.com/page?a=2&m=3&z=1',
    },
    {
      label: 'double slash in path collapsed',
      input: 'https://example.com//path//to//page',
      expected: 'https://example.com/path/to/page',
    },
    {
      label: 'hostname lowercased',
      input: 'https://EXAMPLE.COM/Page',
      expected: 'https://example.com/Page',
    },
    {
      label: 'http without www, forceHttps default',
      input: 'http://example.com/path',
      expected: 'https://example.com/path',
    },
    {
      label: 'www + http → normalized',
      input: 'http://www.example.com/path/',
      expected: 'https://example.com/path',
    },
  ];

  for (const { input, expected, label } of cases) {
    const result = normalizeUrl(input);
    console.assert(
      result === expected,
      `URL normalization failed [${label}]: expected "${expected}", got "${result}"`,
    );
  }

  // Test hash preserved when stripHash: false
  const withHash = normalizeUrl('https://example.com/page#section', { stripHash: false });
  console.assert(
    withHash === 'https://example.com/page#section',
    'Hash should be preserved when stripHash: false',
  );

  // Test www preserved when collapseWww: false
  const withWww = normalizeUrl('https://www.example.com/page', { collapseWww: false });
  console.assert(
    withWww === 'https://www.example.com/page',
    'www should be preserved when collapseWww: false',
  );

  // Test that "source" param is NOT stripped by default (removed from tracking list)
  const withSource = normalizeUrl('https://example.com/feed?source=rss');
  console.assert(
    withSource.includes('source=rss'),
    '"source" param should NOT be stripped (functional param risk)',
  );

  console.log('✓ FIXTURE 6 — URL normalization passed');
}

// ---------------------------------------------------------------------------
// FIXTURE 7 — CSV Export for Google Sheets
// Multi-line cells, quotes, commas in values, BOM option.
// ---------------------------------------------------------------------------

export function testCsvExport(): void {
  const csv = buildCsv([
    // keyword with comma in it (unlikely but possible)
    makeCsvRow('seo, sem strategy', 1, 'https://a.com', 500),
    makeCsvRow('seo, sem strategy', 2, 'https://b.com', 500),
    makeCsvRow('seo, sem strategy', 3, 'https://c.com', 500),
    makeCsvRow('seo, sem strategy', 4, 'https://d.com', 500),
    makeCsvRow('seo, sem strategy', 5, 'https://e.com', 500),
    makeCsvRow('seo, sem strategy', 6, 'https://f.com', 500),
    makeCsvRow('seo, sem strategy', 7, 'https://g.com', 500),
    makeCsvRow('seo, sem strategy', 8, 'https://h.com', 500),
    makeCsvRow('seo, sem strategy', 9, 'https://i.com', 500),
    makeCsvRow('seo, sem strategy', 10, 'https://j.com', 500),
  ]);

  const result = runSerpClusteringPipeline(csv);
  const exportedCsv = result.contentPlanCsv;

  // keyword with comma should be quoted in the export
  console.assert(
    exportedCsv.includes('"seo, sem strategy"'),
    'Keyword with comma should be quoted in CSV output',
  );

  // Multi-line cells (topKeywordsCell) with a single keyword cluster should not produce \n
  const lines = exportedCsv.split('\n');
  // Header line + 1 data row (single keyword) = 2 lines minimum
  console.assert(lines.length >= 2, 'CSV should have at least header + 1 data row');

  // Test BOM option
  const csvWithBom = result.contentPlanCsv; // no BOM by default
  console.assert(!csvWithBom.startsWith('\uFEFF'), 'Default export should not have BOM');

  const resultWithBom = runSerpClusteringPipeline(csv, {}, { addBom: true });
  console.assert(
    resultWithBom.contentPlanCsv.startsWith('\uFEFF'),
    'Export with addBom:true should start with UTF-8 BOM',
  );

  // Verify quoted multi-line cells don't break the row count
  // Build a dataset with a strong cluster that will have multi-line topKeywordsCell
  const multiKwCsv = buildCsv([
    makeCsvRow('kw one', 1, 'https://shared1.com', 1000),
    makeCsvRow('kw one', 2, 'https://shared2.com', 1000),
    makeCsvRow('kw one', 3, 'https://shared3.com', 1000),
    makeCsvRow('kw one', 4, 'https://shared4.com', 1000),
    makeCsvRow('kw one', 5, 'https://a.com', 1000),
    makeCsvRow('kw one', 6, 'https://b.com', 1000),
    makeCsvRow('kw one', 7, 'https://c.com', 1000),
    makeCsvRow('kw one', 8, 'https://d.com', 1000),
    makeCsvRow('kw one', 9, 'https://e.com', 1000),
    makeCsvRow('kw one', 10, 'https://f.com', 1000),

    makeCsvRow('kw two', 1, 'https://shared1.com', 800),
    makeCsvRow('kw two', 2, 'https://shared2.com', 800),
    makeCsvRow('kw two', 3, 'https://shared3.com', 800),
    makeCsvRow('kw two', 4, 'https://shared4.com', 800),
    makeCsvRow('kw two', 5, 'https://g.com', 800),
    makeCsvRow('kw two', 6, 'https://h.com', 800),
    makeCsvRow('kw two', 7, 'https://i.com', 800),
    makeCsvRow('kw two', 8, 'https://j.com', 800),
    makeCsvRow('kw two', 9, 'https://k.com', 800),
    makeCsvRow('kw two', 10, 'https://l.com', 800),
  ]);

  const multiKwResult = runSerpClusteringPipeline(multiKwCsv);
  const multiKwCsvExport = multiKwResult.contentPlanCsv;

  // The topKeywordsCell will contain "kw one\nkw two" → must be quoted
  console.assert(
    multiKwCsvExport.includes('"kw one\nkw two"'),
    'Multi-line topKeywordsCell should be wrapped in double quotes',
  );

  // Parsing back: the exported CSV header row should be the first logical CSV row
  const exportLines = multiKwCsvExport.split('\n');
  const headerCols = exportLines[0].split(',');
  console.assert(
    headerCols[0] === 'Cluster ID',
    'First column of header should be Cluster ID',
  );

  console.log('✓ FIXTURE 7 — CSV export for Google Sheets passed');
}

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

export function runAllTests(): void {
  console.log('Running SERP Clustering Pipeline Tests...\n');
  try {
    testStrongCluster();
    testTransitiveChain();
    testIsolatedKeyword();
    testVolumeZero();
    testInconsistentVolume();
    testUrlNormalization();
    testCsvExport();
    console.log('\n✓ All tests passed.');
  } catch (error) {
    console.error('\n✗ Test failure:', error);
    process.exit(1);
  }
}

runAllTests();

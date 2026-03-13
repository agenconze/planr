import { useState, useRef, useCallback, type ChangeEvent, type DragEvent } from 'react';
import {
  runSerpClusteringPipeline,
  type BuildOptions,
  type ContentPlanRow,
} from './pipeline';

type PipelineResult = ReturnType<typeof runSerpClusteringPipeline>;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function downloadCsv(content: string, filename: string) {
  const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function slug() {
  return new Date().toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Badge
// ---------------------------------------------------------------------------

const BADGE_CLASSES: Record<string, string> = {
  'High Confidence': 'badge-green',
  Review:           'badge-yellow',
  'Low Cohesion':   'badge-red',
  OPTIMISATION:     'badge-optimisation',
  CREATION:         'badge-creation',
  BLOG:             'badge-blog',
  PRODUCT:          'badge-product',
  MIXED:            'badge-mixed',
};

function Badge({ label }: { label: string }) {
  return (
    <span className={`badge ${BADGE_CLASSES[label] ?? 'badge-gray'}`}>
      {label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Logo
// ---------------------------------------------------------------------------

function Logo() {
  return (
    <div className="logo-group">
      <div className="logo-icon-wrap" aria-hidden="true">
        {/* Planner grid icon */}
        <svg width="28" height="28" viewBox="0 0 28 28" fill="none">
          <rect width="28" height="28" rx="7" fill="url(#planr-grad)" />
          {/* 2×2 grid cells */}
          <rect x="7" y="7" width="5" height="5" rx="1.2" fill="rgba(255,255,255,0.55)" />
          <rect x="16" y="7" width="5" height="5" rx="1.2" fill="rgba(255,255,255,0.9)" />
          <rect x="7" y="16" width="5" height="5" rx="1.2" fill="rgba(255,255,255,0.9)" />
          <rect x="16" y="16" width="5" height="5" rx="1.2" fill="rgba(255,255,255,0.55)" />
          <defs>
            <linearGradient id="planr-grad" x1="0" y1="0" x2="28" y2="28" gradientUnits="userSpaceOnUse">
              <stop stopColor="#7B61FF" />
              <stop offset="1" stopColor="#FF4FD8" />
            </linearGradient>
          </defs>
        </svg>
        {/* Orbiting dot */}
        <div className="logo-orbit">
          <div className="logo-orbit-dot" />
        </div>
      </div>
      <div>
        <div className="logo-name">Planr</div>
        <div className="logo-tagline">Content Plan Automation</div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Content Plan Table
// ---------------------------------------------------------------------------

function multilineToJsx(cell: string) {
  const lines = cell.split('\n');
  return lines.map((line, i) => (
    <span key={i} className="cell-line">{line}</span>
  ));
}

function ContentPlanTable({ rows }: { rows: ContentPlanRow[] }) {
  if (rows.length === 0) return <p className="empty">No content plan rows.</p>;
  return (
    <div className="table-scroll">
      <table>
        <thead>
          <tr>
            <th>ToDo SEO</th>
            <th>Client URL</th>
            <th>Main Keyword</th>
            <th className="num">Volume</th>
            <th>Intent Type</th>
            <th>Top Keywords</th>
            <th>Top Volumes</th>
            <th className="num">Avg Volume</th>
            <th>Competitor URLs</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.clusterId}>
              <td className="cell-badge">
                {row.todoSeo
                  ? <Badge label={row.todoSeo} />
                  : <span className="muted">—</span>}
              </td>
              <td className="cell-url">{row.clientUrl || <span className="muted">—</span>}</td>
              <td className="cell-main">{row.intentMainKeyword}</td>
              <td className="num">{row.mainKeywordVolume.toLocaleString()}</td>
              <td className="cell-badge">
                {row.intentType !== '—'
                  ? <Badge label={row.intentType} />
                  : <span className="muted">—</span>}
              </td>
              <td className="cell-multi">{multilineToJsx(row.topKeywordsCell)}</td>
              <td className="cell-multi num">{multilineToJsx(row.topKeywordVolumesCell)}</td>
              <td className="num">{row.indicativeClusterVolume.toLocaleString()}</td>
              <td className="cell-multi cell-url">{multilineToJsx(row.competitorUrlsCell)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// SparkLine — compact SVG trend line
// ---------------------------------------------------------------------------

function SparkLine({ values, color }: { values: number[]; color: string }) {
  if (values.length < 2) return null;
  const W = 108, H = 32, pad = 3;
  const min = Math.min(...values);
  const max = Math.max(...values, min + 1);
  const range = max - min;
  const pts = values
    .map((v, i) => {
      const x = pad + (i / (values.length - 1)) * (W - pad * 2);
      const y = H - pad - ((v - min) / range) * (H - pad * 2);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(' ');
  return (
    <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} className="trend-svg">
      <polyline
        points={pts}
        fill="none"
        stroke={color}
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Insights Section
// ---------------------------------------------------------------------------

function InsightsSection({ rows }: { rows: ContentPlanRow[] }) {
  const [trendIdx, setTrendIdx] = useState(0);
  if (rows.length === 0) return null;

  // Card 1 — Top Search Opportunity
  const topOpportunity = [...rows].sort(
    (a, b) => b.mainKeywordVolume - a.mainKeywordVolume,
  )[0];

  // Card 2 — Search Intent Mix (BLOG / PRODUCT / MIXED only, "—" excluded)
  const intentVolumeMap: Record<string, number> = { BLOG: 0, PRODUCT: 0, MIXED: 0 };
  for (const row of rows) {
    if (row.intentType !== '—') {
      intentVolumeMap[row.intentType] = (intentVolumeMap[row.intentType] ?? 0) + row.indicativeClusterVolume;
    }
  }
  const sortedIntents = (['BLOG', 'PRODUCT', 'MIXED'] as const)
    .map((type) => [type, intentVolumeMap[type]] as [string, number])
    .filter(([, vol]) => vol > 0)
    .sort(([, a], [, b]) => b - a);
  const maxIntentVol = sortedIntents[0]?.[1] ?? 1;

  // Card 3 — SEO Workload Distribution (count-based)
  let creationCount = 0;
  let optimisationCount = 0;
  for (const row of rows) {
    if (row.todoSeo === 'CREATION') creationCount++;
    else if (row.todoSeo === 'OPTIMISATION') optimisationCount++;
  }
  const splitTotal = creationCount + optimisationCount;
  const creationPct = splitTotal > 0 ? (creationCount / splitTotal) * 100 : 0;
  const optimisationPct = 100 - creationPct;

  // Card 4 — Fastest Growing Keyword (first month → last month of historical data)
  type KeywordTrend = {
    keyword: string;
    intentType: string;
    growthPct: number;
    firstMonth: string;
    lastMonth: string;
    sparkline: number[];
  };
  const keywordTrends: KeywordTrend[] = [];
  for (const row of rows) {
    const hist = row.mainKeywordHistorical;
    if (hist.length < 2) continue;
    // Use a rolling window average (up to 3 months) to prevent a single
    // low outlier at the start from producing a misleading positive %.
    const win = Math.min(3, Math.floor(hist.length / 2));
    const firstAvg = hist.slice(0, win).reduce((s, h) => s + h.volume, 0) / win;
    const lastAvg  = hist.slice(-win).reduce((s, h) => s + h.volume, 0) / win;
    if (firstAvg === 0) continue;
    const growthPct = ((lastAvg - firstAvg) / firstAvg) * 100;
    keywordTrends.push({
      keyword: row.intentMainKeyword,
      intentType: row.intentType,
      growthPct,
      firstMonth: hist[0].month,
      lastMonth: hist[hist.length - 1].month,
      sparkline: hist.map((h) => h.volume),
    });
  }
  keywordTrends.sort((a, b) => b.growthPct - a.growthPct);
  const top5Trends = keywordTrends.slice(0, 5);
  const currentTrend = top5Trends[Math.min(trendIdx, Math.max(0, top5Trends.length - 1))];

  const INTENT_COLORS: Record<string, string> = {
    BLOG:    'var(--green)',
    PRODUCT: 'var(--purple)',
    MIXED:   'var(--pink)',
  };

  // "2025-02" → "Feb 2025"
  const formatMonth = (m: string) => {
    const [year, month] = m.split('-');
    return new Date(Number(year), Number(month) - 1, 1)
      .toLocaleDateString('en-GB', { month: 'short', year: 'numeric' });
  };

  return (
    <div className="insights-row">
      {/* Card 1 — Top Search Opportunity */}
      <div className="insight-card">
        <div className="insight-label">Top Search Opportunity</div>
        <div className="insight-value">{topOpportunity.intentMainKeyword}</div>
        <div className="insight-sub">
          {topOpportunity.mainKeywordVolume.toLocaleString()} searches / mo
          {topOpportunity.intentType !== '—' && (
            <span className={`badge ${BADGE_CLASSES[topOpportunity.intentType] ?? 'badge-gray'}`} style={{ marginLeft: 8 }}>
              {topOpportunity.intentType}
            </span>
          )}
        </div>
      </div>

      {/* Card 2 — Search Intent Mix */}
      <div className="insight-card">
        <div className="insight-label">Search Intent Mix</div>
        {sortedIntents.length > 0 ? (
          <div className="bar-chart">
            {sortedIntents.map(([type, vol]) => (
              <div key={type} className="bar-row">
                <div className="bar-type">{type}</div>
                <div className="bar-track">
                  <div
                    className="bar-fill"
                    style={{
                      width: `${Math.round((vol / maxIntentVol) * 100)}%`,
                      background: INTENT_COLORS[type] ?? 'var(--accent)',
                    }}
                  />
                </div>
                <div className="bar-vol">{vol.toLocaleString()}</div>
              </div>
            ))}
          </div>
        ) : (
          <div className="insight-empty">No intent data available</div>
        )}
      </div>

      {/* Card 3 — SEO Workload Distribution */}
      <div className="insight-card">
        <div className="insight-label">SEO Workload Distribution</div>
        {splitTotal > 0 ? (
          <div className="donut-wrap">
            <div
              className="donut"
              style={{
                background: `conic-gradient(var(--blue) ${creationPct}%, var(--amber) 0%)`,
              }}
            >
              <div className="donut-hole" />
            </div>
            <div className="donut-legend">
              <div className="donut-legend-item">
                <span className="donut-dot" style={{ background: 'var(--blue)' }} />
                <span>Creation <strong>{creationCount}</strong> ({Math.round(creationPct)}%)</span>
              </div>
              <div className="donut-legend-item">
                <span className="donut-dot" style={{ background: 'var(--amber)' }} />
                <span>Optimisation <strong>{optimisationCount}</strong> ({Math.round(optimisationPct)}%)</span>
              </div>
            </div>
          </div>
        ) : (
          <div className="insight-empty">Set a Client Domain to enable</div>
        )}
      </div>

      {/* Card 4 — Fastest Growing Keyword */}
      <div className={`insight-card${top5Trends.length > 0 ? '' : ' insight-card--muted'}`}>
        <div className="insight-label">Fastest Growing Keyword</div>
        {top5Trends.length > 0 && currentTrend ? (
          <>
            <div className="insight-value" style={{ color: INTENT_COLORS[currentTrend.intentType] ?? 'var(--text)', fontSize: 13 }}>
              {currentTrend.keyword}
            </div>
            <div className="insight-sub">
              {currentTrend.intentType !== '—' && (
                <span className={`badge ${BADGE_CLASSES[currentTrend.intentType] ?? 'badge-gray'}`}>
                  {currentTrend.intentType}
                </span>
              )}
              <span>{currentTrend.growthPct >= 0 ? '+' : ''}{Math.round(currentTrend.growthPct)}%</span>
            </div>
            <div className="trend-months">
              <span>{formatMonth(currentTrend.firstMonth)}</span>
              <span>{formatMonth(currentTrend.lastMonth)}</span>
            </div>
            <SparkLine
              values={currentTrend.sparkline}
              color={INTENT_COLORS[currentTrend.intentType] ?? 'var(--accent)'}
            />
            {top5Trends.length > 1 && (
              <div className="trend-nav">
                <button
                  className="trend-nav-btn"
                  onClick={() => setTrendIdx((i) => Math.max(0, i - 1))}
                  disabled={trendIdx === 0}
                >&#8592;</button>
                <span className="trend-counter">{trendIdx + 1} / {top5Trends.length}</span>
                <button
                  className="trend-nav-btn"
                  onClick={() => setTrendIdx((i) => Math.min(top5Trends.length - 1, i + 1))}
                  disabled={trendIdx === top5Trends.length - 1}
                >&#8594;</button>
              </div>
            )}
          </>
        ) : (
          <>
            <div className="insight-empty">Historical data not available</div>
            <div className="insight-empty-hint">
              Upload a Monitorank export with Volume Historical data.
            </div>
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main App
// ---------------------------------------------------------------------------

const DEFAULT_OPTIONS = {
  overlapThreshold: 4,
  maxPosition: 10,
  weakMainOverlapThreshold: 2,
  weakMainSimilarityThreshold: 0.3,
  pruneWeakMembers: false,
  autoRefineWeakClusters: false,
  clientDomain: '',
} satisfies BuildOptions;

export default function App() {
  const [csvText, setCsvText]       = useState('');
  const [fileName, setFileName]     = useState('');
  const [isDragging, setIsDragging] = useState(false);
  const [options, setOptions]       = useState(DEFAULT_OPTIONS);
  const [result, setResult]         = useState<PipelineResult | null>(null);
  const [isRunning, setIsRunning]   = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // -- File loading ----------------------------------------------------------

  const loadFile = useCallback((file: File) => {
    setFileName(file.name);
    setResult(null);
    const reader = new FileReader();
    reader.onload = (e) => setCsvText((e.target?.result as string) ?? '');
    reader.readAsText(file, 'UTF-8');
  }, []);

  const handleFileChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) loadFile(file);
    },
    [loadFile],
  );

  const handleDrop = useCallback(
    (e: DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      setIsDragging(false);
      const file = e.dataTransfer.files?.[0];
      if (file) loadFile(file);
    },
    [loadFile],
  );

  // -- Run pipeline ----------------------------------------------------------

  const handleRun = () => {
    if (!csvText.trim()) return;
    setIsRunning(true);
    setTimeout(() => {
      try {
        const r = runSerpClusteringPipeline(csvText, options, { addBom: true });
        setResult(r);
      } catch (err) {
        alert(`Pipeline error: ${err instanceof Error ? err.message : String(err)}`);
      } finally {
        setIsRunning(false);
      }
    }, 0);
  };

  // ---- Render --------------------------------------------------------------

  return (
    <div className="app">
      {/* ── Header ─────────────────────────────────────────────────────── */}
      <header className="app-header">
        <Logo />
      </header>

      <main className="app-main">
        {/* ── Control bar ────────────────────────────────────────────────── */}
        <section className="control-bar">

          {/* Drop zone */}
          <div
            className={`drop-zone ${isDragging ? 'dragging' : ''} ${csvText ? 'loaded' : ''}`}
            onDrop={handleDrop}
            onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
            onDragLeave={() => setIsDragging(false)}
            onClick={() => fileInputRef.current?.click()}
          >
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,text/csv"
              onChange={handleFileChange}
              style={{ display: 'none' }}
            />
            {csvText ? (
              <div className="drop-content">
                <span className="drop-icon drop-icon--ok">✓</span>
                <div>
                  <div className="drop-filename">File Loaded</div>
                  <div className="drop-hint">Click to change</div>
                </div>
              </div>
            ) : (
              <div className="drop-content">
                <span className="drop-icon">↑</span>
                <div>
                  <div className="drop-label">Drop Monitorank CSV</div>
                  <div className="drop-hint">or click to browse</div>
                </div>
              </div>
            )}
          </div>

          {/* Client domain */}
          <div className="control-group">
            <label className="control-label" htmlFor="client-domain">Client Domain</label>
            <input
              id="client-domain"
              type="text"
              className="control-input"
              placeholder="e.g. client.com"
              value={options.clientDomain ?? ''}
              onChange={(e) => setOptions((o) => ({ ...o, clientDomain: e.target.value }))}
            />
            <span className="control-hint">
              Classifies clusters as <strong>OPTIMISATION</strong> or <strong>CREATION</strong>.
            </span>
          </div>

          {/* SERP Depth */}
          <div className="control-group">
            <span className="control-label">SERP Depth</span>
            <div className="serp-depth-options">
              <label className={`depth-option ${options.maxPosition === 10 ? 'active' : ''}`}>
                <input
                  type="radio"
                  name="serpDepth"
                  value="10"
                  checked={options.maxPosition === 10}
                  onChange={() => setOptions((o) => ({ ...o, maxPosition: 10 }))}
                />
                Top 10
              </label>
              <label className={`depth-option ${options.maxPosition === 5 ? 'active' : ''}`}>
                <input
                  type="radio"
                  name="serpDepth"
                  value="5"
                  checked={options.maxPosition === 5}
                  onChange={() => setOptions((o) => ({ ...o, maxPosition: 5 }))}
                />
                Top 5
              </label>
            </div>
            <span className="control-hint">
              Top 10 = broader comparison, more clusters. Top 5 = stricter, cleaner intent signal.
            </span>
          </div>

          {/* Overlap threshold */}
          <div className="control-group">
            <label className="control-label" htmlFor="overlap">
              Overlap Threshold
              <span className="threshold-badge">{options.overlapThreshold}</span>
            </label>
            <input
              id="overlap"
              type="range"
              min={1} max={10} step={1}
              value={options.overlapThreshold}
              onChange={(e) =>
                setOptions((o) => ({ ...o, overlapThreshold: Number(e.target.value) }))
              }
            />
            <span className="control-hint">
              {options.overlapThreshold <= 3
                ? 'Flexible — broader clusters, more groupings'
                : options.overlapThreshold === 4
                ? 'Balanced — cleaner intent, fewer mixed clusters'
                : 'Strict — tight clusters, more isolated keywords'}
            </span>
          </div>

          {/* Run button */}
          <button
            className="btn-run"
            onClick={handleRun}
            disabled={!csvText || isRunning}
          >
            {isRunning ? (
              <><span className="spinner" />Processing…</>
            ) : (
              'Run Clustering'
            )}
          </button>
        </section>

        {/* ── Results ────────────────────────────────────────────────────── */}
        {result && (
          <section className="results-panel">
            {/* Stats + export bar */}
            <div className="results-header">
              <div className="stats-row">
                <div className="stat">
                  <span className="stat-value">{result.serps.length.toLocaleString()}</span>
                  <span className="stat-label">keywords</span>
                </div>
                <div className="stat-sep" />
                <div className="stat">
                  <span className="stat-value">{result.clusters.length.toLocaleString()}</span>
                  <span className="stat-label">clusters</span>
                </div>
                <div className="stat-sep" />
                <div className="stat">
                  <span className="stat-value">
                    {result.contentPlan.filter((r) => r.todoSeo === 'OPTIMISATION').length}
                  </span>
                  <span className="stat-label">optimisation</span>
                </div>
                <div className="stat-sep" />
                <div className="stat">
                  <span className="stat-value">
                    {result.contentPlan.filter((r) => r.todoSeo === 'CREATION').length}
                  </span>
                  <span className="stat-label">creation</span>
                </div>
              </div>
              <button
                className="btn-export"
                onClick={() => downloadCsv(result.contentPlanCsv, `content-plan-${slug()}.csv`)}
              >
                ↓ Export CSV
              </button>
            </div>

            {/* Insights */}
            <InsightsSection rows={result.contentPlan} />

            {/* Table */}
            <ContentPlanTable rows={result.contentPlan} />
          </section>
        )}

        {/* ── Empty state ────────────────────────────────────────────────── */}
        {!result && !isRunning && (
          <div className="empty-state">
            {csvText
              ? <p>File loaded — press <strong>Run Clustering</strong> to generate your content plan.</p>
              : <p>Upload a Monitorank CSV export to get started.</p>}
          </div>
        )}
      </main>
    </div>
  );
}

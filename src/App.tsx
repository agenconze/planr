import { useState, useRef, useMemo, useCallback, type ChangeEvent, type DragEvent } from 'react';
import {
  runSerpClusteringPipeline,
  type BuildOptions,
  type ContentPlanRow,
} from './pipeline';
import {
  computeStrategicInsights,
  toStrategicInputs,
  toPillarCsv,
  type StrategicInsights,
} from './strategicInsights';

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
// Strategic Briefing Section
// ---------------------------------------------------------------------------

function StrategicBriefingSection({ insights }: { insights: StrategicInsights }) {
  const [expandedPillarId, setExpandedPillarId] = useState<string | null>(null);
  const strongPillars = insights.strongPillars;
  const emergingTopics = insights.emergingTopics;

  return (
    <section className="strategic-panel">
      <div className="strategic-header">
        <div>
          <h3>Content Architecture</h3>
          <p>Families are built on all clusters, then ranked with a BLOG lens.</p>
        </div>
        <button
          className="btn-export-secondary"
          onClick={() => downloadCsv(toPillarCsv(insights), `pillar-opportunities-${slug()}.csv`)}
        >
          ↓ Export Pillar CSV
        </button>
      </div>

      <div className="strategic-summary-grid">
        <div className="strategic-summary-card">
          <div className="strategic-kpi-value">{insights.planSummary.totalClusters.toLocaleString()}</div>
          <div className="strategic-kpi-label">Total Clusters</div>
        </div>
        <div className="strategic-summary-card">
          <div className="strategic-kpi-value">
            {insights.planSummary.totalAddressableDemand.toLocaleString()}
          </div>
          <div className="strategic-kpi-label">Total Addressable Search Demand</div>
        </div>
        <div className="strategic-summary-card">
          <div className="strategic-kpi-value">{insights.planSummary.strongPillarCount.toLocaleString()}</div>
          <div className="strategic-kpi-label">Strong Pillars</div>
        </div>
        <div className="strategic-summary-card">
          <div className="strategic-kpi-value">{insights.planSummary.emergingTopicCount.toLocaleString()}</div>
          <div className="strategic-kpi-label">Emerging Topics</div>
        </div>
        <div className="strategic-summary-card">
          <div className="strategic-kpi-value">{insights.planSummary.standaloneCount.toLocaleString()}</div>
          <div className="strategic-kpi-label">Standalone Topics</div>
        </div>
      </div>

      <div className="strategic-grid">
        <div className="strategic-card strategic-card-pillar">
          <h4>Strong Pillars</h4>
          {strongPillars.length === 0 ? (
            <p className="muted">No strong pillars detected with the current quality gate.</p>
          ) : (
            <div className="strategic-list">
              {strongPillars.map((pillar) => (
                <div key={pillar.pillarId} className="strategic-list-item strategic-list-item-pillar">
                  <div>
                    <div className="strategic-title">{pillar.pillarKeyword}</div>
                    <div className="strategic-meta">
                      Total pillar opportunity: {pillar.totalPillarVolume.toLocaleString()}
                    </div>
                    <div className="strategic-meta">
                      Supporting articles: {pillar.supportingArticlesCount}
                    </div>
                    <div className="strategic-meta">
                      Family size: {pillar.familySize}
                    </div>
                    <button
                      className="btn-link"
                      onClick={() =>
                        setExpandedPillarId((current) =>
                          current === pillar.pillarId ? null : pillar.pillarId,
                        )
                      }
                    >
                      {expandedPillarId === pillar.pillarId
                        ? 'Hide supporting articles'
                        : 'Show supporting articles'}
                    </button>
                    {expandedPillarId === pillar.pillarId && (
                      pillar.supportingArticles.length > 0 ? (
                        <ul className="supporting-list">
                          {pillar.supportingArticles.map((article) => (
                            <li key={article.clusterId} className={article.intentType === 'BLOG' ? '' : 'non-blog'}>
                              {article.mainKeyword}
                              <span>{article.volume.toLocaleString()}</span>
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <div className="strategic-meta">No supporting articles detected.</div>
                      )
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="strategic-card">
          <h4>Emerging Topics</h4>
          {emergingTopics.length === 0 ? (
            <p className="muted">No emerging topics detected.</p>
          ) : (
            <div className="strategic-list">
              {emergingTopics.map((topic) => (
                <div key={topic.topicId} className="strategic-list-item">
                  <div>
                    <div className="strategic-title">{topic.candidateKeyword}</div>
                    <div className="strategic-meta">
                      Opportunity: {topic.totalTopicOpportunity.toLocaleString()} • Family size: {topic.familySize}
                    </div>
                  </div>
                  <div className="strategic-volume">{topic.supportingArticlesCount} supports</div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      <details className="strategic-secondary">
        <summary>Standalone topics (low priority)</summary>
        {insights.standaloneTopics.length === 0 ? (
          <p className="muted">No standalone topics detected.</p>
        ) : (
          <div className="strategic-list">
            {insights.standaloneTopics.map((topic) => (
              <div key={topic.clusterId} className="strategic-list-item">
                <div>
                  <div className="strategic-title">{topic.mainKeyword}</div>
                  <div className="strategic-meta">{topic.intentType}</div>
                </div>
                <div className="strategic-volume">{topic.volume.toLocaleString()}</div>
              </div>
            ))}
          </div>
        )}
      </details>
    </section>
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

type ResultTab = 'CONTENT_PLAN' | 'CONTENT_ARCHITECTURE' | 'SEO_FORECASTING';
const ENABLE_CONTENT_ARCHITECTURE_TAB = false;

export default function App() {
  const [csvText, setCsvText]       = useState('');
  const [fileName, setFileName]     = useState('');
  const [isDragging, setIsDragging] = useState(false);
  const [options, setOptions]       = useState(DEFAULT_OPTIONS);
  const [result, setResult]         = useState<PipelineResult | null>(null);
  const [isRunning, setIsRunning]   = useState(false);
  const [activeTab, setActiveTab]   = useState<ResultTab>('CONTENT_PLAN');
  const [currentCtrPct, setCurrentCtrPct] = useState(0.7);
  const [top10CtrPct, setTop10CtrPct]     = useState(2);
  const [top5CtrPct, setTop5CtrPct]       = useState(5);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const strategicInsights = useMemo(
    () =>
      ENABLE_CONTENT_ARCHITECTURE_TAB && result
        ? computeStrategicInsights(toStrategicInputs(result.contentPlan))
        : null,
    [result],
  );

  const clampCtr = (value: number) => Math.max(0, Math.min(20, value));
  const parseCtrInput = (value: string) => {
    if (value.trim() === '') return 0;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? clampCtr(parsed) : 0;
  };

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
        setActiveTab('CONTENT_PLAN');
      } catch (err) {
        alert(`Pipeline error: ${err instanceof Error ? err.message : String(err)}`);
      } finally {
        setIsRunning(false);
      }
    }, 0);
  };

  // ---- Render --------------------------------------------------------------

  const totalContentPlanVolume = result
    ? result.contentPlan.reduce((sum, row) => sum + row.mainKeywordVolume, 0)
    : 0;
  const weightedCtrPct = (top10CtrPct + top5CtrPct) / 2;
  const currentCtrDecimal = currentCtrPct / 100;
  const top10CtrDecimal = top10CtrPct / 100;
  const weightedCtrDecimal = weightedCtrPct / 100;
  const top5CtrDecimal = top5CtrPct / 100;
  const currentTraffic = Math.round(totalContentPlanVolume * currentCtrDecimal);
  const top10Traffic = Math.round(totalContentPlanVolume * top10CtrDecimal);
  const weightedTraffic = Math.round(totalContentPlanVolume * weightedCtrDecimal);
  const top5Traffic = Math.round(totalContentPlanVolume * top5CtrDecimal);

  const rampMonths = [3, 6, 9, 12] as const;
  const rampFactors = [0.3, 0.6, 0.8, 1] as const;

  const scenarios = [
    { key: 'current', label: 'Current CTR', ctrPct: currentCtrPct, traffic: currentTraffic, color: '#64748b' },
    { key: 'top10', label: 'Top 10', ctrPct: top10CtrPct, traffic: top10Traffic, color: '#d97706' },
    { key: 'weighted', label: 'Weighted', ctrPct: weightedCtrPct, traffic: weightedTraffic, color: '#7c3aed' },
    { key: 'top5', label: 'Top 5', ctrPct: top5CtrPct, traffic: top5Traffic, color: '#059669' },
  ] as const;

  const scenarioCurves = scenarios.map((scenario) => ({
    ...scenario,
    points: rampMonths.map((month, idx) => ({
      month,
      factor: rampFactors[idx],
      traffic: Math.round(scenario.traffic * rampFactors[idx]),
    })),
  }));

  const maxTraffic = Math.max(
    1,
    ...scenarioCurves.flatMap((curve) => curve.points.map((point) => point.traffic)),
  );

  const chartWidth = 860;
  const chartHeight = 360;
  const chartMargin = { top: 22, right: 22, bottom: 56, left: 80 };
  const plotWidth = chartWidth - chartMargin.left - chartMargin.right;
  const plotHeight = chartHeight - chartMargin.top - chartMargin.bottom;

  const xForIndex = (idx: number) =>
    chartMargin.left + (idx / (rampMonths.length - 1)) * plotWidth;
  const yForTraffic = (value: number) =>
    chartMargin.top + (1 - value / maxTraffic) * plotHeight;

  const yTicks = Array.from({ length: 5 }, (_, idx) => {
    const value = Math.round((maxTraffic * (4 - idx)) / 4);
    const y = chartMargin.top + (plotHeight * idx) / 4;
    return { value, y };
  });

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

          {/* Overlap threshold */}
          <div className="control-group">
            <span className="control-label">
              Overlap Threshold
              <span className="threshold-badge">{options.overlapThreshold}</span>
            </span>
            <div className="serp-depth-options">
              {[2, 3, 4].map((value) => (
                <label key={value} className={`depth-option ${options.overlapThreshold === value ? 'active' : ''}`}>
                  <input
                    type="radio"
                    name="overlapThreshold"
                    value={value}
                    checked={options.overlapThreshold === value}
                    onChange={() => setOptions((o) => ({ ...o, overlapThreshold: value }))}
                  />
                  {value}
                </label>
              ))}
            </div>
            <span className="control-hint">
              SERP depth is fixed to Top 10. Choose a controlled overlap level: 2, 3, or 4.
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

            <div className="results-tabs" role="tablist" aria-label="Result views">
              <button
                className={`tab-btn ${activeTab === 'CONTENT_PLAN' ? 'active' : ''}`}
                onClick={() => setActiveTab('CONTENT_PLAN')}
              >
                Content Plan
              </button>
              {ENABLE_CONTENT_ARCHITECTURE_TAB && (
                <button
                  className={`tab-btn ${activeTab === 'CONTENT_ARCHITECTURE' ? 'active' : ''}`}
                  onClick={() => setActiveTab('CONTENT_ARCHITECTURE')}
                >
                  Content Architecture
                </button>
              )}
              <button
                className={`tab-btn ${activeTab === 'SEO_FORECASTING' ? 'active' : ''}`}
                onClick={() => setActiveTab('SEO_FORECASTING')}
              >
                SEO Forecasting
              </button>
            </div>

            {activeTab === 'CONTENT_PLAN' && (
              <>
                <InsightsSection rows={result.contentPlan} />
                <ContentPlanTable rows={result.contentPlan} />
              </>
            )}

            {ENABLE_CONTENT_ARCHITECTURE_TAB && activeTab === 'CONTENT_ARCHITECTURE' && (
              strategicInsights ? (
                <StrategicBriefingSection insights={strategicInsights} />
              ) : (
                <div className="empty-state">
                  <p>No BLOG clusters detected for content architecture.</p>
                </div>
              )
            )}

            {activeTab === 'SEO_FORECASTING' && (
              <div className="forecasting-panel">
                <section className="forecast-section">
                  <h3 className="forecast-section-title">Inputs &amp; assumptions</h3>
                  <div className="forecast-assumptions-grid">
                    <div className="forecast-assumption-item forecast-assumption-static">
                      <span className="forecast-assumption-label">Total content plan demand</span>
                      <span className="forecast-assumption-value">{totalContentPlanVolume.toLocaleString()}</span>
                    </div>

                    <label className="forecast-assumption-item" htmlFor="current-ctr">
                      <span className="forecast-assumption-label">Current CTR</span>
                      <div className="forecast-input-wrap">
                        <input
                          id="current-ctr"
                          className="forecast-input"
                          type="number"
                          min={0}
                          max={20}
                          step={0.1}
                          value={currentCtrPct}
                          onChange={(e) => setCurrentCtrPct(parseCtrInput(e.target.value))}
                        />
                        <span className="forecast-input-suffix">%</span>
                      </div>
                    </label>

                    <label className="forecast-assumption-item" htmlFor="top10-ctr">
                      <span className="forecast-assumption-label">Top 10 CTR</span>
                      <div className="forecast-input-wrap">
                        <input
                          id="top10-ctr"
                          className="forecast-input"
                          type="number"
                          min={0}
                          max={20}
                          step={0.1}
                          value={top10CtrPct}
                          onChange={(e) => setTop10CtrPct(parseCtrInput(e.target.value))}
                        />
                        <span className="forecast-input-suffix">%</span>
                      </div>
                    </label>

                    <div className="forecast-assumption-item forecast-assumption-static">
                      <span className="forecast-assumption-label">Weighted CTR</span>
                      <span className="forecast-assumption-value">{weightedCtrPct.toFixed(1)}%</span>
                    </div>

                    <label className="forecast-assumption-item" htmlFor="top5-ctr">
                      <span className="forecast-assumption-label">Top 5 CTR</span>
                      <div className="forecast-input-wrap">
                        <input
                          id="top5-ctr"
                          className="forecast-input"
                          type="number"
                          min={0}
                          max={20}
                          step={0.1}
                          value={top5CtrPct}
                          onChange={(e) => setTop5CtrPct(parseCtrInput(e.target.value))}
                        />
                        <span className="forecast-input-suffix">%</span>
                      </div>
                    </label>

                    <div className="forecast-assumption-item forecast-assumption-static">
                      <span className="forecast-assumption-label">Ramp profile</span>
                      <span className="forecast-assumption-value">30% / 60% / 80% / 100%</span>
                    </div>
                  </div>
                </section>

                <section className="forecast-section">
                  <h3 className="forecast-section-title">Scenario ramp chart</h3>
                  <div className="forecast-chart-wrap">
                    <div className="forecast-legend">
                      {scenarios.map((scenario) => (
                        <div key={scenario.key} className="forecast-legend-item">
                          <span
                            className="forecast-legend-dot"
                            style={{ backgroundColor: scenario.color }}
                            aria-hidden="true"
                          />
                          <span>{scenario.label} ({scenario.ctrPct.toFixed(1)}%)</span>
                        </div>
                      ))}
                    </div>

                    <svg
                      className="forecast-chart"
                      viewBox={`0 0 ${chartWidth} ${chartHeight}`}
                      role="img"
                      aria-label="Traffic ramp projection chart for Current CTR, Top 10, Weighted, and Top 5 scenarios."
                    >
                      {yTicks.map((tick) => (
                        <g key={tick.y}>
                          <line
                            x1={chartMargin.left}
                            x2={chartWidth - chartMargin.right}
                            y1={tick.y}
                            y2={tick.y}
                            className="forecast-grid-line"
                          />
                          <text x={chartMargin.left - 10} y={tick.y + 4} className="forecast-axis-text forecast-axis-text-right">
                            {tick.value.toLocaleString()}
                          </text>
                        </g>
                      ))}

                      {rampMonths.map((month, idx) => (
                        <g key={month}>
                          <line
                            x1={xForIndex(idx)}
                            x2={xForIndex(idx)}
                            y1={chartMargin.top}
                            y2={chartHeight - chartMargin.bottom}
                            className="forecast-grid-line forecast-grid-line-vertical"
                          />
                          <text
                            x={xForIndex(idx)}
                            y={chartHeight - chartMargin.bottom + 18}
                            className="forecast-axis-text"
                            textAnchor="middle"
                          >
                            {month}
                          </text>
                        </g>
                      ))}

                      <line
                        x1={chartMargin.left}
                        x2={chartMargin.left}
                        y1={chartMargin.top}
                        y2={chartHeight - chartMargin.bottom}
                        className="forecast-axis-line"
                      />
                      <line
                        x1={chartMargin.left}
                        x2={chartWidth - chartMargin.right}
                        y1={chartHeight - chartMargin.bottom}
                        y2={chartHeight - chartMargin.bottom}
                        className="forecast-axis-line"
                      />

                      {scenarioCurves.map((scenario) => {
                        const points = scenario.points
                          .map((point, idx) => `${xForIndex(idx)},${yForTraffic(point.traffic)}`)
                          .join(' ');

                        return (
                          <g key={scenario.key}>
                            <polyline
                              points={points}
                              fill="none"
                              stroke={scenario.color}
                              strokeWidth="3"
                              strokeLinecap="round"
                              strokeLinejoin="round"
                            />
                            {scenario.points.map((point, idx) => (
                              <circle
                                key={`${scenario.key}-${point.month}`}
                                cx={xForIndex(idx)}
                                cy={yForTraffic(point.traffic)}
                                r="4.5"
                                fill={scenario.color}
                                stroke="#ffffff"
                                strokeWidth="1.5"
                              >
                                <title>
                                  {`${scenario.label}\nMonth: ${point.month}\nCTR: ${scenario.ctrPct.toFixed(1)}%\nEstimated traffic: ${point.traffic.toLocaleString()}`}
                                </title>
                              </circle>
                            ))}
                          </g>
                        );
                      })}

                      <text
                        x={(chartMargin.left + (chartWidth - chartMargin.right)) / 2}
                        y={chartHeight - 10}
                        className="forecast-axis-label"
                        textAnchor="middle"
                      >
                        Time after content plan is live (months)
                      </text>
                      <text
                        x={18}
                        y={chartMargin.top + plotHeight / 2}
                        className="forecast-axis-label"
                        textAnchor="middle"
                        transform={`rotate(-90 18 ${chartMargin.top + plotHeight / 2})`}
                      >
                        Estimated monthly organic traffic
                      </text>
                    </svg>
                  </div>
                </section>

                <p className="forecast-note">
                  Ramp-up projection after content plan is live; not publication cadence modeling.
                </p>
              </div>
            )}
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

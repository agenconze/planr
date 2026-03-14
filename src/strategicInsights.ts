import type { ContentPlanRow } from './pipeline';

export type StrategicLanguageMode = 'auto' | 'en' | 'fr';
export type StrategicIntentType = 'BLOG' | 'PRODUCT' | 'MIXED' | '—';

export type StrategicClusterInput = {
  clusterId: string;
  mainKeyword: string;
  mainKeywordVolume: number;
  clusterSize: number;
  intentType: StrategicIntentType;
};

export type StrategicLanguageOptions = {
  mode: StrategicLanguageMode;
  fallback: 'en' | 'fr';
  autoDetectMinKeywords: number;
};

export type StrategicNormalizationOptions = {
  lowercase: boolean;
  stripPunctuation: boolean;
  splitOnHyphen: boolean;
  minTokenLength: number;
  lightStemming: boolean;
  keepAlphaNumeric: boolean;
  dropPureYearTokens: boolean;
  yearRegex: RegExp;
};

export type StrategicSimilarityOptions = {
  weights: {
    leadMatch: number;
    tokenJaccard: number;
    stemJaccard: number;
  };
  thresholds: {
    pairMinSimilarity: number;
    headMatchOverrideTokenJaccard: number;
    minMacroTopicSize: number;
  };
  headOverrideGuards: {
    disableIfLeadIsGenericAnchor: boolean;
  };
};

export type StrategicBrandPolicy = {
  mode: 'conservative-separate';
  rule: string;
};

export type StrategicOptions = {
  language: StrategicLanguageOptions;
  normalization: StrategicNormalizationOptions;
  stopwords: {
    common: string[];
    en: string[];
    fr: string[];
  };
  keepInAnalysisButBlockAsLabel: string[];
  blockedLabelTokens: string[];
  genericAnchorTokens: string[];
  similarity: StrategicSimilarityOptions;
  brandPolicy: StrategicBrandPolicy;
  qualityGate: {
    minSupportingClustersForStrong: number;
    minFamilySizeForStrong: number;
    minTotalFamilyOpportunity: number;
  };
};

export type PlanSummary = {
  totalClusters: number;
  totalAddressableDemand: number;
  strongPillarCount: number;
  emergingTopicCount: number;
  standaloneCount: number;
};

export type SupportingArticle = {
  clusterId: string;
  mainKeyword: string;
  volume: number;
  intentType: StrategicIntentType;
};

export type PillarOpportunity = {
  pillarId: string;
  pillarKeyword: string;
  totalPillarVolume: number;
  familySize: number;
  blogContributionShare: number;
  supportingArticlesCount: number;
  supportingArticles: SupportingArticle[];
};

export type EmergingTopic = {
  topicId: string;
  candidateKeyword: string;
  totalTopicOpportunity: number;
  familySize: number;
  supportingArticlesCount: number;
  blogContributionShare: number;
  supportingArticles: SupportingArticle[];
};

export type StandaloneTopic = {
  clusterId: string;
  mainKeyword: string;
  volume: number;
  intentType: StrategicIntentType;
};

export type StrategicInsights = {
  planSummary: PlanSummary;
  strongPillars: PillarOpportunity[];
  emergingTopics: EmergingTopic[];
  standaloneTopics: StandaloneTopic[];
};

function escapeCsvCell(value: string | number): string {
  const text = String(value);
  const escaped = text.replace(/"/g, '""');
  if (/[",\n]/.test(escaped)) {
    return `"${escaped}"`;
  }
  return escaped;
}

type PreparedCluster = {
  clusterId: string;
  mainKeyword: string;
  mainKeywordVolume: number;
  clusterSize: number;
  intentType: StrategicIntentType;
  normalizedKeyword: string;
  tokens: string[];
  stems: string[];
  leadToken: string;
};

type SimilarityEdge = {
  a: number;
  b: number;
};

export const V1_STRATEGIC_OPTIONS: StrategicOptions = {
  language: {
    mode: 'auto',
    fallback: 'en',
    autoDetectMinKeywords: 8,
  },
  normalization: {
    lowercase: true,
    stripPunctuation: true,
    splitOnHyphen: true,
    minTokenLength: 2,
    lightStemming: true,
    keepAlphaNumeric: true,
    dropPureYearTokens: true,
    yearRegex: /^(19|20)\d{2}$/,
  },
  stopwords: {
    common: [
      'what',
      'how',
      'why',
      'when',
      'where',
      'which',
      'who',
      'le',
      'la',
      'les',
      'un',
      'une',
      'des',
      'de',
      'du',
      'd',
      'a',
      'an',
      'the',
      'for',
      'to',
      'of',
      'in',
      'on',
      'and',
      'or',
      'with',
      'without',
      'from',
      'by',
    ],
    en: ['is', 'are', 'this', 'that', 'these', 'those'],
    fr: ['est', 'sont', 'ce', 'cette', 'ces', 'pourquoi', 'comment', 'que', 'quoi'],
  },
  keepInAnalysisButBlockAsLabel: [
    'definition',
    'meaning',
    'guide',
    'comparison',
    'vs',
    'checklist',
    'template',
    'comparatif',
  ],
  blockedLabelTokens: [
    'software',
    'tool',
    'tools',
    'solution',
    'solutions',
    'platform',
    'platforms',
    'system',
    'systems',
    'service',
    'services',
    'process',
    'processes',
    'logiciel',
    'outil',
    'outils',
    'plateforme',
    'plateformes',
    'systeme',
    'systemes',
    'système',
    'systèmes',
    'processus',
    'definition',
    'meaning',
    'guide',
    'comparison',
    'comparatif',
    'vs',
    'checklist',
    'template',
  ],
  genericAnchorTokens: [
    'data',
    'marketing',
    'business',
    'software',
    'logiciel',
    'solution',
    'plateforme',
  ],
  similarity: {
    weights: {
      leadMatch: 0.4,
      tokenJaccard: 0.4,
      stemJaccard: 0.2,
    },
    thresholds: {
      pairMinSimilarity: 0.62,
      headMatchOverrideTokenJaccard: 0.3,
      minMacroTopicSize: 2,
    },
    headOverrideGuards: {
      disableIfLeadIsGenericAnchor: true,
    },
  },
  brandPolicy: {
    mode: 'conservative-separate',
    rule: 'Do not auto-merge different brand-led clusters unless there is strong non-brand overlap.',
  },
  qualityGate: {
    minSupportingClustersForStrong: 2,
    minFamilySizeForStrong: 4,
    minTotalFamilyOpportunity: 1000,
  },
};

export function toStrategicInputs(rows: ContentPlanRow[]): StrategicClusterInput[] {
  return rows.map((row) => ({
    clusterId: row.clusterId,
    mainKeyword: row.intentMainKeyword,
    mainKeywordVolume: Number.isFinite(row.mainKeywordVolume) ? row.mainKeywordVolume : 0,
    clusterSize: row.clusterSize,
    intentType: row.intentType,
  }));
}

function stripDiacritics(input: string): string {
  return input.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function languageStopwords(options: StrategicOptions, mode: 'en' | 'fr'): Set<string> {
  const entries = [
    ...options.stopwords.common,
    ...(mode === 'fr' ? options.stopwords.fr : options.stopwords.en),
  ].map((token) => stripDiacritics(token.toLowerCase()));
  return new Set(entries);
}

function detectLanguage(inputs: StrategicClusterInput[], options: StrategicOptions): 'en' | 'fr' {
  if (options.language.mode === 'en' || options.language.mode === 'fr') {
    return options.language.mode;
  }

  if (inputs.length < options.language.autoDetectMinKeywords) {
    return options.language.fallback;
  }

  const frSet = new Set(options.stopwords.fr.map((token) => stripDiacritics(token.toLowerCase())));
  const enSet = new Set(options.stopwords.en.map((token) => stripDiacritics(token.toLowerCase())));

  let frHits = 0;
  let enHits = 0;

  for (const input of inputs) {
    const text = stripDiacritics(input.mainKeyword.toLowerCase());
    const parts = text.split(/\s+/g);
    for (const part of parts) {
      if (frSet.has(part)) frHits += 1;
      if (enSet.has(part)) enHits += 1;
    }
  }

  if (frHits === enHits) return options.language.fallback;
  return frHits > enHits ? 'fr' : 'en';
}

function stemToken(token: string): string {
  if (token.length <= 3) return token;

  if (token.endsWith('ies') && token.length > 4) {
    return `${token.slice(0, -3)}y`;
  }

  if (token.endsWith('es') && token.length > 4) {
    return token.slice(0, -2);
  }

  if (token.endsWith('s') && token.length > 3) {
    return token.slice(0, -1);
  }

  return token;
}

function normalizeKeyword(
  keyword: string,
  stopwords: Set<string>,
  options: StrategicOptions,
): { normalizedKeyword: string; tokens: string[]; stems: string[] } {
  let normalized = keyword;

  if (options.normalization.lowercase) {
    normalized = normalized.toLowerCase();
  }

  normalized = stripDiacritics(normalized);

  if (options.normalization.splitOnHyphen) {
    normalized = normalized.replace(/-/g, ' ');
  }

  if (options.normalization.stripPunctuation) {
    normalized = normalized.replace(/[^\p{L}\p{N}\s]/gu, ' ');
  }

  const rawTokens = normalized
    .split(/\s+/g)
    .map((token) => token.trim())
    .filter(Boolean);

  const tokens = rawTokens.filter((token) => {
    if (token.length < options.normalization.minTokenLength) {
      return false;
    }

    if (options.normalization.dropPureYearTokens && options.normalization.yearRegex.test(token)) {
      return false;
    }

    if (!options.normalization.keepAlphaNumeric && /\d/.test(token)) {
      return false;
    }

    if (stopwords.has(token)) {
      return false;
    }

    return true;
  });

  const stems = options.normalization.lightStemming
    ? tokens.map((token) => stemToken(token))
    : [...tokens];

  return {
    normalizedKeyword: normalized.trim(),
    tokens,
    stems,
  };
}

function jaccard(a: string[], b: string[]): number {
  if (a.length === 0 || b.length === 0) return 0;
  const setA = new Set(a);
  const setB = new Set(b);

  let intersection = 0;
  for (const token of setA) {
    if (setB.has(token)) intersection += 1;
  }

  const union = new Set([...setA, ...setB]).size;
  return union === 0 ? 0 : intersection / union;
}

function createPreparedClusters(
  inputs: StrategicClusterInput[],
  options: StrategicOptions,
): PreparedCluster[] {
  const lang = detectLanguage(inputs, options);
  const stopwords = languageStopwords(options, lang);

  return inputs.map((input) => {
    const normalized = normalizeKeyword(input.mainKeyword, stopwords, options);
    return {
      clusterId: input.clusterId,
      mainKeyword: input.mainKeyword,
      mainKeywordVolume: Number.isFinite(input.mainKeywordVolume) ? input.mainKeywordVolume : 0,
      clusterSize: input.clusterSize,
      intentType: input.intentType,
      normalizedKeyword: normalized.normalizedKeyword,
      tokens: normalized.tokens,
      stems: normalized.stems,
      leadToken: normalized.tokens[0] ?? '__empty__',
    };
  });
}

function isGenericAnchor(token: string, options: StrategicOptions): boolean {
  const generic = new Set(options.genericAnchorTokens.map((t) => stripDiacritics(t.toLowerCase())));
  return generic.has(token);
}

function similarityScore(a: PreparedCluster, b: PreparedCluster, options: StrategicOptions): number {
  const tokensJaccard = jaccard(a.tokens, b.tokens);
  const stemsJaccard = jaccard(a.stems, b.stems);

  const rawLeadMatch = a.leadToken !== '__empty__' && a.leadToken === b.leadToken;
  const leadMatch =
    rawLeadMatch && !isGenericAnchor(a.leadToken, options)
      ? 1
      : 0;

  const score =
    options.similarity.weights.leadMatch * leadMatch +
    options.similarity.weights.tokenJaccard * tokensJaccard +
    options.similarity.weights.stemJaccard * stemsJaccard;

  return Math.min(1, Math.max(0, score));
}

function shouldCreateEdge(a: PreparedCluster, b: PreparedCluster, options: StrategicOptions): boolean {
  const score = similarityScore(a, b, options);
  if (score >= options.similarity.thresholds.pairMinSimilarity) {
    if (
      options.brandPolicy.mode === 'conservative-separate' &&
      a.leadToken !== '__empty__' &&
      b.leadToken !== '__empty__' &&
      a.leadToken !== b.leadToken
    ) {
      const shared = [...new Set(a.tokens)].filter((token) => b.tokens.includes(token));
      const nonGenericShared = shared.filter((token) => !isGenericAnchor(token, options));
      if (nonGenericShared.length === 0) return false;
      const sharedRatio = jaccard(a.tokens, b.tokens);
      if (nonGenericShared.length === 1 && sharedRatio < 0.7) return false;
    }
    return true;
  }

  const leadMatch = a.leadToken !== '__empty__' && a.leadToken === b.leadToken;
  if (!leadMatch) return false;

  if (
    options.similarity.headOverrideGuards.disableIfLeadIsGenericAnchor &&
    isGenericAnchor(a.leadToken, options)
  ) {
    return false;
  }

  const tokenJaccard = jaccard(a.tokens, b.tokens);
  return tokenJaccard >= options.similarity.thresholds.headMatchOverrideTokenJaccard;
}

function connectedComponents(size: number, edges: SimilarityEdge[]): number[][] {
  const adjacency: number[][] = Array.from({ length: size }, () => []);
  for (const edge of edges) {
    adjacency[edge.a].push(edge.b);
    adjacency[edge.b].push(edge.a);
  }

  const visited = new Array<boolean>(size).fill(false);
  const components: number[][] = [];

  for (let i = 0; i < size; i += 1) {
    if (visited[i]) continue;
    const stack = [i];
    visited[i] = true;
    const component: number[] = [];

    while (stack.length > 0) {
      const current = stack.pop()!;
      component.push(current);
      for (const neighbor of adjacency[current]) {
        if (!visited[neighbor]) {
          visited[neighbor] = true;
          stack.push(neighbor);
        }
      }
    }

    component.sort((a, b) => a - b);
    components.push(component);
  }

  return components;
}

function createMacroTopicId(memberClusterIds: string[]): string {
  const sorted = [...memberClusterIds].sort((a, b) => a.localeCompare(b));
  return `MT-${sorted.join('__')}`;
}

function deriveMacroTopicLabel(component: PreparedCluster[], options: StrategicOptions): string {
  const blocked = new Set(
    [...options.blockedLabelTokens, ...options.keepInAnalysisButBlockAsLabel].map((token) =>
      stripDiacritics(token.toLowerCase()),
    ),
  );
  const scoreByToken = new Map<string, number>();

  for (const cluster of component) {
    const uniqueTokens = new Set(cluster.tokens);
    for (const token of uniqueTokens) {
      scoreByToken.set(token, (scoreByToken.get(token) ?? 0) + 1);
    }
    if (cluster.leadToken !== '__empty__') {
      scoreByToken.set(cluster.leadToken, (scoreByToken.get(cluster.leadToken) ?? 0) + 0.5);
    }
  }

  const candidates = [...scoreByToken.entries()]
    .filter(([token]) => !blocked.has(token) && !isGenericAnchor(token, options))
    .sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      return a[0].localeCompare(b[0]);
    });

  if (candidates.length > 0) return candidates[0][0].toUpperCase();

  const fallback = [...component]
    .sort((a, b) => {
      if (b.mainKeywordVolume !== a.mainKeywordVolume) {
        return b.mainKeywordVolume - a.mainKeywordVolume;
      }
      return a.mainKeyword.localeCompare(b.mainKeyword);
    })[0]
    .mainKeyword;

  return fallback;
}

function choosePillar(component: PreparedCluster[]): PreparedCluster {
  return [...component].sort((a, b) => {
    if (b.mainKeywordVolume !== a.mainKeywordVolume) {
      return b.mainKeywordVolume - a.mainKeywordVolume;
    }

    if (a.tokens.length !== b.tokens.length) {
      return a.tokens.length - b.tokens.length;
    }

    if (a.normalizedKeyword.length !== b.normalizedKeyword.length) {
      return a.normalizedKeyword.length - b.normalizedKeyword.length;
    }

    const keywordCompare = a.mainKeyword.localeCompare(b.mainKeyword);
    if (keywordCompare !== 0) return keywordCompare;

    return a.clusterId.localeCompare(b.clusterId);
  })[0];
}

export function computeStrategicInsights(
  inputs: StrategicClusterInput[],
  options: StrategicOptions = V1_STRATEGIC_OPTIONS,
): StrategicInsights {
  const prepared = createPreparedClusters(inputs, options);

  const edges: SimilarityEdge[] = [];
  for (let i = 0; i < prepared.length; i += 1) {
    for (let j = i + 1; j < prepared.length; j += 1) {
      if (shouldCreateEdge(prepared[i], prepared[j], options)) {
        edges.push({ a: i, b: j });
      }
    }
  }

  const components = connectedComponents(prepared.length, edges);

  const strongPillars: PillarOpportunity[] = [];
  const emergingTopics: EmergingTopic[] = [];
  const standaloneTopics: StandaloneTopic[] = [];

  for (const componentIndexes of components) {
    const component = componentIndexes.map((index) => prepared[index]);

    if (component.length < options.similarity.thresholds.minMacroTopicSize) {
      for (const cluster of component) {
        standaloneTopics.push({
          clusterId: cluster.clusterId,
          mainKeyword: cluster.mainKeyword,
          volume: cluster.mainKeywordVolume,
          intentType: cluster.intentType,
        });
      }
      continue;
    }

    const pillar = choosePillar(component);
    const clusterIds = component.map((cluster) => cluster.clusterId).sort((a, b) => a.localeCompare(b));
    const pillarId = createMacroTopicId(clusterIds);
    const totalPillarVolume = component.reduce((sum, cluster) => sum + cluster.mainKeywordVolume, 0);
    const familySize = component.length;

    const supportingArticles = component
      .filter((cluster) => cluster.clusterId !== pillar.clusterId)
      .map((cluster) => ({
        clusterId: cluster.clusterId,
        mainKeyword: cluster.mainKeyword,
        volume: cluster.mainKeywordVolume,
        intentType: cluster.intentType,
      }))
      .sort((a, b) => {
        if (a.intentType === 'BLOG' && b.intentType !== 'BLOG') return -1;
        if (b.intentType === 'BLOG' && a.intentType !== 'BLOG') return 1;
        if (b.volume !== a.volume) return b.volume - a.volume;
        const kwCmp = a.mainKeyword.localeCompare(b.mainKeyword);
        if (kwCmp !== 0) return kwCmp;
        return a.clusterId.localeCompare(b.clusterId);
      });

    const blogVolume = component
      .filter((cluster) => cluster.intentType === 'BLOG')
      .reduce((sum, cluster) => sum + cluster.mainKeywordVolume, 0);
    const blogContributionShare =
      totalPillarVolume > 0 ? blogVolume / totalPillarVolume : 0;
    const supportingArticlesCount = supportingArticles.length;

    const isStrongPillar =
      supportingArticlesCount >= options.qualityGate.minSupportingClustersForStrong &&
      familySize >= options.qualityGate.minFamilySizeForStrong &&
      totalPillarVolume >= options.qualityGate.minTotalFamilyOpportunity;

    if (isStrongPillar) {
      strongPillars.push({
        pillarId,
        pillarKeyword: pillar.mainKeyword,
        totalPillarVolume,
        familySize,
        blogContributionShare,
        supportingArticlesCount,
        supportingArticles,
      });
    } else {
      emergingTopics.push({
        topicId: pillarId,
        candidateKeyword: pillar.mainKeyword,
        totalTopicOpportunity: totalPillarVolume,
        familySize,
        supportingArticlesCount,
        blogContributionShare,
        supportingArticles,
      });
    }
  }

  const sortedStrongPillars = [...strongPillars].sort((a, b) => {
    if (b.blogContributionShare !== a.blogContributionShare) {
      return b.blogContributionShare - a.blogContributionShare;
    }
    if (b.totalPillarVolume !== a.totalPillarVolume) {
      return b.totalPillarVolume - a.totalPillarVolume;
    }
    return a.pillarKeyword.localeCompare(b.pillarKeyword);
  });

  const sortedEmergingTopics = [...emergingTopics].sort((a, b) => {
    if (b.blogContributionShare !== a.blogContributionShare) {
      return b.blogContributionShare - a.blogContributionShare;
    }
    if (b.totalTopicOpportunity !== a.totalTopicOpportunity) {
      return b.totalTopicOpportunity - a.totalTopicOpportunity;
    }
    return a.candidateKeyword.localeCompare(b.candidateKeyword);
  });

  const sortedStandaloneTopics = [...standaloneTopics].sort((a, b) => {
    if (a.intentType === 'BLOG' && b.intentType !== 'BLOG') return -1;
    if (b.intentType === 'BLOG' && a.intentType !== 'BLOG') return 1;
    if (b.volume !== a.volume) return b.volume - a.volume;
    const kwCmp = a.mainKeyword.localeCompare(b.mainKeyword);
    if (kwCmp !== 0) return kwCmp;
    return a.clusterId.localeCompare(b.clusterId);
  });

  const planSummary: PlanSummary = {
    totalClusters: prepared.length,
    totalAddressableDemand: prepared.reduce((sum, cluster) => sum + cluster.mainKeywordVolume, 0),
    strongPillarCount: sortedStrongPillars.length,
    emergingTopicCount: sortedEmergingTopics.length,
    standaloneCount: sortedStandaloneTopics.length,
  };

  return {
    planSummary,
    strongPillars: sortedStrongPillars,
    emergingTopics: sortedEmergingTopics,
    standaloneTopics: sortedStandaloneTopics,
  };
}

export function toPillarCsv(insights: StrategicInsights): string {
  const header = [
    'Pillar Keyword',
    'Total Pillar Opportunity',
    'Supporting Articles Count',
    'Supporting Article Keywords',
  ];

  const lines = insights.strongPillars.map((pillar) =>
    [
      pillar.pillarKeyword,
      pillar.totalPillarVolume,
      pillar.supportingArticlesCount,
      pillar.supportingArticles.map((article) => article.mainKeyword).join(' | '),
    ]
      .map(escapeCsvCell)
      .join(','),
  );

  return [header.map(escapeCsvCell).join(','), ...lines].join('\n');
}

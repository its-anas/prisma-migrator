import {Client, Pool} from 'pg';
import 'dotenv/config';
import * as cheerio from 'cheerio';
import {ProxyAgent, fetch as undiciFetch} from 'undici';
import fs from 'fs';
import {Parser} from 'json2csv';

const databaseUrl = process.env.DB_URL;

const dbConfig = {
  connectionString: databaseUrl
};

export const pool = new Pool(dbConfig);

export const query = async (text: string, params: string[]): Promise<any> => {
  const client = new Client(dbConfig);

  try {
    await client.connect();
    const result = await client.query(text, params);
    return result;
  } catch (err) {
    console.error('Database Query Error', err);
    throw err;
  } finally {
    await client.end();
  }
};

async function fetchWithProxy(url: string) {
  const PROXY_HOST = process.env.PROXY_HOST;
  const PROXY_PORT = process.env.PROXY_PORT;
  const PROXY_USERNAME = process.env.PROXY_USERNAME;
  const PROXY_PASSWORD = process.env.PROXY_PASSWORD;

  if (!PROXY_HOST || !PROXY_PORT || !PROXY_USERNAME || !PROXY_PASSWORD) {
    throw new Error('Proxy configuration is missing. Please set PROXY_HOST, PROXY_PORT, PROXY_USERNAME, and PROXY_PASSWORD in your environment variables.');
  }

  const proxyUrl = `http://${encodeURIComponent(PROXY_USERNAME)}:` + `${encodeURIComponent(PROXY_PASSWORD)}@${PROXY_HOST}:${PROXY_PORT}`;

  const proxyAgent = new ProxyAgent({uri: proxyUrl});

  return undiciFetch(url, {dispatcher: proxyAgent});
}

export async function fetchHTML(url: string): Promise<cheerio.CheerioAPI | null> {
  const maxAttempts = 3;
  let attempt = 0;
  let lastError: any;

  while (attempt < maxAttempts) {
    try {
      const response = await fetchWithProxy(url);
      if (!response.ok) {
        if (response.status === 429) throw new Error('Rate limit exceeded');
        if (response.status === 404) return null;
        throw new Error(`HTTP error: ${response.status}`);
      }
      const html = await response.text();
      return cheerio.load(html);
    } catch (error) {
      lastError = error;
      attempt++;
      if (attempt >= maxAttempts) {
        throw new Error(`Error fetching HTML for url ${url}: ${error}`);
      }
    }
  }

  throw lastError;
}

// Modified buildBulkInsertQuery to execute queries directly
async function buildBulkInsertQuery<T>(table: string, columns: string[], items: T[], transform: (item: T) => any[], conflictColumns?: string[], conflictTarget?: string[]): Promise<void> {
  if (items.length === 0) return;
  const PG_MAX_PARAMS = 50000; // max allowed parameters by pg
  const batchSize = Math.floor(PG_MAX_PARAMS / columns.length) || 1;
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const queryValues: any[] = [];
    const placeholders = batch
      .map((item, bi) => {
        const values = transform(item);
        queryValues.push(...values);
        return '(' + columns.map((_, j) => `$${bi * columns.length + j + 1}`).join(', ') + ')';
      })
      .join(', ');
    const target = conflictTarget && conflictTarget.length > 0 ? conflictTarget : ['handle'];
    const conflictClause = conflictColumns && conflictColumns.length > 0 ? ` ON CONFLICT (${target.join(', ')}) DO UPDATE SET ` + conflictColumns.map((col) => `${col} = EXCLUDED.${col}`).join(', ') : ` ON CONFLICT DO NOTHING`;
    const queryText = `INSERT INTO "${table}" (${columns.join(', ')}) VALUES ${placeholders}${conflictClause}`;
    await pool.query(queryText, queryValues);
  }
}
export async function saveDevelopers(developers: Developer[]): Promise<void> {
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${developers.length} developers...`);
  try {
    const columns = ['handle', 'name', 'address', 'email', '"countryCode"', '"createdAt"', '"updatedAt"'];
    await buildBulkInsertQuery('Developer', columns, developers, (dev) => [dev.handle, dev.name, dev.address, dev.email, dev.countryCode, new Date(), new Date()], ['name', 'address', 'email', '"countryCode"', '"updatedAt"'], ['handle']);
  } catch (error) {
    throw new Error(`Error saving developers: ${error}`);
  }
}

export async function saveCategories(categories: Category[]): Promise<void> {
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${categories.length} categories...`);
  try {
    const columns = ['handle', 'name', 'level', '"parentHandle"', '"createdAt"', '"updatedAt"'];
    await buildBulkInsertQuery('Category', columns, categories, (cat) => [cat.handle, cat.name, cat.level, cat.parentHandle, new Date(), new Date() || null], ['name', 'level', '"parentHandle"', '"updatedAt"'], ['handle']);
  } catch (error) {
    throw new Error(`Error saving categories: ${error}`);
  }
}

export async function saveCategoryFeatures(categoryFeatures: CategoryFeature[]): Promise<void> {
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${categoryFeatures.length} categoryFeatures...`);
  try {
    const columns = ['handle', 'name', '"createdAt"', '"updatedAt"'];
    await buildBulkInsertQuery('CategoryFeature', columns, categoryFeatures, (categoryFeature) => [categoryFeature.handle, categoryFeature.name, new Date(), new Date()], ['name', '"updatedAt"'], ['handle']);
  } catch (error) {
    throw new Error(`Error saving category features: ${error}`);
  }
}

export async function saveCategoryFeatureGroups(categoryFeatureGroups: CategoryFeatureGroup[]): Promise<void> {
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${categoryFeatureGroups.length} categoryFeature groups...\n`);
  try {
    const columns = ['handle', 'name', '"createdAt"', '"updatedAt"'];
    await buildBulkInsertQuery('CategoryFeatureGroup', columns, categoryFeatureGroups, (tg) => [tg.handle, tg.name, new Date(), new Date()], ['name', '"updatedAt"'], ['handle']);
  } catch (error) {
    throw new Error(`Error saving category feature groups: ${error}`);
  }
}

export async function saveApps(apps: App[]): Promise<void> {
  if (!apps.length) return;
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${apps.length} apps...`);
  try {
    const columns = ['handle', '"developerHandle"', '"launchDate"', '"createdAt"', '"updatedAt"'];
    await buildBulkInsertQuery('App', columns, apps, (app) => [app.handle, app.developerHandle, app.launchDate, new Date(), new Date()], ['"developerHandle"', '"launchDate"', '"updatedAt"'], ['handle']);
  } catch (error) {
    throw new Error(`Error saving apps: ${error}`);
  }
}

export async function saveAppSnapshots(appSnapshots: AppSnapshot[]): Promise<void> {
  if (!appSnapshots.length) return;
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${appSnapshots.length} app snapshots...`);
  try {
    const columns = [
      'handle',
      'name',
      'tagline',
      '"pricingText"',
      '"reviewCount"',
      'rating',
      '"isBuiltForShopify"',
      '"logoUrl"',
      '"demoStoreUrl"',
      'introduction',
      'description',
      '"metaTitle"',
      '"metaDescription"',
      'plans',
      'media',
      'languages',
      '"articlesFeaturedIn"',
      'integrations',
      'features',
      '"capturedAt"'
    ];
    await buildBulkInsertQuery(
      'AppSnapshot',
      columns,
      appSnapshots,
      (snap) => [
        snap.handle,
        snap.name,
        snap.tagline,
        snap.pricingText,
        snap.reviewCount,
        snap.rating,
        snap.isBuiltForShopify,
        snap.logoUrl,
        snap.demoStoreUrl || null,
        snap.introduction,
        snap.description,
        snap.metaTitle || null,
        snap.metaDescription || null,
        JSON.stringify(snap.plans),
        JSON.stringify(snap.media),
        JSON.stringify(snap.languages),
        JSON.stringify(snap.articlesFeaturedIn),
        JSON.stringify(snap.integrations),
        JSON.stringify(snap.features),
        snap.capturedAt
      ],
      [
        'name',
        'tagline',
        '"pricingText"',
        '"reviewCount"',
        'rating',
        '"isBuiltForShopify"',
        '"logoUrl"',
        '"demoStoreUrl"',
        'introduction',
        'description',
        '"metaTitle"',
        '"metaDescription"',
        'plans',
        'media',
        'languages',
        '"articlesFeaturedIn"',
        'integrations',
        'features',
        '"capturedAt"'
      ],
      ['handle', '"capturedAt"']
    );
  } catch (error) {
    throw new Error(`Error saving app snapshots: ${error}`);
  }
}

export async function saveRecommendedApps(recommendedApps: RecommendedAppHistory[]): Promise<void> {
  if (!recommendedApps.length) return;
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${recommendedApps.length} recommended apps...`);
  try {
    const columns = ['"appHandle"', '"recommendedAppHandle"', 'position', '"capturedAt"'];
    await buildBulkInsertQuery(
      'RecommendedAppHistory',
      columns,
      recommendedApps,
      (rec) => [rec.appHandle, rec.recommendedAppHandle, rec.position, new Date()],
      ['position', '"capturedAt"'],
      ['"appHandle"', '"recommendedAppHandle"', 'position', '"capturedAt"']
    );
  } catch (error) {
    throw new Error(`Error saving recommended apps: ${error}`);
  }
}

export async function saveAppCategories(appCategories: AppCategoryHistory[]): Promise<void> {
  if (!appCategories.length) return;
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${appCategories.length} app categories...`);
  try {
    const columns = ['"appHandle"', '"categoryHandle"', '"categoryFeatures"', 'role', '"capturedAt"'];
    await buildBulkInsertQuery(
      'AppCategoryHistory', // changed table name
      columns,
      appCategories,
      (ac) => [ac.appHandle, ac.categoryHandle, ac.categoryFeatures, ac.role, new Date()],
      ['role', '"capturedAt"'],
      ['"appHandle"', '"categoryHandle"', 'role', '"capturedAt"']
    );
  } catch (error) {
    throw new Error(`Error saving app categories: ${error}`);
  }
}

export async function saveCategoryAppPositions(positions: CategoryAppPositionHistory[]): Promise<void> {
  if (!positions.length) return;
  process.stdout.write(`\n${new Date().toLocaleString()} - Saving ${positions.length} category app positions...`);
  try {
    const columns = ['"categoryHandle"', '"appHandle"', 'position', '"capturedAt"'];
    await buildBulkInsertQuery(
      'CategoryAppPositionHistory', // changed table name
      columns,
      positions,
      (pos) => [pos.categoryHandle, pos.appHandle, pos.position, pos.capturedAt],
      [],
      ['"categoryHandle"', '"appHandle"', '"capturedAt"']
    );
  } catch (error) {
    throw new Error(`Error saving category app positions: ${error}`);
  }
}

export async function getAppsByCategory(categoriesHandles: string[], maxPosition?: number): Promise<AppSnapshot[]> {
  let categoryCondition = '';
  let extraCondition = '';
  const params: any[] = [];

  if (categoriesHandles.length > 0) {
    categoryCondition = `
      capp."categoryHandle" = ANY($1::text[])
      AND ach."categoryHandle" = ANY($1::text[])
      AND ach.role IN ('PRIMARY', 'SECONDARY')
    `;
    params.push(categoriesHandles);
  } else {
    categoryCondition = 'TRUE';
  }

  if (maxPosition !== undefined) {
    extraCondition = `AND capp."position" <= $${params.length + 1}`;
    params.push(maxPosition);
  }

  const queryText = `
    SELECT DISTINCT ON (snap."handle") snap.*
    FROM "AppSnapshot" as snap
    WHERE snap."handle" IN (
      SELECT a."handle"
      FROM "App" a
      JOIN "CategoryAppPositionHistory" capp ON capp."appHandle" = a."handle"
      JOIN "AppCategoryHistory" ach ON ach."appHandle" = a."handle"
      WHERE ${categoryCondition}
      ${extraCondition}
    )
    ORDER BY snap."handle", snap."capturedAt" DESC
  `;

  const {rows} = await pool.query<AppSnapshot>(queryText, params);
  return rows;
}

/**
 * Returns apps sorted by “popularity”.
 *
 * Weighted by:
 *
 * 70 % → Recommendations
 *   More points when many apps recommend it,
 *   and even more if they show it in the first positions
 *
 * 30 % → Category rank
 *   A bonus for ranking high inside large, competitive categories.
 *
 * We scale each piece to a 0-1 range (so no one metric overwhelms the other) and combine them:
 *   popularity_score = 0.7 · rec_norm + 0.3 · cat_norm
 *
 */

export async function getAppsByPopularity(): Promise<any[]> {
  const REC_WEIGHT = 0.7;
  const CAT_WEIGHT = 0.3;

  const queryText = `
    WITH category_sizes AS (
      SELECT "categoryHandle", COUNT(DISTINCT "appHandle") AS total_apps
      FROM "CategoryAppPositionHistory"
      GROUP BY "categoryHandle"
    ),
    rank_scores AS (
      SELECT caph."appHandle",
             SUM((1.0 / (caph."position" + 1)) * LOG(cs.total_apps + 1))
             AS weighted_category_score
      FROM "CategoryAppPositionHistory" caph
      JOIN category_sizes cs ON caph."categoryHandle" = cs."categoryHandle"
      GROUP BY caph."appHandle"
    ),
    recommendation_metrics AS (
      SELECT "recommendedAppHandle" AS appHandle,
             COUNT(*)::INT AS recommendation_count,
             SUM(1.0 / ("position" + 1))::FLOAT AS weighted_recommendation_score
      FROM "RecommendedAppHistory"
      WHERE "recommendedAppHandle" IS NOT NULL
      GROUP BY "recommendedAppHandle"
    ),
    app_categories AS (
      SELECT caph."appHandle",
             ARRAY_AGG(DISTINCT ARRAY[c."name", c."handle"]) AS category_pairs
      FROM "CategoryAppPositionHistory" caph
      JOIN "Category" c ON caph."categoryHandle" = c."handle"
      GROUP BY caph."appHandle"
    ),
    latest_snapshots AS (
      SELECT DISTINCT ON (handle)
             handle, name, tagline, "reviewCount", rating
      FROM "AppSnapshot"
      ORDER BY handle, "capturedAt" DESC
    ),
    scores AS (
      SELECT a.handle,
             COALESCE(rec.weighted_recommendation_score, 0) AS weighted_recommendation_score,
             COALESCE(rs.weighted_category_score, 0) AS weighted_category_score,
             MAX(COALESCE(rec.weighted_recommendation_score, 0)) OVER () AS max_rec,
             MAX(COALESCE(rs.weighted_category_score, 0)) OVER () AS max_cat
      FROM "App" a
      LEFT JOIN recommendation_metrics rec ON a.handle = rec.appHandle
      LEFT JOIN rank_scores rs ON a.handle = rs."appHandle"
    )
    SELECT
      COALESCE(snap.name, '') AS app_name,
      COALESCE(snap.tagline, '') AS title,
      a.handle,
      COALESCE(ac.category_pairs, ARRAY[]::TEXT[][2]) AS categories,
      TO_CHAR(a."launchDate", 'MM-DD-YYYY') AS launch_date,

      (${REC_WEIGHT}) * (NULLIF(sc.weighted_recommendation_score, 0) / NULLIF(sc.max_rec, 1)) +
      (${CAT_WEIGHT}) * (NULLIF(sc.weighted_category_score, 0) / NULLIF(sc.max_cat, 1)) AS popularity_score,

      COALESCE(rec.recommendation_count, 0)::INT AS recommendation_count,
      COALESCE(snap."reviewCount", 0)::INT AS review_count,
      COALESCE(snap.rating, 0.0)::FLOAT AS rating,
      a."developerHandle" AS developer_handle,
      d."name" AS developer_name,
      COALESCE(sc.weighted_category_score, 0)::FLOAT AS weighted_category_score,
      COALESCE(sc.weighted_recommendation_score, 0)::FLOAT AS weighted_recommendation_score
    FROM scores sc
    JOIN "App" a ON sc.handle = a.handle
    LEFT JOIN "Developer" d ON a."developerHandle" = d.handle
    LEFT JOIN recommendation_metrics rec ON a.handle = rec.appHandle
    LEFT JOIN rank_scores rs ON a.handle = rs."appHandle"
    LEFT JOIN app_categories ac ON a.handle = ac."appHandle"
    LEFT JOIN latest_snapshots snap ON a.handle = snap.handle
    ORDER BY popularity_score DESC;
  `;

  const {rows} = await pool.query<AppSnapshot>(queryText, []);
  return rows;
}

export async function getCategoriesByDifficulty(): Promise<
  {
    category_handle: string;
    category_name: string;
    total_apps: number;
    average_popularity_score: number;
    total_reviews: number;
    difficulty_score: number;
  }[]
> {
  const REC_WEIGHT = 0.7;
  const CAT_WEIGHT = 0.3;

  const queryText = `
    WITH category_sizes AS (
      SELECT "categoryHandle", COUNT(DISTINCT "appHandle") AS total_apps
      FROM "CategoryAppPositionHistory"
      GROUP BY "categoryHandle"
    ),
    rank_scores AS (
      SELECT caph."appHandle",
             SUM((1.0 / (caph."position" + 1)) * LOG(cs.total_apps + 1))
             AS weighted_category_score
      FROM "CategoryAppPositionHistory" caph
      JOIN category_sizes cs ON caph."categoryHandle" = cs."categoryHandle"
      GROUP BY caph."appHandle"
    ),
    recommendation_metrics AS (
      SELECT "recommendedAppHandle" AS appHandle,
             SUM(1.0 / ("position" + 1))::FLOAT AS weighted_recommendation_score
      FROM "RecommendedAppHistory"
      WHERE "recommendedAppHandle" IS NOT NULL
      GROUP BY "recommendedAppHandle"
    ),
    scores AS (
      SELECT a.handle,
             COALESCE(rec.weighted_recommendation_score, 0) AS weighted_recommendation_score,
             COALESCE(rs.weighted_category_score, 0) AS weighted_category_score,
             MAX(COALESCE(rec.weighted_recommendation_score, 0)) OVER () AS max_rec,
             MAX(COALESCE(rs.weighted_category_score, 0)) OVER () AS max_cat
      FROM "App" a
      LEFT JOIN recommendation_metrics rec ON a.handle = rec.appHandle
      LEFT JOIN rank_scores rs ON a.handle = rs."appHandle"
    ),
    popularity_scores AS (
      SELECT handle,
             (${REC_WEIGHT}) * (NULLIF(weighted_recommendation_score, 0) / NULLIF(max_rec, 1)) +
             (${CAT_WEIGHT}) * (NULLIF(weighted_category_score, 0) / NULLIF(max_cat, 1))
             AS popularity_score
      FROM scores
    ),
    latest_snapshots AS (
      SELECT DISTINCT ON (handle) handle, "reviewCount"
      FROM "AppSnapshot"
      ORDER BY handle, "capturedAt" DESC
    ),
    app_categories AS (
      SELECT caph."appHandle", c."handle" AS category_handle, c."name" AS category_name
      FROM "CategoryAppPositionHistory" caph
      JOIN "Category" c ON caph."categoryHandle" = c."handle"
    )
    SELECT
      ac.category_handle,
      ac.category_name,
      COUNT(DISTINCT ps.handle) AS total_apps,
      AVG(ps.popularity_score) AS average_popularity_score,
      SUM(COALESCE(snap."reviewCount", 0)) AS total_reviews,
      LOG(COUNT(DISTINCT ps.handle) + 1) *
      AVG(ps.popularity_score) *
      LOG(SUM(COALESCE(snap."reviewCount", 0)) + 1) AS difficulty_score
    FROM popularity_scores ps
    JOIN app_categories ac ON ps.handle = ac."appHandle"
    LEFT JOIN latest_snapshots snap ON ps.handle = snap.handle
    GROUP BY ac.category_handle, ac.category_name
    ORDER BY difficulty_score DESC;
  `;

  const {rows} = await pool.query(queryText, []);
  return rows;
}

async function getAppsStats() {
  const apps = await getAppsByPopularity();
  if (!apps.length) {
    console.error('No data returned.');
    process.exit(1);
  }
  // Step 1: Find max number of categories on any app
  const maxCategories = Math.max(...apps.map((app) => (Array.isArray(app.categories) ? app.categories.length : 0)));
  // Step 2: Transform each app
  const transformedApps = apps.map((app) => {
    const base = {
      'App Name': `=HYPERLINK("https://apps.shopify.com/${app.handle}", "${app.app_name.replace(/"/g, '""')}")`,
      'App Title': app.title,
      Handle: app.handle,
      'Launch Date': app.launch_date,
      Developer: `=HYPERLINK("https://apps.shopify.com/partners/${app.developer_handle}", "${app.developer_name.replace(/"/g, '""')}")`,
      'Developer Handle': app.developer_handle,
      'Developer Name': app.developer_name,
      'Recommendation Count': app.recommendation_count,
      'Weighted Recommendation Score': app.weighted_recommendation_score,
      'Weighted Category Score': app.weighted_category_score,
      'Popularity Score': app.popularity_score,
      'Review Count': app.review_count,
      Rating: app.rating
    };
    // Step 3: Add up to N category columns
    const categories = Array.isArray(app.categories) ? app.categories : [];
    for (let i = 0; i < maxCategories; i++) {
      const cat = categories[i];
      base[`Category ${i + 1}`] = cat ? `=HYPERLINK("https://apps.shopify.com/categories/${cat[1]}", "${cat[0].replace(/"/g, '""')}")` : '';
    }
    return base;
  });
  // Step 4: Extract all field names (ordered)
  const fields = Object.keys(transformedApps[0]);
  const parser = new Parser({fields, quote: '"'});
  const csv = parser.parse(transformedApps);

  fs.writeFileSync('./output/apps.csv', csv, 'utf8');
}

async function getCategoriesStats() {
  const categories = await getCategoriesByDifficulty();

  const transformedCategories = categories.map((cat) => ({
    'Category Name': `=HYPERLINK("https://apps.shopify.com/categories/${cat.category_handle}/all", "${cat.category_name}")`,
    'Total Apps': cat.total_apps,
    'Average Popularity Score': cat.average_popularity_score,
    'Total reviews': cat.total_reviews,
    'Difficulty Score': cat.difficulty_score
  }));
  const fields = Object.keys(transformedCategories[0]);
  const parser = new Parser({fields, quote: '"'});
  const csv = parser.parse(transformedCategories);

  fs.writeFileSync('./output/categoriesStats.csv', csv, 'utf8');
}

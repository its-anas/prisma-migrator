import {readFileSync} from 'fs';
import {join} from 'path';
import {getDMMF} from '@prisma/sdk';
import {PrismaClient} from '@prisma/client';
import 'dotenv/config';

function log(msg: string) {
  console.log(`[${new Date().toISOString()}] ${msg}`);
}

function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function loadSchema(path: string): string {
  try {
    return readFileSync(path, 'utf-8');
  } catch (e) {
    throw new Error(`Cannot read schema at "${path}": ${(e as Error).message}`);
  }
}

async function getModels(schema: string): Promise<string[]> {
  const dmmf = await getDMMF({datamodel: schema});
  return dmmf.datamodel.models.map((m) => m.name);
}

async function createClient(url: string): Promise<PrismaClient> {
  const client = new PrismaClient({datasources: {db: {url}}});
  await client.$connect();
  return client;
}

async function migrateModel(model: string, src: PrismaClient, dst: PrismaClient) {
  log(`Starting "${model}"`);

  let rows: any[];
  try {
    rows = await (src as any)[model].findMany();
  } catch (e) {
    log(`  ❌ fetch error: ${(e as Error).message}`);
    return;
  }
  if (!rows.length) {
    log(`  • no rows`);
    return;
  }

  const BATCH_SIZE = 100;
  const chunks = chunkArray(rows, BATCH_SIZE);
  for (let i = 0; i < chunks.length; i++) {
    try {
      await (dst as any)[model].createMany({
        data: chunks[i],
        skipDuplicates: true
      });
      log(`  • batch ${i + 1}/${chunks.length} (${chunks[i].length} rows)`);
    } catch (e) {
      log(`  ❌ insert batch ${i + 1}: ${(e as Error).message}`);
    }
  }

  log(`Finished "${model}" (${rows.length} rows)`);
}

async function main() {
  const sourceUrl = process.env.SOURCE_DB_URL;
  const destUrl = process.env.DEST_DB_URL;
  if (!sourceUrl || !destUrl) {
    console.error('Set SOURCE_DB_URL and DEST_DB_URL');
    process.exit(1);
  }

  const SCHEMA_PATH = join(process.cwd(), 'prisma', 'schema.prisma');

  const rawSchema: string = loadSchema(SCHEMA_PATH);

  const models: string[] = await getModels(rawSchema);

  const srcClient = await createClient(sourceUrl);
  const dstClient = await createClient(destUrl);

  for (const model of models) {
    await migrateModel(model, srcClient, dstClient);
  }

  await Promise.all([srcClient.$disconnect(), dstClient.$disconnect()]);

  log('Migration complete');
}

main().catch((e) => {
  console.error(`Fatal: ${(e as Error).message}`);

  process.exit(1);
});

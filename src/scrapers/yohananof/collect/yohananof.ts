import { createClient } from "@supabase/supabase-js";
import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import { spawn } from "node:child_process";
import { chromium } from "playwright";

/**
 * Yohananof collector (working version):
 * - Downloads PriceFull*.gz via curl (follows redirects)
 * - Validates the downloaded content is actually GZIP (magic bytes 1F 8B)
 * - Saves under: data/yohananof/YYYY-MM-DD/STORE/PriceFull....gz
 * - Uploads ONLY valid gzip files to Supabase Storage bucket (upsert)
 * - Upserts to raw_files (schema: id, chain, store_id, file_url, storage_path, sha256, fetched_at, status, error)
 *
 * NOTE:
 * - These URLs currently return HTML (login/redirect) unless authenticated.
 *   This file will detect that and mark raw_files as failed + skip upload.
 */

type CollectorArgs = {
  supabaseUrl: string;
  serviceRoleKey: string;
  bucket: string;
  maxPages?: number; // kept for API compatibility (not used)
  maxDownloads?: number;
  urls?: string[];
};

type CollectOut = { downloaded: number };

const DEFAULT_URLS = [
  "https://url.publishedprices.co.il/file/d/PriceFull7290803800003-016-202601140010.gz",
  "https://url.publishedprices.co.il/file/d/PriceFull7290803800003-020-202601140446.gz",
];

function parsePriceFullFilenameFromUrl(url: string) {
  const u = new URL(url);
  const filename = path.basename(u.pathname); // PriceFull....gz

  // Expected: PriceFull<chainId>-<store>-<yyyymmddHHMM>.gz
  const m = filename.match(/^PriceFull(\d+)-(\d+)-(\d{12})\.gz$/i);
  if (!m) throw new Error(`Unexpected PriceFull filename format: ${filename}`);

  const chainId = m[1];
  const storeId = m[2];
  const yyyymmddhhmm = m[3];
  const yyyymmdd = yyyymmddhhmm.slice(0, 8);
  const date = `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;

  return { filename, chainId, storeId, date };
}

async function ensureDir(dir: string) {
  await fs.mkdir(dir, { recursive: true });
}

async function sha256File(filePath: string) {
  const buf = await fs.readFile(filePath);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

async function downloadToFileWithLogin(url: string, outPath: string, cookieJarPath: string) {
  await runCurlDownload(url, outPath, cookieJarPath);
}


function isGzip(buf: Buffer) {
  // gzip magic bytes: 1F 8B
  return buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b;
}

function headText(buf: Buffer, n = 300) {
  return buf.toString("utf8", 0, Math.min(n, buf.length));
}



async function upsertRawFile(
  supabase: any,
  row: {
    chain: string;
    store_id: string | null;
    file_url: string;
    storage_path: string | null;
    sha256: string | null;
    fetched_at: string;
    status: string;
    error: string | null;
  }
) {
  const { error } = await supabase
    .from("raw_files")
    .upsert(
      {
        // ❌ לא שולחים id בכלל, כדי לא לשבור FK ל-prices
        chain: row.chain,
        store_id: row.store_id,
        file_url: row.file_url,
        storage_path: row.storage_path,
        sha256: row.sha256,
        fetched_at: row.fetched_at,
        status: row.status,
        error: row.error,
      },
      { onConflict: "chain,file_url" }
    );

  if (error) {
    console.log("[WARN] raw_files upsert failed:", error.message);
  }
}

async function runCurlDownload(url: string, outPath: string, cookieJarPath: string) {
  await new Promise<void>((resolve, reject) => {
    const args = [
      "-L",
      "--fail",
      "--silent",
      "--show-error",
      "-A",
      "Mozilla/5.0",
      "-b",
      cookieJarPath,
      "-o",
      outPath,
      url,
    ];

    const p = spawn("curl", args, { stdio: "inherit", shell: true });
    p.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`curl download failed (${code}) for ${url}`))
    );
  });
}

// יוצר cookies.txt בפורמט Netscape ש-curl יודע לקרוא
async function buildCurlCookieJar(cookieJarPath: string) {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ ignoreHTTPSErrors: true }); // ✅ פותר SSL בלוגין
  const page = await ctx.newPage();

  await page.goto("https://url.publishedprices.co.il/", { waitUntil: "domcontentloaded" });

  // selectors כלליים - אם לא תופס אצלך, נתקן לפי ה-HTML שלך
  await page.fill('input[name="username"], input[type="text"]', "yohananof");

  const pw = await page.$('input[name="password"], input[type="password"]');
  if (pw) await pw.fill(""); // סיסמה ריקה

  await Promise.all([
    page.waitForNavigation({ waitUntil: "domcontentloaded" }).catch(() => {}),
    page.click('button[type="submit"], input[type="submit"]'),
  ]);

  const cookies = await ctx.cookies();
  await browser.close();

  const lines = [
    "# Netscape HTTP Cookie File",
    "# Generated by Playwright for curl",
    ...cookies.map((c) => {
      const domain = c.domain; // כולל נקודה אם צריך
      const includeSub = c.domain.startsWith(".") ? "TRUE" : "FALSE";
      const p = c.path || "/";
      const secure = c.secure ? "TRUE" : "FALSE";
      const expires = c.expires && c.expires > 0 ? Math.floor(c.expires) : 0;
      return `${domain}\t${includeSub}\t${p}\t${secure}\t${expires}\t${c.name}\t${c.value}`;
    }),
  ].join("\n");

  await fs.mkdir(path.dirname(cookieJarPath), { recursive: true });
  await fs.writeFile(cookieJarPath, lines, "utf8");
}


import { chromium, BrowserContext } from "playwright";

async function loginAndGetContext(): Promise<BrowserContext> {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ ignoreHTTPSErrors: true });
  const page = await ctx.newPage();

  await page.goto("https://url.publishedprices.co.il/", { waitUntil: "domcontentloaded" });

  await page.fill('input[name="username"], input[type="text"]', "yohananof");
  const pw = await page.$('input[name="password"], input[type="password"]');
  if (pw) await pw.fill("");

  await Promise.all([
    page.waitForNavigation({ waitUntil: "domcontentloaded" }).catch(() => {}),
    page.click('button[type="submit"], input[type="submit"]'),
  ]);

  return ctx; // שים לב: browser נשאר חי דרך ctx
}

async function discoverPriceFullUrls(ctx: BrowserContext): Promise<string[]> {
  const page = await ctx.newPage();
  await page.goto("https://url.publishedprices.co.il/file", { waitUntil: "domcontentloaded" });

  // לפעמים יש pagination / טעינה דינמית. ננסה לגלול קצת כדי להגדיל סיכוי שכל הלינקים נטענו
  for (let i = 0; i < 6; i++) {
    await page.mouse.wheel(0, 2000);
    await page.waitForTimeout(300);
  }

  const hrefs = await page.$$eval("a[href]", (as) => as.map((a) => (a as HTMLAnchorElement).href));

  const urls = hrefs
    .filter((h) => h.includes("/file/d/"))
    .filter((h) => /PriceFull\d+-\d+-\d{12}\.gz$/i.test(h));

  // הסרת כפילויות
  return Array.from(new Set(urls));
}



export async function runYohananofCollector(args: CollectorArgs): Promise<CollectOut> {
  const supabase = createClient(args.supabaseUrl, args.serviceRoleKey);

    const cookieJarPath = path.join(process.cwd(), "data", "yohananof", "cookies.txt");
  await buildCurlCookieJar(cookieJarPath);
  console.log("[AUTH] Cookie jar ready:", cookieJarPath);

  // 📌 מיקום 2: login + גילוי אוטומטי של PriceFull
  const ctx = await loginAndGetContext();
  const discovered = await discoverPriceFullUrls(ctx);
  await ctx.close();

  console.log(`[DISCOVER] Found PriceFull links: ${discovered.length}`);

  // 📌 מיקום 3: בחירת כמה להוריד
  const urls = discovered.slice(0, args.maxDownloads ?? 50);

  let downloaded = 0;
  for (const url of urls) {
    const { filename, storeId, date } = parsePriceFullFilenameFromUrl(url);

    // Local path: data/yohananof/YYYY-MM-DD/STORE/filename
    const localDir = path.join(process.cwd(), "data", "yohananof", date, storeId);
    const localPath = path.join(localDir, filename);
    await ensureDir(localDir);

    // Download (always re-download to avoid caching HTML as "good")
    await downloadToFileWithLogin(url, localPath, cookieJarPath);

    const st = await fs.stat(localPath);
    console.log(`[DL] ${filename} bytes=${st.size}`);

    const fileBuf = await fs.readFile(localPath);

    const storagePath = path.posix.join("yohananof", date, storeId, filename);
    const fetchedAt = new Date().toISOString();

    // If not gzip, mark as failed and SKIP upload
    if (!isGzip(fileBuf)) {
      const head = headText(fileBuf, 300);
      console.log("[BAD] Download is NOT gzip (likely login/redirect HTML). First 300 chars:\n", head);

      await upsertRawFile(supabase, {
        chain: "yohananof",
        store_id: storeId,
        file_url: url,
        storage_path: storagePath, // keep for debugging; optional in schema
        sha256: null,
        fetched_at: fetchedAt,
        status: "failed",
        error: "Download returned non-gzip content (likely HTML login/redirect)",
      });

      // Do not count as downloaded-success
      continue;
    }

    const sha = crypto.createHash("sha256").update(fileBuf).digest("hex");

    // Upload to bucket (upsert)
    const up = await supabase.storage.from(args.bucket).upload(storagePath, fileBuf, {
      upsert: true,
      contentType: "application/gzip",
    });

    if (up.error) {
      console.log(`[WARN] Storage upload failed for ${filename}: ${up.error.message}`);

      await upsertRawFile(supabase, {
        chain: "yohananof",
        store_id: storeId,
        file_url: url,
        storage_path: storagePath,
        sha256: sha,
        fetched_at: fetchedAt,
        status: "failed",
        error: `Storage upload failed: ${up.error.message}`,
      });

      continue;
    }

    console.log(`[OK] ${filename} store=${storeId} sha=${sha.slice(0, 10)}...`);

    await upsertRawFile(supabase, {
      chain: "yohananof",
      store_id: storeId,
      file_url: url,
      storage_path: storagePath,
      sha256: sha,
      fetched_at: fetchedAt,
      status: "downloaded",
      error: null,
    });

    downloaded += 1;
  }

  return { downloaded };
}

import crypto from "node:crypto";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";

const CHAIN = "yochananof";
const BASE = "https://url.publishedprices.co.il";

function sha256(buf: Buffer) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function basicAuthHeader(user: string, pass: string) {
  const token = Buffer.from(`${user}:${pass}`).toString("base64");
  return `Basic ${token}`;
}

/**
 * יוחננוף: PriceFull7290803800003-009-202601140501.gz
 * פורמט נפוץ: PriceFull<chain>-<subchain>-<store>-<YYYYMMDDHHMM>.gz
 */
function extractStoreIdFromFilename(nameOrUrl: string): string | null {
  // PriceFull<chain>-<sub>-<store>-YYYYMMDDHHMM.gz  => store
  let m = nameOrUrl.match(/PriceFull\d+-\d+-([0-9]{1,4})-\d{12}\.gz/i);
  if (m) return m[1].padStart(3, "0");

  // PriceFull<chain>-<store>-YYYYMMDDHHMM.gz  => store (ליתר ביטחון)
  m = nameOrUrl.match(/PriceFull\d+-([0-9]{1,4})-\d{12}\.gz/i);
  if (m) return m[1].padStart(3, "0");

  return null;
}

/**
 * מנסה למשוך לינקים מהעמוד הראשי (אם יש רשימה/קישורים).
 * אם לא נמצא — נשתמש ב-seeds.
 */
async function tryExtractLinksFromHomepage(authHeader: string): Promise<string[]> {
  const res = await fetch(`${BASE}/`, {
    headers: {
      authorization: authHeader,
      "user-agent": "SmartCartPriceCollector/1.0",
      "accept-language": "he-IL,he;q=0.9,en;q=0.8",
    },
  });

  if (!res.ok) return [];
  const html = await res.text();

  const $ = cheerio.load(html);
  const links: string[] = [];

  $("a[href]").each((_, a) => {
    const href = $(a).attr("href");
    if (!href) return;

    const isGz = /\.gz(\?|$)/i.test(href);
    const isPriceFull = /PriceFull\d+/i.test(href);
    if (!isGz || !isPriceFull) return;

    const abs = href.startsWith("http")
      ? href
      : href.startsWith("/")
        ? `${BASE}${href}`
        : `${BASE}/${href}`;

    links.push(abs);
  });

  return Array.from(new Set(links));
}

type RawFileInsert = {
  chain: string;
  store_id: string | null;
  file_url: string;
  storage_path: string | null;
  sha256: string | null;
  status: "downloaded" | "failed" | "skipped";
  error: string | null;
};

export async function runYohananofCollector(opts: {
  supabaseUrl: string;
  serviceRoleKey: string;
  bucket: string; // raw-prices
  maxDownloads?: number;

  // אם לא תביא seeds, ננסה לשלוף מה-homepage
  seedUrls?: string[];

  // פרטי התחברות
  username?: string; // default yohananof
  password?: string; // default ""
}) {
  const supabase = createClient(opts.supabaseUrl, opts.serviceRoleKey);

  const maxDownloads = opts.maxDownloads ?? 50;
  const username = opts.username ?? "yohananof";
  const password = opts.password ?? "";
  const auth = basicAuthHeader(username, password);

  // seeds מהקוד או מה-env (CSV)
  const envSeeds = (process.env.YOHANANOF_SEED_URLS ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  let links =
    (opts.seedUrls?.length ? opts.seedUrls : null) ??
    (envSeeds.length ? envSeeds : null) ??
    null;

  if (!links) {
    links = await tryExtractLinksFromHomepage(auth);
  }

  // אם עדיין אין כלום — אין מה להוריד
  if (!links.length) return { downloaded: 0, reason: "no links found (provide seedUrls or set YOHANANOF_SEED_URLS)" };

  // נעדיף PriceFull בלבד
  links = links.filter((u) => /PriceFull\d+.*\.gz/i.test(u));

  let downloaded = 0;

  for (const fileUrl of links) {
    if (downloaded >= maxDownloads) break;

    // דדופ: אם אותו file_url כבר קיים ב-raw_files, מדלגים
    const { data: existing, error: existErr } = await supabase
      .from("raw_files")
      .select("id")
      .eq("chain", CHAIN)
      .eq("file_url", fileUrl)
      .limit(1);

    if (existErr) throw existErr;
    if (existing && existing.length > 0) continue;

    let gzBuf: Buffer | null = null;
    let gzSha: string | null = null;

    let filename: string = `PriceFull_${Date.now()}.gz`;
    let storeId: string | null = null;
    let storagePath: string | null = null;

    try {
      const res = await fetch(fileUrl, {
        headers: {
          authorization: auth,
          "user-agent": "SmartCartPriceCollector/1.0",
          "accept-language": "he-IL,he;q=0.9,en;q=0.8",
        },
      });

      if (!res.ok) throw new Error(`Download HTTP ${res.status}`);

      const arr = new Uint8Array(await res.arrayBuffer());
      gzBuf = Buffer.from(arr);
      gzSha = sha256(gzBuf);

      // filename מה-URL הסופי (או content-disposition אם קיים)
      const contentDisp = res.headers.get("content-disposition") || "";
      const cdMatch = contentDisp.match(/filename="?([^"]+)"?/i);

      filename =
        cdMatch?.[1] ??
        (() => {
          const finalUrl = new URL(res.url);
          const base = finalUrl.pathname.split("/").pop();
          return base && base.toLowerCase().endsWith(".gz") ? base : `PriceFull_${Date.now()}.gz`;
        })();

      storeId =
        extractStoreIdFromFilename(filename) ??
        extractStoreIdFromFilename(res.url) ??
        extractStoreIdFromFilename(fileUrl);

      if (!storeId) {
        const row: RawFileInsert = {
          chain: CHAIN,
          store_id: null,
          file_url: fileUrl,
          storage_path: null,
          sha256: gzSha,
          status: "skipped",
          error: `no storeId in filename: ${filename}`,
        };
        await supabase.from("raw_files").insert(row);
        console.log(`[SKIP] not a store price file: ${filename}`);
        continue;
      }

      const now = new Date();
      const yyyy = now.getFullYear();
      const mm = String(now.getMonth() + 1).padStart(2, "0");
      const dd = String(now.getDate()).padStart(2, "0");

      storagePath = `${CHAIN}/${yyyy}-${mm}-${dd}/${storeId}/${filename}`;

      const up = await supabase.storage
        .from(opts.bucket)
        .upload(storagePath, gzBuf, { contentType: "application/gzip", upsert: false });

      if (up.error) throw up.error;

      const row: RawFileInsert = {
        chain: CHAIN,
        store_id: storeId,
        file_url: fileUrl,
        storage_path: storagePath,
        sha256: gzSha,
        status: "downloaded",
        error: null,
      };

      const ins = await supabase.from("raw_files").insert(row);
      if (ins.error) throw ins.error;

      downloaded++;
      console.log(`[OK] ${filename} store=${storeId} sha=${gzSha.slice(0, 10)}...`);
    } catch (e: any) {
      const row: RawFileInsert = {
        chain: CHAIN,
        store_id: storeId,
        file_url: fileUrl,
        storage_path: storagePath,
        sha256: gzSha,
        status: "failed",
        error: String(e?.message ?? e),
      };
      await supabase.from("raw_files").insert(row);
      console.log(`[FAIL] store=${storeId ?? "?"} ${row.error}`);
    }
  }

  return { downloaded };
}

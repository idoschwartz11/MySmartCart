import crypto from "node:crypto";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";

const CHAIN = "shufersal";
const BASE = "https://prices.shufersal.co.il";
const UPDATE_CATEGORY = `${BASE}/FileObject/UpdateCategory`;

function sha256(buf: Buffer) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

/**
 * שופרסל מגיעים בכמה פורמטים של Price:
 * 1) Price<chain>-<sub>-<store>-YYYYMMDD-HHMMSS.gz  (ארוך)
 * 2) Price<chain>-<store>-YYYYMMDDHHMM.gz            (קצר)
 * אנחנו רוצים להחזיר storeId בכל אחד מהם.
 */
function extractStoreIdFromFilename(nameOrUrl: string): string | null {
  // Price ארוך: Price<chain>-<sub>-<store>-YYYYMMDD-HHMMSS.gz
  let m = nameOrUrl.match(/Price\d+-\d+-([0-9]{1,4})-\d{8}-\d{6}\.gz/i);
  if (m) return m[1].padStart(3, "0");

  // Price קצר: Price<chain>-<store>-YYYYMMDDHHMM.gz
  m = nameOrUrl.match(/Price\d+-([0-9]{1,4})-\d{12}\.gz/i);
  if (m) return m[1].padStart(3, "0");

  // PriceFull ארוך: PriceFull<chain>-<sub>-<store>-YYYYMMDD-HHMMSS.gz
  m = nameOrUrl.match(/PriceFull\d+-\d+-([0-9]{1,4})-\d{8}-\d{6}\.gz/i);
  if (m) return m[1].padStart(3, "0");

  // PriceFull קצר/אחר: PriceFull<chain>-<store>-YYYYMMDDHHMM.gz
  m = nameOrUrl.match(/PriceFull\d+-([0-9]{1,4})-\d{12}\.gz/i);
  if (m) return m[1].padStart(3, "0");

  return null;
}

/**
 * מביא HTML של עמוד תוצאות (pagination)
 */
async function fetchPageHtml(page: number): Promise<string> {
  const u = new URL(UPDATE_CATEGORY);
  u.searchParams.set("catID", "0");
  u.searchParams.set("page", String(page));
  u.searchParams.set("sort", "Size");
  u.searchParams.set("sortdir", "DESC");
  u.searchParams.set("storeId", "0");

  const res = await fetch(u.toString(), {
    headers: {
      "user-agent": "SmartCartPriceCollector/1.0",
      "accept-language": "he-IL,he;q=0.9,en;q=0.8",
    },
  });

  if (!res.ok) throw new Error(`UpdateCategory HTTP ${res.status}`);
  return await res.text();
}

/**
 * מחלץ לינקים ל-Price*.gz בלבד (לא Promo)
 */
function extractPriceGzLinks(html: string): string[] {
  const $ = cheerio.load(html);
  const links: string[] = [];

  $("a[href]").each((_, a) => {
    const href = $(a).attr("href");
    if (!href) return;

    const isBlob = /blob\.core\.windows\.net/i.test(href);
    const isGz = /\.gz(\?|$)/i.test(href);

    // Price בלבד, בלי Promo
	//const isPrice = /(\/|^)Price(?:Full)?\d+/i.test(href); // Price... או PriceFull...
	const isPriceFull = /(\/|^)PriceFull\d+/i.test(href);
	const isPromo = /(\/|^)Promo\d+/i.test(href);


	if (isBlob && isGz && isPriceFull && !isPromo) {
	  links.push(href);
	}

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

export async function runShufersalCollector(opts: {
  supabaseUrl: string;
  serviceRoleKey: string;
  bucket: string; // raw-prices
  maxPages?: number;
  maxDownloads?: number;
}) {
  const supabase = createClient(opts.supabaseUrl, opts.serviceRoleKey);

  const maxPages = opts.maxPages ?? 10;
  const maxDownloads = opts.maxDownloads ?? 50;

  let downloaded = 0;

  for (let page = 1; page <= maxPages; page++) {
    const html = await fetchPageHtml(page);
    const links = extractPriceGzLinks(html);

    if (links.length === 0) break;

    for (const fileUrl of links) {
      if (downloaded >= maxDownloads) return { downloaded };

      // דדופ: אם כבר יש לנו את אותו file_url, מדלגים
      const { data: existing, error: existErr } = await supabase
        .from("raw_files")
        .select("id")
        .eq("chain", CHAIN)
        .eq("file_url", fileUrl)
        .limit(1);

      if (existErr) throw existErr;
      if (existing && existing.length > 0) continue;

      // Vars שנרצה גם ב-catch
      let gzBuf: Buffer | null = null;
      let gzSha: string | null = null;

      let filename: string = `Price_${Date.now()}.gz`;
      let storeId: string | null = null;
      let storagePath: string | null = null;

      try {
        const res = await fetch(fileUrl, {
          headers: {
            "user-agent": "SmartCartPriceCollector/1.0",
            "accept-language": "he-IL,he;q=0.9,en;q=0.8",
          },
        });

        if (!res.ok) throw new Error(`Download HTTP ${res.status}`);

        const arr = new Uint8Array(await res.arrayBuffer());
        gzBuf = Buffer.from(arr);
        gzSha = sha256(gzBuf);

        // filename מהכותרת או מה-URL הסופי אחרי redirect
        const contentDisp = res.headers.get("content-disposition") || "";
        const cdMatch = contentDisp.match(/filename="?([^"]+)"?/i);

        filename =
          cdMatch?.[1] ??
          (() => {
            const finalUrl = new URL(res.url);
            const base = finalUrl.pathname.split("/").pop();
            return base && base.toLowerCase().endsWith(".gz") ? base : `Price_${Date.now()}.gz`;
          })();

        storeId =
          extractStoreIdFromFilename(filename) ??
          extractStoreIdFromFilename(res.url) ??
          extractStoreIdFromFilename(fileUrl);

        // אם לא הצלחנו לזהות storeId, זה כנראה לא קובץ סניף - מדלגים
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

        // העלאה ל-Storage
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
  }

  return { downloaded };
}

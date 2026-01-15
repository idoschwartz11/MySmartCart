import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { XMLParser } from "fast-xml-parser";
import pako from "pako";

const CHAIN = "yohananof";
const BUCKET = "raw-prices";
const DEBUG = process.env.DEBUG_PARSE === "1";

function normalizeCanonical(name: string): string {
  return name
    .replace(/\d+(\.\d+)?\s*(גרם|גר|ג|מ"ל|מל|ליטר|ל|ק"ג|קג|יח'|יחידה|יחידות|מ"ג|מג|ml|gr|kg|l|g)/gi, "")
    .replace(/\s*[xX×]\s*\d+/g, "")
    .replace(/\d+%/g, "")
    .replace(/[()[\]{}]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function toArray<T>(x: any): T[] {
  if (!x) return [];
  return Array.isArray(x) ? x : [x];
}

function numOrNull(x: any): number | null {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

async function main() {
  const supabase = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );

  // קח קבצים שהורדו ועדיין לא פורקו
  const { data: files, error } = await supabase
    .from("raw_files")
    .select("id, storage_path, store_id")
    .eq("chain", CHAIN)
    .eq("status", "downloaded")
	.like("storage_path", "%PriceFull%")
    .order("fetched_at", { ascending: false })
    .limit(50);

  if (error) throw error;
  if (!files?.length) {
    console.log("No downloaded files to parse.");
    return;
  }

  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
  });

  for (const f of files) {
    if (!f.storage_path) continue;

    console.log("Parsing:", f.storage_path);

  // הורדה מה-Storage
  const dl = await supabase.storage.from(BUCKET).download(f.storage_path);
  if (dl.error) {
    await supabase
      .from("raw_files")
      .update({ status: "failed", error: dl.error.message })
      .eq("id", f.id);
    console.log("Download failed:", dl.error.message);
    continue;
  }

  // 🔹 רק עכשיו יש נתונים – ממירים ל-buffer
  const ab = await dl.data.arrayBuffer();
  const buf = new Uint8Array(ab);

  // 🔍 בדיקת גודל הקובץ
  console.log("Downloaded bytes from storage:", buf.length);

  // 🔍 בדיקת magic bytes של gzip (1F 8B)
  if (!(buf.length > 2 && buf[0] === 0x1f && buf[1] === 0x8b)) {
    const head = new TextDecoder("utf-8").decode(buf.slice(0, 300));
    console.log("NOT GZIP. First 300 chars:\n", head);

    await supabase
      .from("raw_files")
      .update({ status: "failed", error: "Not a gzip file (magic bytes missing)" })
      .eq("id", f.id);

    continue;
  }

  // ✅ רק אם זה gzip – ממשיכים ל-ungzip
  const xmlText = new TextDecoder("utf-8").decode(pako.ungzip(buf));


    if (DEBUG) {
      const docPeek = parser.parse(xmlText.slice(0, 5000));
      console.log("Top-level XML keys:", Object.keys(docPeek ?? {}));
      console.log("XML head:", xmlText.slice(0, 200).replace(/\s+/g, " "));
    }

    const doc = parser.parse(xmlText);

    // אצלך זה: { '?xml': ..., root: { ... } }
    const root = doc?.root ?? doc?.Root ?? null;
    if (!root) {
      await supabase.from("raw_files").update({ status: "failed", error: "XML missing root" }).eq("id", f.id);
      console.log("XML missing root");
      continue;
    }

    // שדות root: ChainId/SubChainId/StoreId (ולפעמים גרסאות אחרות)
    const chainIdStr = (root.ChainID ?? root.ChainId) != null ? String(root.ChainID ?? root.ChainId) : null;
    const subChainIdStr = (root.SubChainID ?? root.SubChainId) != null ? String(root.SubChainID ?? root.SubChainId) : null;
    const storeIdStr = (root.StoreID ?? root.StoreId) != null ? String(root.StoreID ?? root.StoreId) : null;

    const storeIdNorm = storeIdStr ? storeIdStr.padStart(3, "0") : (f.store_id ? String(f.store_id).padStart(3, "0") : null);

    if (!storeIdNorm) {
      await supabase.from("raw_files").update({ status: "failed", error: "Missing StoreId in XML" }).eq("id", f.id);
      console.log("Missing StoreId in XML");
      continue;
    }

    // עדכון raw_files עם store_id אמיתי
    await supabase.from("raw_files").update({ store_id: storeIdNorm }).eq("id", f.id);

    const bikoretNo = root.BikoretNo != null ? Number(root.BikoretNo) : null;

    // items
    const items = toArray<any>(root?.Items?.Item);
    if (!items.length) {
      await supabase.from("raw_files").update({ status: "failed", error: "No Items in XML" }).eq("id", f.id);
      console.log("No Items in XML");
      continue;
    }

    type PriceRow = {
	  raw_file_id: string;
      chain: string;
      sub_chain_id: string | null;
      store_id: string | null;
      bikoret_no: number | null;

      item_code: string | null;
      barcode: string | null;
      item_name: string;
      canonical_key: string | null;

      price: number;
      unit_qty: number | null;
      unit_of_measure: string | null;
      price_update_time: string | null;
      last_sale_datetime: string | null;

      is_weighted: boolean | null;
      qty_in_package: number | null;
    };

    const rows: PriceRow[] = [];

    for (const it of items) {
      const rawName = String(it.ItemName ?? "").trim();
      const cleanName = rawName.replace(/\s+/g, " ");
      const price = Number(it.ItemPrice);

      if (!cleanName || !Number.isFinite(price)) continue;

      const itemCode = it.ItemCode != null ? String(it.ItemCode) : null;

      // אצל שופרסל ItemCode לפעמים נראה כמו ברקוד
      const barcode = itemCode && /^\d{8,14}$/.test(itemCode) ? itemCode : null;

      const qty = numOrNull(it.Quantity);
      const uom = it.UnitOfMeasure != null ? String(it.UnitOfMeasure) : null;

      const priceUpdate = it.PriceUpdateTime != null ? String(it.PriceUpdateTime) : null;
      const lastSale = it.LastSaleDateTime != null ? String(it.LastSaleDateTime) : null;

      const isWeighted = it.bIsWeighted == null ? null : String(it.bIsWeighted) === "1";
      const qtyInPackage = it.QtyInPackage != null ? Number(it.QtyInPackage) : null;

      const canonical = normalizeCanonical(cleanName) || null;
	  
	  if (!itemCode) continue;
	  
	rows.push({
	  raw_file_id: f.id,

	  chain: CHAIN,
	  sub_chain_id: subChainIdStr,
	  store_id: storeIdNorm,
	  bikoret_no: bikoretNo,

	  item_code: itemCode,
	  barcode,
	  item_name: cleanName,
	  canonical_key: canonical,

	  price,
	  unit_qty: qty,
	  unit_of_measure: uom,
	  price_update_time: priceUpdate,
	  last_sale_datetime: lastSale,

	  is_weighted: isWeighted,
	  qty_in_package: qtyInPackage,
	});
    }

    // Inserts בבאצ'ים
    const BATCH = 500;
    for (let i = 0; i < rows.length; i += BATCH) {
      const chunk = rows.slice(i, i + BATCH);
      const ins = await supabase
	  .from("prices")
	  .upsert(chunk, { onConflict: "raw_file_id,item_code" });

      if (ins.error) {
        await supabase.from("raw_files").update({ status: "failed", error: ins.error.message }).eq("id", f.id);
        console.log("Insert failed:", ins.error.message);
        continue;
      }
    }

    await supabase.from("raw_files").update({ status: "parsed", error: null }).eq("id", f.id);
    console.log(`Parsed OK. store=${storeIdNorm} items=${rows.length} chainId=${chainIdStr ?? "?"}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

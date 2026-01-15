import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import pako from "pako";

const supabase = createClient(process.env.SUPABASE_URL!, process.env.SUPABASE_SERVICE_ROLE_KEY!);
const BUCKET = "raw-prices";

async function main() {
  const { data, error } = await supabase
    .from("raw_files")
    .select("storage_path")
    .eq("chain", "shufersal")
    .like("storage_path", "%/Promo%")
    .order("fetched_at", { ascending: false })
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (!data?.storage_path) {
    console.log("No promo file found");
    return;
  }

  const dl = await supabase.storage.from(BUCKET).download(data.storage_path);
  if (dl.error) throw dl.error;

  const ab = await dl.data.arrayBuffer();
  const xml = new TextDecoder("utf-8").decode(pako.ungzip(new Uint8Array(ab)));

  console.log("PROMO XML HEAD:\n", xml.slice(0, 800));
}

main().catch(console.error);

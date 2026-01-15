import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

async function main() {
  console.log("Scraper boot OK");

  // בדיקת חיבור מהירה
  const { data, error } = await supabase.from("raw_files").select("id").limit(1);
  if (error) {
    console.error("DB check failed:", error.message);
    process.exit(1);
  }
  console.log("DB check OK. rows:", data?.length ?? 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

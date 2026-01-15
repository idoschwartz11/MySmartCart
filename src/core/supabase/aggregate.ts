import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

async function main() {
  await supabase.rpc("refresh_product_stats_daily", { days_back: 2 });
  console.log("Aggregation done");
}

main().catch(console.error);

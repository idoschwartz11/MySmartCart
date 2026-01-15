import "dotenv/config";
import { runYohananofCollector } from "./yohananof";

async function main() {
  const SUPABASE_URL = process.env.SUPABASE_URL!;
  const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

  const out = await runYohananofCollector({
    supabaseUrl: SUPABASE_URL,
    serviceRoleKey: SERVICE_KEY,
    bucket: "raw-prices",
    maxDownloads: 20,

    // ✅ seed URL שהבאת
    seedUrls: [
      "https://url.publishedprices.co.il/file/d/PriceFull7290803800003-009-202601140501.gz",
    ],

    username: "yohananof",
    password: "",
  });

  console.log("DONE:", out);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

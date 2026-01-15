import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { spawn } from "node:child_process";
import { runYohananofCollector } from "./collect/yohananof";

function runCmd(cmd: string, args: string[]) {
  return new Promise<void>((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: "inherit", shell: true });
    p.on("exit", (code) =>
      code === 0 ? resolve() : reject(new Error(`${cmd} ${args.join(" ")} exited ${code}`))
    );
  });
}

async function main() {
  const SUPABASE_URL = process.env.SUPABASE_URL!;
  const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

  console.log("=== STEP 1: COLLECT (Yohananof PriceFull) ===");
  const out = await runYohananofCollector({
    supabaseUrl: SUPABASE_URL,
    serviceRoleKey: SERVICE_KEY,
    bucket: "raw-prices",
    maxPages: 50,
    maxDownloads: 50,
  });
  console.log("Collected:", out);

  console.log("\n=== STEP 2: PARSE (PriceFull -> prices) ===");
  await runCmd("npx", ["tsx", "src/scrapers/yohananof/parse/parse_yohananof.ts"]);

  // אם עדיין לא יצרת את הפונקציה ב-Supabase, תשאיר false
  const RUN_AGGREGATE = true;

  if (RUN_AGGREGATE) {
    console.log("\n=== STEP 3: AGGREGATE DAILY ===");
    const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

    const { error } = await supabase.rpc("refresh_product_stats_daily", { days_back: 2 });
    if (error) {
      console.log("Aggregate skipped (RPC error):", error.message);
    } else {
      console.log("Aggregation OK ✅");
    }
  } else {
    console.log("\n=== STEP 3: AGGREGATE DAILY (skipped) ===");
  }

  console.log("\nALL DONE ✅");
}

main().catch((e) => {
  console.error("run_all failed:", e);
  process.exit(1);
});

import "dotenv/config";
import { runShufersalCollector } from "./shufersal";

async function main() {
	
	console.log("URL:", process.env.SUPABASE_URL);
	console.log("KEY prefix:", process.env.SUPABASE_SERVICE_ROLE_KEY?.slice(0, 12));
  const SUPABASE_URL = process.env.SUPABASE_URL!;
  const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

  const out = await runShufersalCollector({
    supabaseUrl: SUPABASE_URL,
    serviceRoleKey: SERVICE_KEY,
    bucket: "raw-prices",
    maxPages: 50,
    maxDownloads: 50,
  });

  console.log("DONE:", out);
}

main().catch((e) => {
	
	
  console.error(e);
  process.exit(1);
});

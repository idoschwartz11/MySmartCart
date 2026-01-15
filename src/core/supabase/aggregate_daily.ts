import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

async function main() {
  const supabase = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );

  // מחשב סטטיסטיקה ליום הנוכחי (לפי price_update_time אם יש)
  const sql = `
    insert into public.product_stats_daily
      (day, chain, canonical_key, avg_price, sample_count, min_price, max_price)
    select
      (coalesce(price_update_time, fetched_at))::date as day,
      chain,
      canonical_key,
      round(avg(price)::numeric, 2) as avg_price,
      count(*) as sample_count,
      min(price) as min_price,
      max(price) as max_price
    from public.prices
    where canonical_key is not null
      and coalesce(price_update_time, fetched_at) >= now() - interval '2 days'
    group by 1,2,3
    on conflict (day, chain, canonical_key)
    do update set
      avg_price = excluded.avg_price,
      sample_count = excluded.sample_count,
      min_price = excluded.min_price,
      max_price = excluded.max_price;
  `;

  const { error } = await supabase.rpc("execute_sql", { sql }); // אם אין לך RPC כזה, נעשה דרך SQL Editor
  if (error) {
    console.log("RPC not available. Run the SQL in Supabase SQL Editor instead.");
    console.log(sql);
    return;
  }

  console.log("Aggregated OK");
}

main().catch(console.error);

import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "./weather.module.css";

export const metadata: Metadata = { title: "Weather" };
export const dynamic = "force-dynamic";

type Row = {
  visit_id: string;
  scheduled_date: string;
  client_name: string | null;
  property_address: string | null;
  crew_name: string | null;
  weather_sensitive: boolean;
  precip_pct: number | null;
  wind_mph: number | null;
  temp_high_f: number | null;
  summary: string | null;
  fetched_at: string | null;
  alert_id: string | null;
  alert_status: string | null;
  reasons: string[] | null;
};

type Settings = {
  enabled: boolean;
  rain_chance_pct: number;
  wind_mph: number;
  min_temp_f: number | null;
  max_temp_f: number | null;
  lookahead_days: number;
};

const DEFAULTS: Settings = { enabled: false, rain_chance_pct: 60, wind_mph: 25, min_temp_f: 35, max_temp_f: 100, lookahead_days: 3 };
const REASON: Record<string, string> = { rain: "Rain", wind: "Wind", heat: "Heat", cold: "Cold" };

function dayLabel(ymd: string) {
  return new Date(`${ymd}T12:00:00Z`).toLocaleDateString("en-US", { weekday: "short", month: "short", day: "numeric", timeZone: "UTC" });
}
function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
const back = (q: string) => redirect(`/weather?${q}`);

async function handle(formData: FormData) {
  "use server";
  const action = String(formData.get("action") ?? "");
  const date = String(formData.get("new_date") ?? "");
  const { error } = await (await supabaseServer()).rpc("handle_weather_alert", {
    p_alert_id: String(formData.get("alert_id") ?? ""),
    p_action: action,
    p_new_date: action === "move" && date ? date : (null as unknown as string),
    p_note: String(formData.get("note") ?? "") || (null as unknown as string),
  });
  back(error ? `error=${encodeURIComponent(friendlyError(error))}` : `done=${action}`);
}

async function saveSettings(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const num = (k: string) => {
    const v = String(formData.get(k) ?? "").trim();
    return v === "" ? null : Number(v);
  };
  const { error } = await (await supabaseServer()).from("weather_settings").upsert(
    {
      tenant_id: company.tenant_id,
      enabled: formData.get("enabled") === "on",
      rain_chance_pct: num("rain_chance_pct") ?? DEFAULTS.rain_chance_pct,
      wind_mph: num("wind_mph") ?? DEFAULTS.wind_mph,
      min_temp_f: num("min_temp_f"),
      max_temp_f: num("max_temp_f"),
      lookahead_days: num("lookahead_days") ?? DEFAULTS.lookahead_days,
    },
    { onConflict: "tenant_id" },
  );
  back(error ? `error=${encodeURIComponent(error.message.includes("check") ? "Check the numbers: they're outside the allowed range." : friendlyError(error))}` : "done=settings");
}

async function saveServices(formData: FormData) {
  "use server";
  const supabase = await supabaseServer();
  const ids = formData.getAll("service_id").map(String);
  const on = new Set(formData.getAll("sensitive").map(String));
  for (const id of ids) {
    const { error } = await supabase.from("services").update({ weather_sensitive: on.has(id) }).eq("id", id);
    if (error) back(`error=${encodeURIComponent(friendlyError(error))}`);
  }
  back("done=services");
}

const DONE: Record<string, string> = {
  services: "Services updated.",
  move: "Visit moved. It's in the customer's history.",
  acknowledge: "Marked as seen. It stays on the list until the day passes.",
  dismiss: "Dismissed. It won't come back for that day.",
  settings: "Weather settings saved.",
};

export default async function WeatherPage({ searchParams }: { searchParams: Promise<{ error?: string; done?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const supabase = await supabaseServer();
  const [{ data: rowsData, error }, { data: s }, { data: svc }] = await Promise.all([
    supabase.rpc("weather_outlook", { p_tenant_id: company.tenant_id }),
    supabase.from("weather_settings").select("enabled, rain_chance_pct, wind_mph, min_temp_f, max_temp_f, lookahead_days")
      .eq("tenant_id", company.tenant_id).maybeSingle(),
    supabase.from("services").select("id, name, weather_sensitive").eq("tenant_id", company.tenant_id).eq("active", true).order("name"),
  ]);
  const services = (svc ?? []) as { id: string; name: string; weather_sensitive: boolean }[];
  const settings: Settings = (s as Settings | null) ?? DEFAULTS;
  const rows = (rowsData ?? []) as Row[];
  const open = rows.filter((r) => r.alert_id && (r.alert_status === "open" || r.alert_status === "acknowledged"));
  const fetched = rows.map((r) => r.fetched_at).filter(Boolean).sort().at(-1);
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div>
          <h1>Weather</h1>
          <p className={styles.lede}>
            {settings.enabled
              ? `Checking the National Weather Service forecast for the next ${settings.lookahead_days} days of visits. Nothing moves on its own; you decide.`
              : "Weather checks are off. Turn them on below to flag visits on rainy, windy, very hot or very cold days."}
            {settings.enabled && (fetched
              ? ` Last forecast ${new Date(fetched).toLocaleString("en-US", { timeZone: company.timezone, weekday: "short", hour: "numeric", minute: "2-digit" })}.`
              : " No forecast has been pulled yet.")}
          </p>
        </div>
      </header>

      {sp.done && <p className="notice" role="status">{DONE[sp.done] ?? "Saved."}</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {error && <p className="error-text" role="alert">{friendlyError(error)}</p>}

      <section aria-labelledby="needs">
        <h2 id="needs">Needs a decision</h2>
        {open.length === 0 ? (
          <p className={styles.muted}>{settings.enabled ? "No weather problems in the forecast." : "Nothing to review."}</p>
        ) : (
          <ul className={styles.alerts}>
            {open.map((r) => (
              <li key={r.alert_id} className={`${styles.alert} ${r.alert_status === "acknowledged" ? styles.ack : ""}`}>
                <div className={styles.alertTop}>
                  <div>
                    <p className={styles.when}>{dayLabel(r.scheduled_date)}</p>
                    <p>
                      <Link href={`/schedule?week=${r.scheduled_date}`}>{r.client_name ?? "Visit"}</Link>
                      {r.property_address ? <span className={styles.muted}>, {r.property_address}</span> : null}
                      {r.crew_name ? <span className={styles.muted}> · {r.crew_name}</span> : null}
                    </p>
                  </div>
                  <p>
                    <span className={styles.why}>{(r.reasons ?? []).map((x) => REASON[x] ?? x).join(" + ")}</span>
                    <span className={styles.muted}>
                      {" "}· {r.precip_pct ?? 0}% rain · {r.wind_mph ?? "–"} mph · high {r.temp_high_f ?? "–"}°
                      {r.summary ? ` · ${r.summary}` : ""}
                    </span>
                  </p>
                </div>
                <form action={handle} className={styles.actions}>
                  <input type="hidden" name="alert_id" value={r.alert_id!} />
                  <div className="field">
                    <label htmlFor={`d-${r.alert_id}`}>Move to</label>
                    <input id={`d-${r.alert_id}`} name="new_date" type="date" min={today} defaultValue={addDays(r.scheduled_date, 1)} className="input" />
                  </div>
                  <input type="hidden" name="note" value="" />
                  <button className="button" name="action" value="move">Move visit</button>
                  {r.alert_status === "open" && <button className="button quiet" name="action" value="acknowledge">Keep, mark seen</button>}
                  <button className="button quiet" name="action" value="dismiss">Dismiss</button>
                </form>
              </li>
            ))}
          </ul>
        )}
      </section>

      {rows.length > 0 && (
        <section aria-labelledby="outlook">
          <h2 id="outlook">Upcoming visits</h2>
          <table className={styles.table}>
            <thead>
              <tr>
                <th>Day</th>
                <th>Customer</th>
                <th className={styles.num}>Rain</th>
                <th className={styles.num}>Wind</th>
                <th className={styles.num}>High</th>
                <th>Forecast</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.visit_id}>
                  <td>{dayLabel(r.scheduled_date)}</td>
                  <td>
                    {r.client_name ?? "–"}
                    {!r.weather_sensitive && <span className={styles.muted}> (weather doesn't matter)</span>}
                  </td>
                  <td className={styles.num}>{r.precip_pct == null ? "–" : `${r.precip_pct}%`}</td>
                  <td className={styles.num}>{r.wind_mph == null ? "–" : `${r.wind_mph} mph`}</td>
                  <td className={styles.num}>{r.temp_high_f == null ? "–" : `${r.temp_high_f}°`}</td>
                  <td>
                    {r.summary ?? <span className={styles.muted}>Not pulled yet</span>}
                    {r.alert_status && <span className={styles.flag}> · {r.alert_status === "moved" ? "moved" : r.alert_status}</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}

      <section>
        <details className={styles.settings} open={!settings.enabled}>
          <summary>Weather rules for {company.name}</summary>
          <form action={saveSettings} className={styles.settingsForm}>
            <label className={styles.check}>
              <input type="checkbox" name="enabled" defaultChecked={settings.enabled} /> Check the weather
            </label>
            <div className="field">
              <label htmlFor="rain_chance_pct">Flag rain at (%)</label>
              <input id="rain_chance_pct" name="rain_chance_pct" type="number" min={1} max={100} defaultValue={settings.rain_chance_pct} className="input" />
            </div>
            <div className="field">
              <label htmlFor="wind_mph">Flag wind at (mph)</label>
              <input id="wind_mph" name="wind_mph" type="number" min={5} max={150} defaultValue={settings.wind_mph} className="input" />
            </div>
            <div className="field">
              <label htmlFor="max_temp_f">Flag heat at (°F)</label>
              <input id="max_temp_f" name="max_temp_f" type="number" min={40} max={140} defaultValue={settings.max_temp_f ?? ""} className="input" />
            </div>
            <div className="field">
              <label htmlFor="min_temp_f">Flag highs below (°F)</label>
              <input id="min_temp_f" name="min_temp_f" type="number" min={-40} max={120} defaultValue={settings.min_temp_f ?? ""} className="input" />
            </div>
            <div className="field">
              <label htmlFor="lookahead_days">Days ahead</label>
              <select id="lookahead_days" name="lookahead_days" defaultValue={settings.lookahead_days} className="select">
                {[1, 2, 3, 4, 5, 6, 7].map((n) => <option key={n} value={n}>{n}</option>)}
              </select>
            </div>
            <button className="button" type="submit">Save rules</button>
          </form>
          {services.length > 0 && (
            <form action={saveServices} className={styles.services}>
              <p><strong>Weather matters for:</strong></p>
              {services.map((v) => (
                <label key={v.id} className={styles.check}>
                  <input type="hidden" name="service_id" value={v.id} />
                  <input type="checkbox" name="sensitive" value={v.id} defaultChecked={v.weather_sensitive} /> {v.name}
                </label>
              ))}
              <button className="button quiet" type="submit">Save services</button>
            </form>
          )}
          <p className={styles.muted}>
            Unchecked services are never flagged. Forecasts come from the National Weather Service and cover the US.
          </p>
        </details>
      </section>
    </div>
  );
}

import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { friendlyError, googleRouteUrl, routingProvider, type GeoPoint, type NavTarget } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import { geocodeUS, oneLine } from "@/lib/geocode";
import styles from "./routes.module.css";

export const metadata: Metadata = { title: "Routes" };
export const dynamic = "force-dynamic";

type Stop = {
  visit_id: string;
  status: "scheduled" | "in_progress" | "completed" | "skipped" | "canceled";
  sort_order: number;
  job_title: string;
  client_name: string | null;
  property_id: string | null;
  address: string | null;
  latitude: number | null;
  longitude: number | null;
  crew_id: string | null;
  crew_name: string | null;
  est_minutes: number | null;
};
type Plan = { crew_id: string; provider: string; road_based: boolean; est_drive_minutes: number | null; est_drive_miles: number | null; planned_at: string };
type Yard = { id: string; name: string; address_line1: string | null; city: string | null; region: string | null; postal_code: string | null; latitude: number | null; longitude: number | null };

const STARTED = new Set(["in_progress", "completed", "skipped"]);
const valid = (s?: string) => (s && /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : undefined);
const go = (date: string, q = ""): never => redirect(`/routes?date=${date}${q ? `&${q}` : ""}`);
const fail = (date: string, e: unknown): never => go(date, `error=${encodeURIComponent(friendlyError(e))}`);

function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
const point = (s: { latitude: number | null; longitude: number | null }): GeoPoint | null =>
  s.latitude != null && s.longitude != null ? { lat: s.latitude, lon: s.longitude } : null;

async function load(tenant: string, date: string) {
  const supabase = await supabaseServer();
  const [{ data: stops, error }, { data: plans }, { data: yard }] = await Promise.all([
    supabase.rpc("schedule", { p_tenant_id: tenant, p_from: date, p_to: date }),
    supabase.from("route_plans").select("crew_id, provider, road_based, est_drive_minutes, est_drive_miles, planned_at").eq("tenant_id", tenant).eq("route_date", date),
    supabase.from("yards").select("id, name, address_line1, city, region, postal_code, latitude, longitude").eq("tenant_id", tenant).eq("is_default", true).eq("active", true).maybeSingle(),
  ]);
  const all = ((stops ?? []) as Stop[]).filter((s) => s.status !== "canceled");
  return { supabase, error, stops: all, plans: (plans ?? []) as Plan[], yard: yard as Yard | null };
}

async function move(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const date = String(formData.get("date"));
  const crew = String(formData.get("crew_id"));
  const id = String(formData.get("visit_id"));
  const dir = Number(formData.get("dir"));
  const { supabase, stops } = await load(company.tenant_id, date);
  const ids = stops.filter((s) => s.crew_id === crew).map((s) => s.visit_id);
  const i = ids.indexOf(id);
  const j = i + dir;
  if (i < 0 || j < 0 || j >= ids.length) go(date);
  [ids[i], ids[j]] = [ids[j]!, ids[i]!];
  const { error } = await supabase.rpc("set_route_order", { p_tenant_id: company.tenant_id, p_crew_id: crew, p_date: date, p_visit_ids: ids });
  if (error) fail(date, error);
  go(date);
}

async function optimize(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const date = String(formData.get("date"));
  const crew = String(formData.get("crew_id"));
  const { supabase, stops, yard } = await load(company.tenant_id, date);
  const mine = stops.filter((s) => s.crew_id === crew);
  // Stops already started or finished keep their place; only what's left is planned.
  const started = mine.filter((s) => STARTED.has(s.status));
  const remaining = mine.filter((s) => !STARTED.has(s.status));
  const yardPoint = yard ? point(yard) : null;
  const lastDone = [...started].reverse().find((s) => point(s));
  const provider = routingProvider(process.env.ROUTING_PROVIDER);
  const plan = await provider.plan({
    start: lastDone ? point(lastDone) : yardPoint,
    end: yardPoint,
    stops: remaining.map((s) => ({ id: s.visit_id, point: point(s), serviceMinutes: s.est_minutes })),
  });
  const { error } = await supabase.rpc("set_route_order", {
    p_tenant_id: company.tenant_id, p_crew_id: crew, p_date: date,
    p_visit_ids: [...started.map((s) => s.visit_id), ...plan.order],
    p_provider: plan.provider, p_road_based: plan.roadBased,
    p_est_drive_minutes: plan.driveMinutes as number, p_est_drive_miles: plan.driveMiles as number,
    p_start_yard_id: yard?.id ?? (null as unknown as string),
  });
  if (error) fail(date, error);
  go(date, plan.unplaced.length ? `notice=unplaced&n=${plan.unplaced.length}` : "notice=optimized");
}

async function locate(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const date = String(formData.get("date"));
  const supabase = await supabaseServer();
  const { data: props } = await supabase.from("properties")
    .select("id, address_line1, city, region, postal_code")
    .eq("tenant_id", company.tenant_id).in("id", formData.getAll("property_id").map(String)).is("latitude", null);
  let found = 0;
  for (const p of props ?? []) {
    const c = await geocodeUS(oneLine(p));
    if (!c) continue;
    const { error } = await supabase.from("properties")
      .update({ latitude: c.latitude, longitude: c.longitude, geocode_source: "census", geocoded_at: new Date().toISOString() })
      .eq("id", p.id).is("latitude", null);
    if (!error) found++;
  }
  go(date, `notice=located&n=${found}&of=${(props ?? []).length}`);
}

async function saveYard(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const date = String(formData.get("date"));
  const f = (k: string) => String(formData.get(k) ?? "").trim() || null;
  const fields = { name: f("name") ?? "Yard", address_line1: f("address_line1"), city: f("city"), region: f("region"), postal_code: f("postal_code") };
  const c = await geocodeUS(oneLine(fields));
  const supabase = await supabaseServer();
  const id = f("yard_id");
  const row = { ...fields, latitude: c?.latitude ?? null, longitude: c?.longitude ?? null };
  const { error } = id
    ? await supabase.from("yards").update(row).eq("id", id)
    : await supabase.from("yards").insert({ ...row, tenant_id: company.tenant_id, is_default: true });
  if (error) fail(date, error);
  go(date, c ? "notice=yard" : "notice=yard_unlocated");
}

const NOTICE: Record<string, (n: string, of: string) => string> = {
  optimized: () => "Stops reordered for the shortest straight-line drive. Crews see the new order right away.",
  unplaced: (n) => `Reordered. ${n} stop(s) have no map location yet and were put at the end.`,
  located: (n, of) => `Found map locations for ${n} of ${of} address(es).`,
  yard: () => "Yard saved. Routes start and end there.",
  yard_unlocated: () => "Yard saved, but the address couldn't be found on the map. Check it and save again.",
};

export default async function RoutesPage({ searchParams }: { searchParams: Promise<Record<string, string | undefined>> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const date = valid(sp.date) ?? today;
  const { stops, plans, yard, error } = await load(company.tenant_id, date);

  const crews = new Map<string, { id: string | null; name: string; stops: Stop[] }>();
  for (const s of stops) {
    const key = s.crew_id ?? "none";
    const c = crews.get(key) ?? { id: s.crew_id, name: s.crew_name ?? "No crew yet", stops: [] };
    c.stops.push(s);
    crews.set(key, c);
  }
  const label = new Date(`${date}T12:00:00Z`).toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", timeZone: "UTC" });

  return (
    <div className={styles.page}>
      <header className={styles.header}>
        <div>
          <h1>Routes</h1>
          <p className={styles.muted}>{label}</p>
        </div>
        <nav className={styles.dayNav} aria-label="Day">
          <Link className="button quiet" href={`/routes?date=${addDays(date, -1)}`}>Previous day</Link>
          {date !== today && <Link className="button quiet" href="/routes">Today</Link>}
          <Link className="button quiet" href={`/routes?date=${addDays(date, 1)}`}>Next day</Link>
        </nav>
      </header>

      {sp.notice && NOTICE[sp.notice] && <p className="notice" role="status">{NOTICE[sp.notice]!(sp.n ?? "0", sp.of ?? "0")}</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}
      {error && <p className="error-text" role="alert">{friendlyError(error)}</p>}

      {crews.size === 0 && <p className={styles.muted}>No visits on this day. <Link href={`/schedule?week=${date}`}>Open the schedule</Link>.</p>}

      <div className={styles.crews}>
        {[...crews.values()].map((c) => {
          const plan = plans.find((p) => p.crew_id === c.id);
          const left = c.stops.filter((s) => !STARTED.has(s.status));
          const nav: NavTarget[] = left.map((s) => point(s) ?? s.address ?? "").filter((x) => x !== "");
          const navUrl = googleRouteUrl(nav);
          const missing = c.stops.filter((s) => !point(s) && s.property_id);
          return (
            <section key={c.id ?? "none"} className={styles.crew} aria-labelledby={`crew-${c.id ?? "none"}`}>
              <div className={styles.crewTop}>
                <h2 id={`crew-${c.id ?? "none"}`}>{c.name}</h2>
                <p className={styles.muted}>
                  {c.stops.length} stop{c.stops.length === 1 ? "" : "s"}
                  {plan?.est_drive_miles != null &&
                    ` · about ${plan.est_drive_miles} mi, ${plan.est_drive_minutes} min driving${plan.road_based ? "" : " (straight-line estimate)"}`}
                </p>
              </div>
              <ol className={styles.stops}>
                {c.stops.map((s, i) => (
                  <li key={s.visit_id} className={STARTED.has(s.status) ? styles.doneStop : undefined}>
                    <span className={styles.n}>{i + 1}</span>
                    <span className={styles.what}>
                      <strong>{s.client_name ?? s.job_title}</strong>
                      <span className={styles.muted}>{s.address ?? "No address"}{!point(s) && s.address ? " · not on map yet" : ""}</span>
                      {STARTED.has(s.status) && <span className={styles.muted}>{s.status.replace("_", " ")}</span>}
                    </span>
                    {c.id && (
                      <form action={move} className={styles.moveBtns}>
                        <input type="hidden" name="date" value={date} />
                        <input type="hidden" name="crew_id" value={c.id} />
                        <input type="hidden" name="visit_id" value={s.visit_id} />
                        <button name="dir" value="-1" disabled={i === 0} aria-label={`Move ${s.client_name ?? "stop"} earlier`}>↑</button>
                        <button name="dir" value="1" disabled={i === c.stops.length - 1} aria-label={`Move ${s.client_name ?? "stop"} later`}>↓</button>
                      </form>
                    )}
                  </li>
                ))}
              </ol>
              <div className={styles.crewActions}>
                {c.id && left.length > 1 && (
                  <form action={optimize}>
                    <input type="hidden" name="date" value={date} />
                    <input type="hidden" name="crew_id" value={c.id} />
                    <button className="button" type="submit">Suggest best order</button>
                  </form>
                )}
                {navUrl && <a className="button quiet" href={navUrl} target="_blank" rel="noreferrer">Open in Google Maps</a>}
                {missing.length > 0 && (
                  <form action={locate}>
                    <input type="hidden" name="date" value={date} />
                    {missing.map((s) => <input key={s.visit_id} type="hidden" name="property_id" value={s.property_id!} />)}
                    <button className="button quiet" type="submit">Find {missing.length} address{missing.length === 1 ? "" : "es"} on the map</button>
                  </form>
                )}
                {!c.id && <p className={styles.muted}>Assign these to a crew on the <Link href={`/schedule?week=${date}`}>schedule</Link> to route them.</p>}
              </div>
            </section>
          );
        })}
      </div>

      <details className={styles.yard} open={!yard}>
        <summary>{yard ? `Start and end: ${yard.name}${yard.address_line1 ? `, ${yard.address_line1}` : ""}` : "Set where crews start the day"}</summary>
        <form action={saveYard} className={styles.yardForm}>
          <input type="hidden" name="date" value={date} />
          {yard && <input type="hidden" name="yard_id" value={yard.id} />}
          <div className="field"><label htmlFor="y-name">Name</label><input id="y-name" name="name" defaultValue={yard?.name ?? "Yard"} className="input" /></div>
          <div className="field"><label htmlFor="y-a1">Street</label><input id="y-a1" name="address_line1" required defaultValue={yard?.address_line1 ?? ""} className="input" /></div>
          <div className="field"><label htmlFor="y-city">City</label><input id="y-city" name="city" defaultValue={yard?.city ?? ""} className="input" /></div>
          <div className="field"><label htmlFor="y-reg">State</label><input id="y-reg" name="region" defaultValue={yard?.region ?? ""} className="input" /></div>
          <div className="field"><label htmlFor="y-zip">ZIP</label><input id="y-zip" name="postal_code" defaultValue={yard?.postal_code ?? ""} className="input" /></div>
          <button className="button" type="submit">Save yard</button>
        </form>
        {yard && !point(yard) && <p className="error-text">This address isn&apos;t on the map yet, so routes start at the first stop.</p>}
      </details>
      <p className={styles.muted}>
        Suggested orders use straight-line distance, which is free and works well for most local routes. Road-based routing and live traffic can be switched on later.
      </p>
    </div>
  );
}

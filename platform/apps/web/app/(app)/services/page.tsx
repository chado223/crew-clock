import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { formatMoney, friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";
import styles from "../money.module.css";

export const metadata: Metadata = { title: "Services" };
export const dynamic = "force-dynamic";

const fields = (f: FormData) => {
  const price = Number(f.get("default_price") ?? "");
  const minutes = Number(f.get("default_minutes") ?? "");
  return {
    name: String(f.get("name") ?? "").trim(),
    default_price: f.get("default_price") && Number.isFinite(price) && price >= 0 ? Math.round(price * 100) / 100 : null,
    default_minutes: f.get("default_minutes") && Number.isFinite(minutes) && minutes > 0 ? Math.round(minutes) : null,
    weather_sensitive: f.get("weather_sensitive") === "on",
  };
};
const done = (q: string): never => { revalidatePath("/services"); redirect(`/services?${q}`); };

async function add(formData: FormData) {
  "use server";
  const { company } = await currentCompany();
  const row = fields(formData);
  if (!row.name) done(`error=${encodeURIComponent("Name the service.")}`);
  const { error } = await (await supabaseServer()).from("services").insert({ ...row, tenant_id: company.tenant_id });
  done(error ? `error=${encodeURIComponent(error.code === "23505" ? "There's already a service with that name." : friendlyError(error))}` : "saved=1");
}

async function save(formData: FormData) {
  "use server";
  const row = fields(formData);
  if (!row.name) done(`error=${encodeURIComponent("Name the service.")}`);
  const { error } = await (await supabaseServer()).from("services")
    .update({ ...row, ...(formData.get("active") ? { active: formData.get("active") === "on" } : {}) }).eq("id", String(formData.get("id")));
  done(error ? `error=${encodeURIComponent(error.code === "23505" ? "There's already a service with that name." : friendlyError(error))}` : "saved=1");
}

export default async function ServicesPage({ searchParams }: { searchParams: Promise<{ error?: string; saved?: string }> }) {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const sp = await searchParams;
  const { data } = await (await supabaseServer()).from("services")
    .select("id, name, default_price, default_minutes, weather_sensitive, active").eq("tenant_id", company.tenant_id).order("active", { ascending: false }).order("name");
  const services = (data ?? []) as { id: string; name: string; default_price: number | null; default_minutes: number | null; weather_sensitive: boolean; active: boolean }[];

  return (
    <div className={styles.page}>
      <header>
        <h1>Services</h1>
        <p className={styles.empty}>What you sell. A job can use a service so its price and time fill in, and profit can be compared by service.</p>
      </header>
      {sp.saved && <p className="notice" role="status">Saved.</p>}
      {sp.error && <p className="error-text" role="alert">{sp.error}</p>}

      <section className={styles.panel} aria-labelledby="add">
        <h2 id="add">Add a service</h2>
        <form action={add} className={styles.form}>
          <div className="field"><label htmlFor="n">Name</label><input id="n" name="name" required maxLength={120} className="input" placeholder="e.g. Mow & edge" /></div>
          <div className="field"><label htmlFor="p">Usual price ($)</label><input id="p" name="default_price" type="number" min={0} step="0.01" className="input" /></div>
          <div className="field"><label htmlFor="m">Usual minutes on site</label><input id="m" name="default_minutes" type="number" min={1} className="input" /></div>
          <label className={styles.inline}><input type="checkbox" name="weather_sensitive" defaultChecked /> Weather matters</label>
          <button className="button" type="submit">Add service</button>
        </form>
      </section>

      {services.length === 0 ? <p className={styles.empty}>No services yet.</p> : (
        <ul className={styles.list}>
          {services.map((s) => (
            <li key={s.id}>
              <details>
                <summary className={styles.row}>
                  <span>{s.name}{!s.active ? " (retired)" : ""}</span>
                  <span>{s.default_minutes ? `${s.default_minutes} min` : ""}</span>
                  <span>{s.weather_sensitive ? "" : "Any weather"}</span>
                  <span className={styles.num}>{s.default_price != null ? formatMoney(s.default_price) : ""}</span>
                </summary>
                <form action={save} className={styles.form}>
                  <input type="hidden" name="id" value={s.id} />
                  <div className="field"><label htmlFor={`n-${s.id}`}>Name</label><input id={`n-${s.id}`} name="name" required defaultValue={s.name} className="input" /></div>
                  <div className="field"><label htmlFor={`p-${s.id}`}>Usual price ($)</label><input id={`p-${s.id}`} name="default_price" type="number" min={0} step="0.01" defaultValue={s.default_price ?? ""} className="input" /></div>
                  <div className="field"><label htmlFor={`m-${s.id}`}>Usual minutes</label><input id={`m-${s.id}`} name="default_minutes" type="number" min={1} defaultValue={s.default_minutes ?? ""} className="input" /></div>
                  <label className={styles.inline}><input type="checkbox" name="weather_sensitive" defaultChecked={s.weather_sensitive} /> Weather matters</label>
                  <button className="button" type="submit">Save</button>
                  <button className="button quiet" type="submit" name="active" value={s.active ? "off" : "on"}>{s.active ? "Retire" : "Bring back"}</button>
                </form>
                <p className="hint">Changing a usual price doesn&apos;t change existing jobs; edit a job to change its price.</p>
              </details>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

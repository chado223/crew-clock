"use client";

import { useState, useTransition } from "react";
import Link from "next/link";
import { mapImport, IMPORT_COLUMNS, type MappedImport, parseCsv } from "@crew/shared";
import { runImport, type ImportResult } from "./actions";
import styles from "./import.module.css";

const LABEL: Record<string, string> = {
  name: "Name", email: "Email", phone: "Phone", company_name: "Company", kind: "Type", status: "Status",
  lead_source: "Lead source", tags: "Tags", notes: "Notes", address_line1: "Street address", city: "City",
  region: "State", postal_code: "ZIP", access_notes: "Access notes", lawn_sqft: "Lawn size", service: "Service",
  price: "Price", frequency: "Frequency", day: "Day", start_date: "Start date", crew: "Crew",
};

export function Importer() {
  const [file, setFile] = useState<string | null>(null);
  const [mapped, setMapped] = useState<MappedImport | null>(null);
  const [preview, setPreview] = useState<ImportResult | null>(null);
  const [done, setDone] = useState<ImportResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [pending, start] = useTransition();

  async function onFile(f: File | undefined) {
    setError(null); setPreview(null); setDone(null); setMapped(null);
    if (!f) return;
    if (f.size > 5_000_000) { setError("That file is over 5 MB. Split it into smaller files."); return; }
    const m = mapImport(parseCsv(await f.text()));
    setFile(f.name);
    if (!m.mapped.name) { setError("Couldn't find a Name column. The first row must be column headings."); return; }
    setMapped(m);
    start(async () => {
      const r = await runImport(m.rows, true);
      if (r.error) setError(r.error); else setPreview(r.result ?? null);
    });
  }

  function commit() {
    if (!mapped) return;
    start(async () => {
      const r = await runImport(mapped.rows, false);
      if (r.error) setError(r.error); else { setDone(r.result ?? null); setPreview(null); }
    });
  }

  if (done) {
    return (
      <div className={styles.result} role="status">
        <h2>Imported</h2>
        <p className={styles.big}>{done.clients} customers, {done.properties} properties, {done.jobs} jobs.</p>
        {done.skipped.length + done.errors.length > 0 && <p>{done.skipped.length + done.errors.length} rows were left out (listed in the preview).</p>}
        <p><Link href="/clients">See customers</Link> · <Link href="/schedule">See the schedule</Link></p>
      </div>
    );
  }

  return (
    <div className={styles.flow}>
      <label className={styles.drop}>
        <span className={styles.dropTitle}>{file ?? "Choose a CSV file"}</span>
        <span className={styles.muted}>Export from your spreadsheet or old software as CSV. First row = column headings.</span>
        <input type="file" accept=".csv,text/csv" onChange={(e) => onFile(e.target.files?.[0])} />
      </label>

      {error && <p className="error-text" role="alert">{error}</p>}
      {pending && <p className={styles.muted} role="status">Checking the file…</p>}

      {mapped && (
        <section className={styles.panel} aria-labelledby="cols">
          <h2 id="cols">Columns</h2>
          <ul className={styles.cols}>
            {Object.keys(IMPORT_COLUMNS).filter((k) => mapped.mapped[k]).map((k) => (
              <li key={k}><strong>{LABEL[k]}</strong> ← {mapped.mapped[k]}</li>
            ))}
          </ul>
          {mapped.ignored.length > 0 && <p className={styles.muted}>Not used: {mapped.ignored.join(", ")}</p>}
        </section>
      )}

      {preview && (
        <section className={styles.panel} aria-labelledby="prev">
          <h2 id="prev">Preview</h2>
          <p className={styles.big}>
            {preview.rows} rows: <strong>{preview.clients}</strong> new customers, <strong>{preview.properties}</strong> properties,{" "}
            <strong>{preview.jobs}</strong> jobs.
          </p>
          {preview.skipped.length > 0 && (
            <>
              <h3>Skipped ({preview.skipped.length})</h3>
              <ul className={styles.issues}>{preview.skipped.map((s) => <li key={`s${s.row}`}>Row {s.row + 1}: {s.reason}</li>)}</ul>
            </>
          )}
          {preview.errors.length > 0 && (
            <>
              <h3 className={styles.bad}>Needs fixing ({preview.errors.length})</h3>
              <ul className={styles.issues}>{preview.errors.map((s) => <li key={`e${s.row}`}>Row {s.row + 1}: {s.reason}</li>)}</ul>
              <p className={styles.muted}>Row numbers match your spreadsheet. Fix these and import the file again; rows already imported are skipped.</p>
            </>
          )}
          <button className="button" onClick={commit} disabled={pending || preview.clients === 0}>
            {preview.clients === 0 ? "Nothing new to import" : `Import ${preview.clients} customers`}
          </button>
        </section>
      )}
    </div>
  );
}

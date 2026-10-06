/** Small RFC 4180 CSV reader/writer (quotes, embedded commas/newlines, BOM). */
export function parseCsv(input: string): string[][] {
  const text = input.replace(/^﻿/, "");
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i]!;
    if (quoted) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; } else quoted = false;
      } else field += c;
    } else if (c === '"' && field === "") {
      quoted = true;
    } else if (c === ",") {
      row.push(field); field = "";
    } else if (c === "\n" || c === "\r") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      row.push(field); field = "";
      if (row.some((f) => f.trim() !== "")) rows.push(row);
      row = [];
    } else field += c;
  }
  row.push(field);
  if (row.some((f) => f.trim() !== "")) rows.push(row);
  return rows;
}

export function toCsv(rows: (string | number | null | undefined)[][]): string {
  const cell = (v: string | number | null | undefined) => {
    const s = v == null ? "" : String(v);
    // Leading = + - @ would run as a formula in spreadsheets; neutralize.
    const safe = /^[=+\-@\t\r]/.test(s) ? `'${s}` : s;
    return /[",\n\r]/.test(safe) ? `"${safe.replace(/"/g, '""')}"` : safe;
  };
  return rows.map((r) => r.map(cell).join(",")).join("\r\n") + "\r\n";
}

/** Import columns we understand, and the header spellings people use for them. */
export const IMPORT_COLUMNS: Record<string, string[]> = {
  name: ["name", "customer", "customer name", "client", "client name", "full name"],
  email: ["email", "e-mail", "email address"],
  phone: ["phone", "phone number", "mobile", "cell", "telephone"],
  company_name: ["company", "business", "company name"],
  kind: ["type", "customer type", "kind"],
  status: ["status", "customer status"],
  lead_source: ["source", "lead source", "referral", "how they found us"],
  tags: ["tags", "tag", "labels"],
  notes: ["notes", "note", "internal notes"],
  address_line1: ["address", "street", "street address", "address 1", "address line 1", "service address"],
  city: ["city", "town"],
  region: ["state", "st", "province", "region"],
  postal_code: ["zip", "zip code", "postal code", "zipcode"],
  access_notes: ["access", "access notes", "gate", "gate notes", "instructions"],
  lawn_sqft: ["lawn size", "sq ft", "sqft", "square feet", "lawn sqft"],
  service: ["service", "job", "job title", "service name", "work"],
  price: ["price", "rate", "price per visit", "amount", "charge"],
  frequency: ["frequency", "how often", "schedule", "interval"],
  day: ["day", "weekday", "service day", "mow day"],
  start_date: ["start", "start date", "first date", "starts"],
  crew: ["crew", "team", "route"],
};

export interface MappedImport {
  rows: Record<string, string>[];
  mapped: Record<string, string>; // our key -> their header
  ignored: string[]; // their headers we don't use
}

export function mapImport(table: string[][]): MappedImport {
  const [header = [], ...body] = table;
  const norm = (h: string) => h.trim().toLowerCase().replace(/[_]+/g, " ").replace(/\s+/g, " ");
  const index: Record<string, number> = {};
  const mapped: Record<string, string> = {};
  header.forEach((h, i) => {
    const n = norm(h);
    for (const [key, aliases] of Object.entries(IMPORT_COLUMNS)) {
      if (index[key] === undefined && aliases.includes(n)) { index[key] = i; mapped[key] = h.trim(); return; }
    }
  });
  const used = new Set(Object.values(index));
  const ignored = header.filter((h, i) => !used.has(i) && h.trim() !== "").map((h) => h.trim());
  const rows = body.map((r) => {
    const o: Record<string, string> = {};
    for (const [key, i] of Object.entries(index)) {
      const v = (r[i] ?? "").trim();
      if (v !== "") o[key] = v;
    }
    return o;
  });
  return { rows, mapped, ignored };
}

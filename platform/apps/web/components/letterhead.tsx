import { supabaseServer } from "@/lib/supabase/server";

export type CompanyProfile = {
  name: string; phone: string | null; email: string | null; website: string | null;
  address_line1: string | null; city: string | null; region: string | null; postal_code: string | null;
  default_tax_rate: number; payment_terms_days: number; invoice_note: string | null; estimate_note: string | null;
};

export async function companyProfile(tenantId: string): Promise<CompanyProfile | null> {
  const { data } = await (await supabaseServer()).from("tenants")
    .select("name, phone, email, website, address_line1, city, region, postal_code, default_tax_rate, payment_terms_days, invoice_note, estimate_note")
    .eq("id", tenantId).maybeSingle();
  return (data as CompanyProfile | null) ?? null;
}

/** Company name and contact block at the top of invoices and estimates (prints too). */
export function Letterhead({ p }: { p: CompanyProfile | null }) {
  if (!p) return null;
  const place = [p.city, [p.region, p.postal_code].filter(Boolean).join(" ")].filter(Boolean).join(", ");
  return (
    <div style={{ display: "grid", gap: 2, marginBottom: 18 }}>
      <strong style={{ fontSize: "1.25rem" }}>{p.name}</strong>
      {p.address_line1 && <span>{p.address_line1}</span>}
      {place && <span>{place}</span>}
      {(p.phone || p.email) && <span>{[p.phone, p.email].filter(Boolean).join(" · ")}</span>}
      {p.website && <span>{p.website}</span>}
    </div>
  );
}

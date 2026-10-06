import { NextResponse, type NextRequest } from "next/server";
import { toCsv, type TimesheetRow, type WeeklyHoursRow } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";

export const dynamic = "force-dynamic";

const ymd = (s: string | null) => (s && /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null);
const hours = (sec: number) => (sec / 3600).toFixed(2);

/**
 * CSV downloads for the office. Every export reads through the signed-in
 * user's session, so the same database rules apply as on screen; numbers come
 * from the same functions the screens use (timesheet, weekly_hours).
 */
export async function GET(req: NextRequest, { params }: { params: Promise<{ kind: string }> }) {
  const { kind } = await params;
  const { company } = await currentCompany();
  if (!isManager(company)) return new NextResponse("Not allowed", { status: 403 });
  const supabase = await supabaseServer();
  const q = req.nextUrl.searchParams;
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const from = ymd(q.get("from")) ?? `${today.slice(0, 8)}01`;
  const to = ymd(q.get("to")) ?? today;
  let rows: (string | number | null)[][] = [];
  let name = kind;

  switch (kind) {
    case "customers": {
      const { data, error } = await supabase.from("clients")
        .select("name, kind, status, company_name, email, phone, lead_source, tags, created_at, properties(address_line1, city, region, postal_code, status)")
        .eq("tenant_id", company.tenant_id).order("name");
      if (error) return new NextResponse(error.message, { status: 500 });
      rows = [["Name", "Type", "Status", "Company", "Email", "Phone", "Lead source", "Tags", "Address", "City", "State", "ZIP", "Added"]];
      for (const c of (data ?? []) as unknown as { name: string; kind: string; status: string; company_name: string | null; email: string | null;
        phone: string | null; lead_source: string | null; tags: string[]; created_at: string;
        properties: { address_line1: string; city: string | null; region: string | null; postal_code: string | null; status: string }[] }[]) {
        const props = c.properties.filter((p) => p.status === "active");
        for (const p of props.length ? props : [null]) {
          rows.push([c.name, c.kind, c.status, c.company_name, c.email, c.phone, c.lead_source, (c.tags ?? []).join(", "),
            p?.address_line1 ?? null, p?.city ?? null, p?.region ?? null, p?.postal_code ?? null, c.created_at.slice(0, 10)]);
        }
      }
      break;
    }
    case "invoices": {
      const { data, error } = await supabase.from("invoices")
        .select("number, status, sent_at, due_at, subtotal, tax_amount, total, amount_paid, clients(name)")
        .eq("tenant_id", company.tenant_id).gte("created_at", `${from}T00:00:00`).lte("created_at", `${to}T23:59:59`).order("created_at");
      if (error) return new NextResponse(error.message, { status: 500 });
      rows = [["Invoice", "Customer", "Status", "Sent", "Due", "Subtotal", "Tax", "Total", "Paid", "Balance"]];
      for (const i of (data ?? []) as unknown as { number: string | null; status: string; sent_at: string | null; due_at: string | null; subtotal: number;
        tax_amount: number; total: number; amount_paid: number; clients: { name: string } | null }[]) {
        rows.push([i.number, i.clients?.name ?? null, i.status, i.sent_at?.slice(0, 10) ?? null, i.due_at?.slice(0, 10) ?? null,
          i.subtotal, i.tax_amount, i.total, i.amount_paid, Number(i.total) - Number(i.amount_paid)]);
      }
      name = `invoices-${from}-to-${to}`;
      break;
    }
    case "payments": {
      const { data, error } = await supabase.from("payments")
        .select("received_on, amount, method, reference, voided_at, invoices(number, clients(name))")
        .eq("tenant_id", company.tenant_id).gte("received_on", from).lte("received_on", to).order("received_on");
      if (error) return new NextResponse(error.message, { status: 500 });
      rows = [["Received", "Customer", "Invoice", "Amount", "Method", "Reference", "Voided"]];
      for (const p of (data ?? []) as unknown as { received_on: string; amount: number; method: string; reference: string | null; voided_at: string | null;
        invoices: { number: string | null; clients: { name: string } | null } | null }[]) {
        rows.push([p.received_on, p.invoices?.clients?.name ?? null, p.invoices?.number ?? null, p.amount, p.method, p.reference, p.voided_at ? "yes" : ""]);
      }
      name = `payments-${from}-to-${to}`;
      break;
    }
    case "timesheet": {
      const { data, error } = await supabase.rpc("timesheet", { p_tenant_id: company.tenant_id, p_from: from, p_to: to });
      if (error) return new NextResponse(error.message, { status: 500 });
      rows = [["Employee", "Date", "Clock in", "Clock out", "Unpaid breaks (h)", "Paid hours", "Status"]];
      const tz = company.timezone;
      const t = (iso: string | null) => (iso ? new Intl.DateTimeFormat("en-US", { timeZone: tz, hour: "numeric", minute: "2-digit" }).format(new Date(iso)) : null);
      for (const r of (data ?? []) as TimesheetRow[]) {
        rows.push([r.employee_name, r.work_date, t(r.clock_in), t(r.clock_out), hours(r.break_seconds), r.worked_seconds == null ? null : hours(r.worked_seconds), r.status]);
      }
      name = `timesheet-${from}-to-${to}`;
      break;
    }
    case "payroll": {
      const week = ymd(q.get("week"));
      if (!week) return new NextResponse("Pick a week", { status: 400 });
      const { data, error } = await supabase.rpc("weekly_hours", { p_tenant_id: company.tenant_id, p_week_start: week });
      if (error) return new NextResponse(error.message, { status: 400 });
      rows = [["Employee", "Regular hours", "Overtime hours", "Total hours", "Closed shifts", "Open shifts", "Needs review"]];
      for (const r of (data ?? []) as WeeklyHoursRow[]) {
        rows.push([r.employee_name, r.regular_hours, r.overtime_hours, r.total_hours, r.closed_shifts, r.open_shifts, r.needs_review_shifts]);
      }
      name = `payroll-week-of-${week}`;
      break;
    }
    default:
      return new NextResponse("Unknown export", { status: 404 });
  }

  return new NextResponse(toCsv(rows), {
    headers: {
      "Content-Type": "text/csv; charset=utf-8",
      "Content-Disposition": `attachment; filename="${company.name.replace(/[^A-Za-z0-9]+/g, "-")}-${name}.csv"`,
      "Cache-Control": "no-store",
    },
  });
}

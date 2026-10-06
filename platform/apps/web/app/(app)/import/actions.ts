"use server";

import { friendlyError } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import { supabaseServer } from "@/lib/supabase/server";

export type ImportResult = {
  dry_run: boolean;
  rows: number;
  clients: number;
  properties: number;
  jobs: number;
  skipped: { row: number; reason: string }[];
  errors: { row: number; reason: string }[];
};

/** Runs the import in the database. dryRun = preview: validated and counted, nothing saved. */
export async function runImport(rows: Record<string, string>[], dryRun: boolean): Promise<{ result?: ImportResult; error?: string }> {
  const { company } = await currentCompany();
  if (!isManager(company)) return { error: "Only owners and admins can import." };
  if (!Array.isArray(rows) || rows.length === 0) return { error: "The file has no rows." };
  if (rows.length > 2000) return { error: "Import up to 2,000 rows at a time. Split the file and import each part." };
  const { data, error } = await (await supabaseServer()).rpc("import_customers", {
    p_tenant_id: company.tenant_id,
    p_rows: rows,
    p_dry_run: dryRun,
  });
  if (error) return { error: friendlyError(error) };
  return { result: data as ImportResult };
}

import { createContext, useCallback, useContext, useEffect, useState } from "react";
import type { Company } from "@crew/shared";
import { supabase } from "./supabase";

type CompanyState = {
  company: Company | null;
  companies: Company[];
  loading: boolean;
  error: unknown;
  reload: () => Promise<void>;
};

const Ctx = createContext<CompanyState>({ company: null, companies: [], loading: true, error: null, reload: async () => {} });

/** The company the crew member works for (first one where they're an employee). */
export function CompanyProvider({ children }: { children: React.ReactNode }) {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>(null);

  const reload = useCallback(async () => {
    const { data, error } = await supabase.rpc("my_companies");
    setError(error);
    if (!error) setCompanies((data ?? []) as Company[]);
    setLoading(false);
  }, []);

  useEffect(() => {
    reload();
  }, [reload]);

  const company = companies.find((c) => c.employee_id) ?? null;
  return <Ctx.Provider value={{ company, companies, loading, error, reload }}>{children}</Ctx.Provider>;
}

export const useCompany = () => useContext(Ctx);

/** Today's date in the company's zone, YYYY-MM-DD. */
export function companyToday(timeZone: string) {
  return new Intl.DateTimeFormat("en-CA", { timeZone }).format(new Date());
}

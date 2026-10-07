import { createContext, useCallback, useContext, useEffect, useState } from "react";
import AsyncStorage from "@react-native-async-storage/async-storage";
import type { Company } from "@crew/shared";
import { supabase } from "./supabase";

type CompanyState = {
  company: Company | null;
  companies: Company[];
  loading: boolean;
  error: unknown;
  /** True when the list shown is the copy saved on this phone (no signal). */
  offline: boolean;
  reload: () => Promise<void>;
};

const Ctx = createContext<CompanyState>({ company: null, companies: [], loading: true, error: null, offline: false, reload: async () => {} });

const key = (userId: string) => `crew.companies.${userId}`;

/**
 * The company the crew member works for (first one where they're an employee).
 * The last good answer is kept on the phone per person, so the app opens and
 * clocks in with no signal.
 */
export function CompanyProvider({ children }: { children: React.ReactNode }) {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown>(null);
  const [offline, setOffline] = useState(false);

  const reload = useCallback(async () => {
    const { data: s } = await supabase.auth.getSession();
    const uid = s.session?.user.id;
    const { data, error } = await supabase.rpc("my_companies");
    if (!error) {
      const list = (data ?? []) as Company[];
      setCompanies(list);
      setOffline(false);
      setError(null);
      if (uid) await AsyncStorage.setItem(key(uid), JSON.stringify(list)).catch(() => {});
    } else {
      setError(error);
      const saved = uid ? await AsyncStorage.getItem(key(uid)).catch(() => null) : null;
      if (saved) {
        setCompanies(JSON.parse(saved) as Company[]);
        setOffline(true);
      }
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    reload();
  }, [reload]);

  const company = companies.find((c) => c.employee_id) ?? null;
  return <Ctx.Provider value={{ company, companies, loading, error, offline, reload }}>{children}</Ctx.Provider>;
}

export const useCompany = () => useContext(Ctx);

/** Today's date in the company's zone, YYYY-MM-DD. */
export function companyToday(timeZone: string) {
  return new Intl.DateTimeFormat("en-CA", { timeZone }).format(new Date());
}

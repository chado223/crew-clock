import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { currentCompany, isManager } from "@/lib/company";
import { Importer } from "./importer";
import styles from "./import.module.css";

export const metadata: Metadata = { title: "Import customers" };

export default async function ImportPage() {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  return (
    <div className={styles.page}>
      <header>
        <h1>Import customers</h1>
        <p className={styles.lede}>
          Bring in customers, their service address and their regular work from a spreadsheet. You&apos;ll see a preview first;
          nothing is saved until you press Import. Customers already here (same email, phone, or name and address) are skipped.
        </p>
      </header>
      <Importer />
      <details className={styles.help}>
        <summary>Columns we understand</summary>
        <p>Name (required), Email, Phone, Company, Type (residential/commercial), Status (active/lead), Lead source, Tags, Notes,
          Address, City, State, ZIP, Access notes, Lawn size, Service, Price, Frequency (weekly, biweekly, every 3 weeks, monthly, once),
          Day (Mon–Sun), Start date (YYYY-MM-DD or MM/DD/YYYY), Crew (must match a crew name).</p>
        <p>Common spellings work too, for example “Customer Name”, “Street Address”, “Zip Code”, “Mow Day”.</p>
      </details>
    </div>
  );
}

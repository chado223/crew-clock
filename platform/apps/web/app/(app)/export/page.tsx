import type { Metadata } from "next";
import Link from "next/link";
import { redirect } from "next/navigation";
import { weekStart } from "@crew/shared";
import { currentCompany, isManager } from "@/lib/company";
import styles from "../import/import.module.css";

export const metadata: Metadata = { title: "Import & export" };

export default async function ExportPage() {
  const { company } = await currentCompany();
  if (!isManager(company)) redirect("/");
  const today = new Intl.DateTimeFormat("en-CA", { timeZone: company.timezone }).format(new Date());
  const monthStart = `${today.slice(0, 8)}01`;
  const lastWeek = weekStart(new Date(Date.now() - 7 * 86_400_000), company.timezone);

  return (
    <div className={styles.page}>
      <header>
        <h1>Import & export</h1>
        <p className={styles.lede}>Download your data as CSV files that open in Excel, Google Sheets or your accountant&apos;s software. Hours use the same calculation as every screen.</p>
      </header>

      <section className={styles.panel}>
        <h2>Bring customers in</h2>
        <p><Link href="/import" className="button">Import customers from CSV</Link></p>
      </section>

      <section className={styles.panel}>
        <h2>Payroll</h2>
        <form action="/export/payroll" method="get" className={styles.cols}>
          <div className="field"><label htmlFor="wk">Week starting</label><input id="wk" name="week" type="date" defaultValue={lastWeek} className="input" /></div>
          <div><button className="button" type="submit">Download payroll hours</button></div>
        </form>
        <p className={styles.muted}>Regular and overtime hours per person for that week, using your overtime rule. Shifts that need review are counted so you can fix them first.</p>
      </section>

      <section className={styles.panel}>
        <h2>Date range</h2>
        <form method="get" className={styles.cols}>
          <div className="field"><label htmlFor="from">From</label><input id="from" name="from" type="date" defaultValue={monthStart} className="input" /></div>
          <div className="field"><label htmlFor="to">To</label><input id="to" name="to" type="date" defaultValue={today} className="input" /></div>
          <div><button className="button quiet" type="submit" formAction="/export/timesheet">Timesheet (every shift)</button></div>
          <div><button className="button quiet" type="submit" formAction="/export/invoices">Invoices</button></div>
          <div><button className="button quiet" type="submit" formAction="/export/payments">Payments</button></div>
        </form>
      </section>

      <section className={styles.panel}>
        <h2>Everything else</h2>
        <p><a className="button quiet" href="/export/customers">Customers and addresses</a></p>
      </section>
    </div>
  );
}

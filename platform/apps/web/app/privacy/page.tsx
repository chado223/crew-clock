import type { Metadata } from "next";
import { LegalPage } from "@/components/legal";

export const metadata: Metadata = { title: "Privacy" };

export default function Privacy() {
  return (
    <LegalPage title="Privacy policy" updated="October 2026">
      <h2>What Crew is</h2>
      <p>
        Crew is software that lawn care and field service companies use to run their business: customers, schedules,
        time clock, invoices and a customer portal. Your company decides what goes into it; we run the service for them.
      </p>
      <h2>What we collect</h2>
      <ul>
        <li><strong>Sign-in:</strong> your email address. Sign-in is by emailed code; there are no passwords.</li>
        <li><strong>If you work for a company:</strong> your name, contact details your employer enters, clock-in and clock-out
          times, breaks, the stops you work, notes and photos you take on the job. Pay rates are visible only to the company's managers.</li>
        <li><strong>If you're a company's customer:</strong> your name, contact details, service addresses, the work done,
          invoices and payments, messages, and photos the company chooses to share with you.</li>
        <li><strong>Location:</strong> the app does not track your location. Service addresses are turned into map
          coordinates to plan routes and check weather.</li>
        <li><strong>Technical:</strong> basic logs needed to keep the service secure and working.</li>
      </ul>
      <h2>How it's used</h2>
      <p>
        Only to provide the service to your company: payroll hours, scheduling, billing, job records and messages.
        We don't sell personal information and don't use it for advertising.
      </p>
      <h2>Who can see it</h2>
      <p>
        Each company's data is kept separate from every other company's, enforced in the database. Inside a company,
        crew members see their own hours and assigned stops; managers see the company's records. Customers see only
        their own customer-facing records, never internal notes, pay or costs.
      </p>
      <h2>Service providers</h2>
      <p>
        Data is stored with Supabase (database, sign-in and file storage, United States). Weather comes from the US
        National Weather Service and addresses are located with the US Census geocoder; only addresses are sent to them.
        Email providers used to send sign-in codes and company messages receive the email address and message.
      </p>
      <h2>How long it's kept</h2>
      <p>
        Business records (hours, invoices, job history) are kept while the company uses Crew, because companies need
        them for payroll, tax and their own records. A company can export its data at any time.
      </p>
      <h2>Your choices</h2>
      <ul>
        <li>Delete your account from the app (Account → Delete my account) or at <a href="/account">/account</a>.
          Your sign-in and access are removed. Records that belong to your employer's or service company's
          business (like hours you worked) stay with that company, without the link to your login.</li>
        <li>Ask the company you work for, or are a customer of, to correct or remove information they entered about you.</li>
        <li>Choose how a company may contact you from the customer portal.</li>
      </ul>
      <h2>Children</h2>
      <p>Crew is for businesses and is not directed to children under 13.</p>
      <h2>Changes</h2>
      <p>We'll update this page and its date when this policy changes.</p>
    </LegalPage>
  );
}

import type { Metadata } from "next";
import { LegalPage } from "@/components/legal";

export const metadata: Metadata = { title: "Terms" };

export default function Terms() {
  return (
    <LegalPage title="Terms of use" updated="October 2026">
      <h2>The service</h2>
      <p>
        Crew provides scheduling, time-clock, customer, billing and portal tools to lawn care and field service
        companies. A company account is opened by its owner, who is responsible for the people they invite and the
        information they enter.
      </p>
      <h2>Your responsibilities</h2>
      <ul>
        <li>Keep your email account secure; it is how you sign in.</li>
        <li>Record time and work honestly. Changes to recorded time are logged with who made them and why.</li>
        <li>Only enter information you have the right to use, and follow the law that applies to your business,
          including wage-and-hour and consumer-messaging rules.</li>
        <li>Don't try to access another company's data or interfere with the service.</li>
      </ul>
      <h2>Your data</h2>
      <p>
        A company owns its business data and can export it at any time. We use it only to provide the service, as
        described in the <a href="/privacy">privacy policy</a>.
      </p>
      <h2>Payments</h2>
      <p>
        Any subscription prices are shown before you agree to them. Payments customers make to a company go to that company.
      </p>
      <h2>Availability</h2>
      <p>
        We work to keep Crew available and your data backed up, but the service is provided as is. Payroll and tax
        filings remain the company's responsibility; check exported hours before paying.
      </p>
      <h2>Ending</h2>
      <p>
        You can delete your account at any time. A company owner can export their data and ask for the company to be closed.
      </p>
      <h2>Governing law</h2>
      <p>These terms are governed by the laws of the State of Tennessee.</p>
    </LegalPage>
  );
}

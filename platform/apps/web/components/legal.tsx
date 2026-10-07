import Link from "next/link";
import styles from "../app/auth.module.css";

/** Who runs the service and how to reach them; set at launch. */
export const OPERATOR = process.env.NEXT_PUBLIC_LEGAL_OPERATOR ?? "CWLC (Chad Washam Lawncare), Seymour, Tennessee";
export const SUPPORT_EMAIL = process.env.NEXT_PUBLIC_SUPPORT_EMAIL ?? "";
const APPROVED = process.env.NEXT_PUBLIC_LEGAL_APPROVED === "yes";

export function LegalPage({ title, updated, children }: { title: string; updated: string; children: React.ReactNode }) {
  return (
    <main className={styles.page}>
      <article className={styles.panel} style={{ maxWidth: 720 }}>
        <p className={styles.brand}>Crew</p>
        {!APPROVED && (
          <p className="error-text">Draft — not yet reviewed. This page will be finalized before the app is released.</p>
        )}
        <h1>{title}</h1>
        <p className={styles.lede}>Last updated {updated}</p>
        {children}
        <p className={styles.lede}>
          Questions: {SUPPORT_EMAIL ? <a href={`mailto:${SUPPORT_EMAIL}`}>{SUPPORT_EMAIL}</a> : "contact your company or the operator below"}.
          {" "}Operator: {OPERATOR}.
        </p>
        <p className={styles.lede}><Link href="/privacy">Privacy</Link> · <Link href="/terms">Terms</Link></p>
      </article>
    </main>
  );
}

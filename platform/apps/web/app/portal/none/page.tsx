import type { Metadata } from "next";
import styles from "../portal.module.css";

export const metadata: Metadata = { title: "No account found" };

export default function NoPortalAccount() {
  return (
    <section className={styles.section}>
      <h1>We couldn't find your service account</h1>
      <p>
        This email isn't connected to a customer account yet. Ask your lawn care company to send you an invite link, then sign in
        with the email they used.
      </p>
    </section>
  );
}

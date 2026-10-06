import Link from "next/link";

export default function NotFound() {
  return (
    <main style={{ maxWidth: 560, margin: "15vh auto", padding: "0 20px", display: "grid", gap: 14 }}>
      <h1>Not found</h1>
      <p>That page or record doesn&apos;t exist, or you don&apos;t have access to it.</p>
      <p><Link href="/">Go to Today</Link></p>
    </main>
  );
}

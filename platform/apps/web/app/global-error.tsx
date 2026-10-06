"use client";

export default function GlobalError({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  return (
    <html lang="en">
      <body style={{ fontFamily: "system-ui, sans-serif", background: "#F3F5F1", color: "#17251C" }}>
        <main style={{ maxWidth: 560, margin: "15vh auto", padding: "0 20px" }}>
          <h1>Something went wrong</h1>
          <p>The app couldn&apos;t load. Try again in a moment{error.digest ? ` (reference ${error.digest})` : ""}.</p>
          <button onClick={() => reset()} style={{ padding: "12px 18px", borderRadius: 8, border: 0, background: "#2F6B3A", color: "#fff", fontWeight: 600 }}>
            Try again
          </button>
        </main>
      </body>
    </html>
  );
}

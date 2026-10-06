"use client";

import { useEffect } from "react";

/** Shown when a page fails to load. The details go to the server/browser logs, not the screen. */
export default function ErrorPage({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => {
    console.error(error);
  }, [error]);
  return (
    <main style={{ maxWidth: 560, margin: "15vh auto", padding: "0 20px", display: "grid", gap: 14 }}>
      <h1>Something went wrong</h1>
      <p>This page couldn&apos;t load. Your data is safe. Try again, and if it keeps happening, let us know{error.digest ? ` (reference ${error.digest})` : ""}.</p>
      <div style={{ display: "flex", gap: 10 }}>
        <button className="button" onClick={() => reset()}>Try again</button>
        <a className="button quiet" href="/">Go to Today</a>
      </div>
    </main>
  );
}

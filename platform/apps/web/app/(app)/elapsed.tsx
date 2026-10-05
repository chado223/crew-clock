"use client";

import { useEffect, useState } from "react";
import { elapsedSince, formatDuration } from "@crew/shared";

/** Live running time for an open shift. Display only; totals come from the database. */
export function Elapsed({ since, className }: { since: string; className?: string }) {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 30_000);
    return () => clearInterval(id);
  }, []);
  return (
    <span className={className} suppressHydrationWarning>
      {formatDuration(elapsedSince(since, now))}
    </span>
  );
}

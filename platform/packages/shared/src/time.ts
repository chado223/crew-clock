/**
 * Display helpers only. Hours are CALCULATED in the database
 * (timesheet / weekly_hours); clients only format what it returns.
 */

/** 9000 -> "2:30" (hours:minutes) */
export function formatDuration(seconds: number | null | undefined): string {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return "–";
  const total = Math.floor(seconds / 60);
  const h = Math.floor(total / 60);
  const m = total % 60;
  return `${h}:${m.toString().padStart(2, "0")}`;
}

/** Live elapsed time for an open shift (display only). */
export function elapsedSince(iso: string, now: Date = new Date()): number {
  return Math.max(0, Math.floor((now.getTime() - new Date(iso).getTime()) / 1000));
}

/** First day of the work week containing `date`, as YYYY-MM-DD in the company's zone. */
export function weekStart(date: Date, timeZone: string, weekStartIsoDay = 1): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
  }).formatToParts(date);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
  const isoDow = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].indexOf(get("weekday")) + 1;
  const local = new Date(Date.UTC(Number(get("year")), Number(get("month")) - 1, Number(get("day"))));
  const back = (isoDow - weekStartIsoDay + 7) % 7;
  local.setUTCDate(local.getUTCDate() - back);
  return local.toISOString().slice(0, 10);
}

export function formatClockTime(iso: string, timeZone: string): string {
  return new Intl.DateTimeFormat("en-US", { timeZone, hour: "numeric", minute: "2-digit" }).format(new Date(iso));
}

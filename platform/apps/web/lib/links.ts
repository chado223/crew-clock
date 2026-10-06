/** Where an owner_attention item (or any record reference) opens in the app. */
export function recordHref(refType: string, refId: string | null, refDate?: string | null, kind?: string): string {
  if (kind === "weather") return "/weather";
  if ((kind === "unassigned" || kind === "missed_visit") && refDate) return `/schedule?week=${refDate}`;
  switch (refType) {
    case "invoice": return `/invoices/${refId}`;
    case "estimate": return `/estimates/${refId}`;
    case "client": return `/clients/${refId}`;
    case "visit": return refDate ? `/routes?date=${refDate}` : "/schedule";
    case "time_entry": return "/time";
    case "message": return "/messages";
    default: return "/";
  }
}

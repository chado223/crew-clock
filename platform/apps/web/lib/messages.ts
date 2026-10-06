export type MessageRow = {
  id: string;
  template_key: string;
  channel: "email" | "sms";
  mode: "test" | "live";
  status: string;
  suppressed_reason: string | null;
  to_address: string | null;
  delivered_to: string | null;
  subject: string | null;
  error: string | null;
  created_at: string;
  sent_at: string | null;
};

export const MESSAGE_COLUMNS =
  "id, template_key, channel, mode, status, suppressed_reason, to_address, delivered_to, subject, error, created_at, sent_at";

const REASONS: Record<string, string> = {
  messaging_off: "messages are turned off",
  test_mode_no_test_recipient: "test mode is on and no test address is set",
  no_email_on_file: "no email on file",
  no_sms_on_file: "no mobile number on file",
  unsubscribed: "the customer unsubscribed",
  email_not_allowed: "the customer turned off email",
  no_sms_consent: "the customer hasn't agreed to texts",
};

/** One plain sentence about what happened to a message. */
export function describeMessage(m: MessageRow): string {
  if (m.status === "suppressed") {
    const r = m.suppressed_reason ?? "";
    return `Not sent: ${REASONS[r] ?? (r.startsWith("customer_turned_off_") ? "the customer turned these off" : r.replaceAll("_", " "))}.`;
  }
  const where = m.mode === "test" ? `test copy to ${m.delivered_to} (customer: ${m.to_address})` : `to ${m.delivered_to}`;
  if (m.status === "queued" || m.status === "sending") return `Queued, ${where}.`;
  if (m.status === "sent" || m.status === "delivered") return `${m.status === "delivered" ? "Delivered" : "Sent"}, ${where}.`;
  if (m.status === "failed") return `Failed${m.error ? `: ${m.error}` : ""}.`;
  return m.status === "canceled" ? "Canceled." : m.status;
}

export const TEMPLATE_LABEL: Record<string, string> = {
  estimate_sent: "Estimate",
  invoice_sent: "Invoice",
  invoice_reminder: "Payment reminder",
  visit_reminder: "Visit reminder",
  visit_rescheduled: "Visit moved",
  portal_invite: "Portal invite",
  team_invite: "Team invite",
};

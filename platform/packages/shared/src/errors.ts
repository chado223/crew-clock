/**
 * The database raises stable message keys (see platform/README.md).
 * Web and mobile show the same wording for the same problem.
 */
const MESSAGES: Record<string, string> = {
  not_authenticated: "Sign in to continue.",
  forbidden: "You don't have permission to do that in this company.",
  not_an_active_employee: "Your account isn't set up as an active employee here. Ask your manager.",
  already_clocked_in: "You're already on the clock.",
  not_clocked_in: "You're not on the clock.",
  overlaps_existing_shift: "That time overlaps a shift that's already recorded.",
  clock_out_before_clock_in: "Clock-out has to be after clock-in.",
  punch_in_future: "That time is in the future. Check your phone's clock.",
  punch_too_old: "That punch is more than 3 days old. Ask a manager to add it.",
  already_on_break: "You're already on break.",
  not_on_break: "You're not on break.",
  reason_required: "Add a short reason for the change.",
  invalid_company_name: "Company name needs 2 to 120 characters.",
  invalid_timezone: "Pick a valid time zone.",
  invalid_email: "Enter a valid email address.",
  already_member: "That person is already on your team.",
  only_owner_can_invite_managers: "Only an owner can invite admins or owners.",
  only_owner_can_change_roles: "Only an owner can change roles.",
  only_owner_can_remove_managers: "Only an owner can remove admins or owners.",
  cannot_remove_last_owner: "A company needs at least one owner. Add another owner first.",
  invitation_invalid: "This invite link isn't valid. Ask for a new one.",
  invitation_expired: "This invite link has expired. Ask for a new one.",
  invitation_already_used: "This invite has already been used.",
  invitation_email_mismatch: "This invite was sent to a different email. Sign in with that email.",
  invalid_activity_kind: "Pick what kind of entry this is.",
  summary_required: "Write a short note.",
  not_found: "That record doesn't exist or was removed.",
  invalid_date_range: "Pick a date range of two months or less.",
  visit_not_scheduled: "Only scheduled visits can be moved.",
  visit_not_open: "This visit is already finished.",
  date_required: "Pick a date.",
  job_not_found: "That job doesn't exist.",
  crew_not_found: "That crew doesn't exist.",
  employee_not_found: "That employee doesn't exist.",
  document_not_draft: "This is no longer a draft, so its lines can't change.",
  estimate_empty: "Add at least one line first.",
  estimate_not_approved: "Record the customer's approval first.",
  estimate_converted: "This estimate already became jobs.",
  property_required: "Pick the property this work is for.",
  invalid_status: "That status isn't allowed here.",
  nothing_to_invoice: "No completed, unbilled visits for this customer in that period.",
  invalid_tax_rate: "Tax rate must be between 0% and 100%.",
  invoice_empty: "This invoice has no amount.",
  invoice_not_open: "Payments can only be recorded on sent invoices.",
  invalid_amount: "Enter an amount greater than zero.",
  overpayment: "That's more than the balance due.",
  estimate_not_open: "This estimate has already been answered.",
  estimate_expired: "This estimate has expired. Ask for an updated one.",
  details_required: "Tell us a little about what you need.",
  too_many_requests: "You've sent several requests already. We'll be in touch soon.",
  invalid_phone: "Enter a phone number using digits.",
  note_too_long: "Keep the note under 2,000 characters.",
  week_start_mismatch: "That date isn't the first day of your work week.",
};

/** Map a Supabase/Postgres error to a message a person can act on. */
export function friendlyError(err: unknown): string {
  const raw =
    typeof err === "string"
      ? err
      : err && typeof err === "object" && "message" in err
        ? String((err as { message: unknown }).message)
        : "";
  const key = raw.split(":")[0]?.trim() ?? "";
  return MESSAGES[key] ?? "Something went wrong. Try again, and contact support if it keeps happening.";
}

/**
 * Shapes returned by the database functions. Hand-written for now; will be
 * replaced by types generated from the staging schema.
 */
export type Role = "owner" | "admin" | "crew";

export interface Company {
  tenant_id: string;
  name: string;
  role: Role;
  employee_id: string | null;
  timezone: string;
}

export type ShiftStatus = "open" | "closed" | "needs_review" | "voided";

export interface TimesheetRow {
  entry_id: string;
  employee_id: string;
  employee_name: string;
  work_date: string;
  clock_in: string;
  clock_out: string | null;
  break_seconds: number;
  worked_seconds: number | null;
  status: ShiftStatus;
  job_id: string | null;
  source: string;
}

export interface WeeklyHoursRow {
  employee_id: string;
  employee_name: string;
  week_start: string;
  week_end: string;
  total_seconds: number;
  regular_seconds: number;
  overtime_seconds: number;
  total_hours: number;
  regular_hours: number;
  overtime_hours: number;
  closed_shifts: number;
  open_shifts: number;
  needs_review_shifts: number;
}

export interface TimeEntry {
  id: string;
  tenant_id: string;
  employee_id: string;
  clock_in: string;
  clock_out: string | null;
}

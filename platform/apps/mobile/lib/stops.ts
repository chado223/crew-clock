/** One row of the database's schedule() read model, as the crew app uses it. */
export interface Stop {
  visit_id: string;
  scheduled_date: string;
  status: "scheduled" | "in_progress" | "completed" | "skipped" | "canceled";
  status_reason: string | null;
  job_title: string;
  client_name: string | null;
  client_phone: string | null;
  address: string | null;
  access_notes: string | null;
  latitude: number | null;
  longitude: number | null;
  crew_name: string | null;
  assignees: string[];
  est_minutes: number | null;
  completion_notes: string | null;
}

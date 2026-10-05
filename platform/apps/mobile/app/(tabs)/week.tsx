import { useCallback, useState } from "react";
import { Pressable, RefreshControl, ScrollView, StyleSheet, Text, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useFocusEffect } from "expo-router";
import { formatClockTime, formatDuration, friendlyError, weekStart, type TimesheetRow, type WeeklyHoursRow } from "@crew/shared";
import { supabase } from "../../lib/supabase";
import { useCompany } from "../../lib/company";
import { color, font } from "../../lib/theme";

function addDays(ymd: string, n: number) {
  const d = new Date(`${ymd}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
const dayName = (ymd: string) =>
  new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "long", month: "short", day: "numeric" }).format(new Date(`${ymd}T12:00:00Z`));

/** The crew member's own hours, from the same calculation payroll uses. */
export default function WeekScreen() {
  const { company } = useCompany();
  const [offset, setOffset] = useState(0);
  const [totals, setTotals] = useState<WeeklyHoursRow | null>(null);
  const [shifts, setShifts] = useState<TimesheetRow[]>([]);
  const [error, setError] = useState<string | null>(null);

  const start = company ? addDays(weekStart(new Date(), company.timezone), offset * 7) : null;

  const load = useCallback(async () => {
    if (!company || !start) return;
    setError(null);
    const [w, t] = await Promise.all([
      supabase.rpc("weekly_hours", { p_tenant_id: company.tenant_id, p_week_start: start }),
      supabase.rpc("timesheet", { p_tenant_id: company.tenant_id, p_from: start, p_to: addDays(start, 6) }),
    ]);
    if (w.error || t.error) return setError(friendlyError(w.error ?? t.error));
    // Crew members only ever receive their own rows (database rules).
    setTotals(((w.data ?? []) as WeeklyHoursRow[])[0] ?? null);
    setShifts((t.data ?? []) as TimesheetRow[]);
  }, [company, start]);

  useFocusEffect(
    useCallback(() => {
      load();
    }, [load]),
  );

  if (!company || !start) return <SafeAreaView style={styles.safe} />;

  const days = Array.from(new Set(shifts.map((s) => s.work_date)));

  return (
    <SafeAreaView style={styles.safe} edges={["top"]}>
      <ScrollView contentContainerStyle={styles.body} refreshControl={<RefreshControl refreshing={false} onRefresh={load} />}>
        <Text style={styles.title}>My week</Text>
        <View style={styles.weekNav}>
          <Pressable accessibilityRole="button" onPress={() => setOffset((o) => o - 1)} hitSlop={12} style={styles.navBtn}>
            <Text style={styles.link}>Previous</Text>
          </Pressable>
          <Text style={styles.range}>
            {new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" }).format(new Date(`${start}T12:00:00Z`))} to{" "}
            {new Intl.DateTimeFormat("en-US", { timeZone: "UTC", month: "short", day: "numeric" }).format(new Date(`${addDays(start, 6)}T12:00:00Z`))}
          </Text>
          <Pressable accessibilityRole="button" disabled={offset >= 0} onPress={() => setOffset((o) => Math.min(0, o + 1))} hitSlop={12} style={styles.navBtn}>
            <Text style={[styles.link, offset >= 0 && styles.disabled]}>Next</Text>
          </Pressable>
        </View>

        {error && <Text style={styles.error}>{error}</Text>}

        <View style={styles.totals}>
          <View style={styles.total}>
            <Text style={styles.big}>{formatDuration(totals?.total_seconds ?? 0)}</Text>
            <Text style={styles.label}>Total hours</Text>
          </View>
          <View style={styles.total}>
            <Text style={[styles.big, styles.ot]}>{formatDuration(totals?.overtime_seconds ?? 0)}</Text>
            <Text style={styles.label}>Overtime</Text>
          </View>
        </View>
        {(totals?.open_shifts ?? 0) + (totals?.needs_review_shifts ?? 0) > 0 && (
          <Text style={styles.note}>
            A shift still needs a clock-out or a manager's review, so it isn't counted yet.
          </Text>
        )}

        {days.length === 0 ? (
          <Text style={styles.lede}>No shifts this week.</Text>
        ) : (
          days.map((d) => (
            <View key={d} style={styles.day}>
              <Text style={styles.dayName}>{dayName(d)}</Text>
              {shifts
                .filter((s) => s.work_date === d)
                .map((s) => (
                  <View key={s.entry_id} style={styles.shift}>
                    <Text style={styles.shiftTime}>
                      {formatClockTime(s.clock_in, company.timezone)}
                      {s.clock_out ? ` to ${formatClockTime(s.clock_out, company.timezone)}` : " (still on the clock)"}
                    </Text>
                    <Text style={styles.shiftHours}>{formatDuration(s.worked_seconds)}</Text>
                  </View>
                ))}
            </View>
          ))
        )}
        <Text style={styles.footnote}>Hours shown here are the same ones used for payroll. If something looks wrong, tell your manager.</Text>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: color.daylight },
  body: { padding: 20, gap: 16 },
  title: { fontFamily: font.textBold, fontSize: 28, color: color.ink },
  weekNav: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  navBtn: { minHeight: 44, justifyContent: "center" },
  range: { fontFamily: font.textBold, fontSize: 17, color: color.ink },
  link: { fontFamily: font.textMedium, fontSize: 16, color: color.turf },
  disabled: { color: color.line },
  error: { fontFamily: font.textMedium, fontSize: 16, color: color.error },
  totals: { flexDirection: "row", gap: 12 },
  total: { flex: 1, padding: 16, borderRadius: 14, backgroundColor: color.surface, borderWidth: 1, borderColor: color.line },
  big: { fontFamily: font.figure, fontSize: 48, color: color.ink, fontVariant: ["tabular-nums"] },
  ot: { color: color.turf },
  label: { fontFamily: font.textMedium, fontSize: 15, color: color.inkSoft },
  note: { fontFamily: font.text, fontSize: 15, color: color.ink, backgroundColor: "#F6DFD8", padding: 12, borderRadius: 10 },
  lede: { fontFamily: font.text, fontSize: 17, color: color.inkSoft },
  day: { gap: 6 },
  dayName: { fontFamily: font.textBold, fontSize: 17, color: color.ink },
  shift: { flexDirection: "row", justifyContent: "space-between", padding: 14, borderRadius: 12, backgroundColor: color.surface },
  shiftTime: { fontFamily: font.text, fontSize: 16, color: color.ink },
  shiftHours: { fontFamily: font.figure, fontSize: 20, color: color.ink },
  footnote: { fontFamily: font.text, fontSize: 14, color: color.inkSoft, marginTop: 8 },
});

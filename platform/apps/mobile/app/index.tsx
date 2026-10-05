import { useCallback, useEffect, useState } from "react";
import { AppState, Pressable, RefreshControl, ScrollView, StyleSheet, Text, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { elapsedSince, formatClockTime, formatDuration, friendlyError, type Company, type TimeEntry } from "@crew/shared";
import { supabase } from "../lib/supabase";
import { flush, pendingPunches, punch, type QueuedPunch } from "../lib/punchQueue";
import { color, font } from "../lib/theme";

export default function ClockScreen() {
  const [company, setCompany] = useState<Company | null>(null);
  const [open, setOpen] = useState<TimeEntry | null>(null);
  const [queued, setQueued] = useState<QueuedPunch[]>([]);
  const [now, setNow] = useState(() => new Date());
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    const { rejected } = await flush().catch(() => ({ rejected: [] }));
    if (rejected.length > 0) setMessage(`A saved punch couldn't be recorded: ${friendlyError(rejected[0]!.error)}`);
    setQueued(await pendingPunches());

    const { data: companies, error } = await supabase.rpc("my_companies");
    if (error) {
      setLoading(false);
      return setMessage(friendlyError(error));
    }
    const c = ((companies ?? []) as Company[]).find((x) => x.employee_id) ?? null;
    setCompany(c);
    if (c?.employee_id) {
      const { data } = await supabase
        .from("time_entries")
        .select("id, tenant_id, employee_id, clock_in, clock_out")
        .eq("employee_id", c.employee_id)
        .is("clock_out", null)
        .is("voided_at", null)
        .maybeSingle();
      setOpen((data as TimeEntry | null) ?? null);
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    refresh();
    const tick = setInterval(() => setNow(new Date()), 15_000);
    const sub = AppState.addEventListener("change", (s) => s === "active" && refresh());
    return () => {
      clearInterval(tick);
      sub.remove();
    };
  }, [refresh]);

  // What the person sees reflects queued punches too, so offline taps feel immediate.
  const lastQueued = queued[queued.length - 1];
  const onClock = lastQueued ? lastQueued.kind === "in" : open !== null;
  const since = lastQueued?.kind === "in" ? lastQueued.at : open?.clock_in;

  async function onPress() {
    if (!company) return;
    setBusy(true);
    setMessage(null);
    try {
      const result = await punch(onClock ? "out" : "in", company.tenant_id);
      if (result === "queued") setMessage("No signal. Your punch is saved on this phone and will send automatically.");
      await refresh();
    } catch (err) {
      setMessage(friendlyError(err));
      await refresh();
    } finally {
      setBusy(false);
    }
  }

  if (loading) return <SafeAreaView style={styles.safe} />;

  if (!company) {
    return (
      <SafeAreaView style={styles.safe}>
        <View style={styles.center}>
          <Text style={styles.title}>You're not on a team yet</Text>
          <Text style={styles.lede}>Ask your manager to invite this email, then open the invite link.</Text>
          <Pressable onPress={() => supabase.auth.signOut()} accessibilityRole="button">
            <Text style={styles.link}>Sign out</Text>
          </Pressable>
        </View>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={[styles.safe, onClock && styles.safeOn]}>
      <ScrollView
        contentContainerStyle={styles.body}
        refreshControl={<RefreshControl refreshing={false} onRefresh={refresh} />}
      >
        <View style={styles.top}>
          <Text style={[styles.company, onClock && styles.onText]}>{company.name}</Text>
          <Pressable onPress={() => supabase.auth.signOut()} accessibilityRole="button" hitSlop={12}>
            <Text style={[styles.link, onClock && styles.onSoft]}>Sign out</Text>
          </Pressable>
        </View>

        <View style={styles.readout}>
          <Text style={[styles.status, onClock && styles.onSoft]}>{onClock ? "On the clock" : "Off the clock"}</Text>
          <Text style={[styles.elapsed, onClock && styles.elapsedOn]} accessibilityLabel={onClock ? "Time on shift" : undefined}>
            {onClock && since ? formatDuration(elapsedSince(since, now)) : "0:00"}
          </Text>
          {onClock && since && (
            <Text style={[styles.since, styles.onSoft]}>since {formatClockTime(since, company.timezone)}</Text>
          )}
        </View>

        {message && <Text style={[styles.message, onClock && styles.onText]}>{message}</Text>}
        {queued.length > 0 && (
          <Text style={[styles.queued, onClock && styles.onSoft]}>
            {queued.length} {queued.length === 1 ? "punch" : "punches"} waiting to send
          </Text>
        )}
      </ScrollView>

      <Pressable
        accessibilityRole="button"
        accessibilityLabel={onClock ? "Clock out" : "Clock in"}
        disabled={busy}
        onPress={onPress}
        style={({ pressed }) => [styles.punch, onClock ? styles.punchOut : styles.punchIn, (pressed || busy) && styles.pressed]}
      >
        <Text style={[styles.punchText, onClock && styles.punchTextOut]}>{onClock ? "Clock out" : "Clock in"}</Text>
      </Pressable>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: color.daylight },
  safeOn: { backgroundColor: color.ink },
  body: { flexGrow: 1, paddingHorizontal: 24, paddingTop: 12, gap: 24 },
  center: { flex: 1, justifyContent: "center", paddingHorizontal: 24, gap: 14 },
  top: { flexDirection: "row", justifyContent: "space-between", alignItems: "center" },
  company: { fontFamily: font.textBold, fontSize: 18, color: color.ink },
  readout: { flexGrow: 1, justifyContent: "center", gap: 4 },
  status: { fontFamily: font.textMedium, fontSize: 20, color: color.inkSoft },
  elapsed: { fontFamily: font.figure, fontSize: 112, lineHeight: 116, color: color.inkSoft, fontVariant: ["tabular-nums"] },
  elapsedOn: { color: color.hivis },
  since: { fontFamily: font.text, fontSize: 18 },
  message: { fontFamily: font.textMedium, fontSize: 17, color: color.ink },
  queued: { fontFamily: font.text, fontSize: 16, color: color.inkSoft },
  title: { fontFamily: font.textBold, fontSize: 26, color: color.ink },
  lede: { fontFamily: font.text, fontSize: 17, color: color.inkSoft },
  link: { fontFamily: font.textMedium, fontSize: 16, color: color.turf },
  onText: { color: "#F1F5EE" },
  onSoft: { color: "#B9C6B6" },
  // Thumb-zone button: full width, tall enough to hit with gloves.
  punch: { margin: 16, minHeight: 96, borderRadius: 20, alignItems: "center", justifyContent: "center" },
  punchIn: { backgroundColor: color.turf },
  punchOut: { backgroundColor: color.hivis },
  pressed: { opacity: 0.85 },
  punchText: { fontFamily: font.textBold, fontSize: 28, color: "#fff" },
  punchTextOut: { color: color.ink },
});

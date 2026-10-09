import { useCallback, useEffect, useRef, useState } from "react";
import { AppState, Linking, Pressable, RefreshControl, ScrollView, StyleSheet, Text, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { Link, useFocusEffect, useRouter } from "expo-router";
import { elapsedSince, formatClockTime, formatDuration, friendlyError, googleRouteUrl, type NavTarget, type TimeEntry } from "@crew/shared";
import { supabase } from "../../lib/supabase";
import { companyToday, useCompany } from "../../lib/company";
import {
  dismissSyncProblem, flush, heldForOthers, pendingActions, pendingBreakState, pendingClockState, pendingVisitStatus, perform, syncProblems,
  type QueuedAction,
} from "../../lib/actionQueue";
import { readCache, saveCache } from "../../lib/cache";
import { flushPhotos } from "../../lib/photos";
import type { Stop } from "../../lib/stops";
import { color, font } from "../../lib/theme";

export default function TodayScreen() {
  const router = useRouter();
  const { company, loading: companyLoading, reload: reloadCompany, offline: companyOffline, error: companyError } = useCompany();
  const [open, setOpen] = useState<TimeEntry | null>(null);
  const [stops, setStops] = useState<Stop[]>([]);
  const [queued, setQueued] = useState<QueuedAction[]>([]);
  const [now, setNow] = useState(() => new Date());
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [onBreak, setOnBreak] = useState(false);
  const [cachedAt, setCachedAt] = useState<string | null>(null);
  const [noData, setNoData] = useState(false);
  const [problems, setProblems] = useState<Awaited<ReturnType<typeof syncProblems>>>([]);
  const [held, setHeld] = useState(0);
  const [photoFailures, setPhotoFailures] = useState(0);
  const acting = useRef(false); // blocks a double tap before the screen re-renders

  const refresh = useCallback(async () => {
    await flush().catch(() => undefined);
    const photos = await flushPhotos().catch(() => ({ failed: 0 }));
    if (photos.failed > 0) setPhotoFailures((n) => n + photos.failed);
    setQueued(await pendingActions());
    setProblems(await syncProblems());
    setHeld(await heldForOthers());
    if (!company?.employee_id) return;

    const today = companyToday(company.timezone);
    const [{ data: entry, error: entryError }, { data: sched, error }] = await Promise.all([
      supabase
        .from("time_entries")
        .select("id, tenant_id, employee_id, clock_in, clock_out, time_entry_breaks(id, ended_at)")
        .eq("employee_id", company.employee_id)
        .is("clock_out", null)
        .is("voided_at", null)
        .maybeSingle(),
      supabase.rpc("schedule", { p_tenant_id: company.tenant_id, p_from: today, p_to: today }),
    ]);
    if (!entryError && !error) {
      const e = (entry as (TimeEntry & { time_entry_breaks?: { ended_at: string | null }[] }) | null) ?? null;
      setOpen(e);
      setOnBreak(!!e?.time_entry_breaks?.some((b) => b.ended_at === null));
      setStops((sched ?? []) as Stop[]);
      setCachedAt(null);
      setNoData(false);
      await saveCache(company.tenant_id, "today", { day: today, entry: e, stops: sched ?? [] });
    } else {
      // No signal: show the last copy of today saved on this phone.
      const c = await readCache<{ day: string; entry: (TimeEntry & { time_entry_breaks?: { ended_at: string | null }[] }) | null; stops: Stop[] }>(company.tenant_id, "today");
      if (c && c.value.day === today) {
        setOpen(c.value.entry);
        setOnBreak(!!c.value.entry?.time_entry_breaks?.some((b) => b.ended_at === null));
        setStops(c.value.stops);
        setCachedAt(c.at);
        setNoData(false);
      } else {
        setNoData(true);
      }
    }
  }, [company]);

  useFocusEffect(
    useCallback(() => {
      refresh();
    }, [refresh]),
  );

  useEffect(() => {
    const tick = setInterval(() => setNow(new Date()), 15_000);
    // Signal comes back while the screen is open: keep trying to send what's waiting.
    const retry = setInterval(async () => {
      if ((await pendingActions()).length > 0) refresh();
    }, 45_000);
    const sub = AppState.addEventListener("change", (s) => s === "active" && refresh());
    return () => {
      clearInterval(tick);
      clearInterval(retry);
      sub.remove();
    };
  }, [refresh]);

  // Queued actions count immediately, so offline taps feel instant.
  const pending = pendingClockState(queued);
  const onClock = pending ? pending.onClock : open !== null;
  const since = pending?.onClock ? pending.since : open?.clock_in;
  const pendingBreak = pendingBreakState(queued);
  const breakNow = onClock && (pendingBreak ?? onBreak);

  async function onBreakTap() {
    if (!company || acting.current) return;
    acting.current = true;
    setBusy(true);
    setMessage(null);
    try {
      const result = await perform({ kind: breakNow ? "break_end" : "break_start", tenantId: company.tenant_id });
      if (result === "queued") setMessage("No signal. Saved on this phone; it will send automatically.");
    } catch (err) {
      setMessage(friendlyError(err));
    } finally {
      await refresh();
      setBusy(false);
      acting.current = false;
    }
  }

  async function onPunch() {
    if (!company || acting.current) return;
    acting.current = true;
    setBusy(true);
    setMessage(null);
    try {
      const result = await perform({ kind: onClock ? "out" : "in", tenantId: company.tenant_id });
      if (result === "queued") setMessage("No signal. Saved on this phone; it will send automatically.");
    } catch (err) {
      setMessage(friendlyError(err));
    } finally {
      await refresh();
      setBusy(false);
      acting.current = false;
    }
  }

  if (companyLoading) return <SafeAreaView style={styles.safe} />;

  if (!company && companyError && !companyOffline) {
    // No signal on first open and nothing saved yet: don't claim they have no team.
    return (
      <SafeAreaView style={styles.safe}>
        <View style={styles.center}>
          <Text style={styles.title}>No signal</Text>
          <Text style={styles.lede}>Crew needs signal once to load your company on this phone. After that it works without signal.</Text>
          <Pressable onPress={reloadCompany} accessibilityRole="button" style={styles.secondary}>
            <Text style={styles.secondaryText}>Try again</Text>
          </Pressable>
        </View>
      </SafeAreaView>
    );
  }

  if (!company) {
    return (
      <SafeAreaView style={styles.safe}>
        <View style={styles.center}>
          <Text style={styles.title}>You're not on a team yet</Text>
          <Text style={styles.lede}>Ask your manager to invite this email, then open the invite link on this phone.</Text>
          <Pressable onPress={reloadCompany} accessibilityRole="button" style={styles.secondary}>
            <Text style={styles.secondaryText}>Check again</Text>
          </Pressable>
          <Pressable onPress={() => router.push("/account")} accessibilityRole="button" hitSlop={12}>
            <Text style={styles.link}>Sign out or delete account</Text>
          </Pressable>
        </View>
      </SafeAreaView>
    );
  }

  const shown = stops.filter((s) => s.status !== "canceled");
  const remaining = shown.filter((s) => !["completed", "skipped", "canceled"].includes(pendingVisitStatus(queued, s.visit_id) ?? s.status));
  // The whole rest of the day in Google Maps, in the office's order, from where the phone is.
  const routeUrl = googleRouteUrl(
    remaining
      .map((s): NavTarget | null => (s.latitude != null && s.longitude != null ? { lat: s.latitude, lon: s.longitude } : s.address))
      .filter((t): t is NavTarget => !!t),
  );

  return (
    <SafeAreaView style={styles.safe} edges={["top"]}>
      <ScrollView contentContainerStyle={styles.body} refreshControl={<RefreshControl refreshing={false} onRefresh={refresh} />}>
        <View style={styles.top}>
          <Text style={styles.company}>{company.name}</Text>
          <Pressable onPress={() => router.push("/account")} accessibilityRole="button" hitSlop={12}>
            <Text style={styles.link}>Account</Text>
          </Pressable>
        </View>

        <View style={[styles.clock, onClock && styles.clockOn]}>
          <Text style={[styles.status, onClock && styles.onSoft]}>{onClock ? "On the clock" : "Off the clock"}</Text>
          <Text style={[styles.elapsed, onClock && styles.elapsedOn]}>
            {onClock && since ? formatDuration(elapsedSince(since, now)) : "0:00"}
          </Text>
          {onClock && since && <Text style={[styles.since, styles.onSoft]}>since {formatClockTime(since, company.timezone)}</Text>}
          {breakNow && <Text style={[styles.since, styles.onSoft]}>On break (unpaid)</Text>}
        </View>
        {onClock && (
          <Pressable accessibilityRole="button" disabled={busy} onPress={onBreakTap}
            style={({ pressed }) => [styles.breakBtn, (pressed || busy) && styles.pressed]}>
            <Text style={styles.breakText}>{breakNow ? "End break" : "Start break"}</Text>
          </Pressable>
        )}
        {cachedAt && (
          <Text style={styles.queued}>No signal. Showing today as saved at {formatClockTime(cachedAt, company.timezone)}.</Text>
        )}

        {message && <Text style={styles.message}>{message}</Text>}
        {queued.length > 0 && (
          <Text style={styles.queued}>{queued.length} {queued.length === 1 ? "update" : "updates"} waiting for signal</Text>
        )}
        {held > 0 && (
          <Text style={styles.queued}>
            {held} {held === 1 ? "update" : "updates"} saved by someone else on this phone will send when they sign back in.
          </Text>
        )}
        {photoFailures > 0 && (
          <Text style={styles.message}>
            {photoFailures === 1 ? "A photo" : `${photoFailures} photos`} couldn't be attached (the stop may have been removed). Tell the office.
          </Text>
        )}
        {problems.map((p) => (
          <View key={p.action.eventId} style={styles.problem}>
            <Text style={styles.problemText}>
              Not recorded: {labelFor(p.action.kind)} at {formatClockTime(p.action.at, company.timezone)}. {friendlyError(p.error)} {p.reported === false ? "The office will be told when there's signal." : "The office has been told."}
            </Text>
            <Pressable accessibilityRole="button" hitSlop={10} onPress={async () => { await dismissSyncProblem(p.action.eventId); setProblems(await syncProblems()); }}>
              <Text style={styles.link}>OK</Text>
            </Pressable>
          </View>
        ))}

        <View style={styles.stopsHead}>
          <Text style={styles.h2}>Today's stops</Text>
          {shown.length > 0 && <Text style={styles.count}>{remaining.length} left</Text>}
        </View>
        {shown.length === 0 ? (
          <Text style={styles.lede}>
            {noData ? "Couldn't load today's stops: no signal. Pull down to try again." : "No stops assigned to you today."}
          </Text>
        ) : (
          shown.map((s, i) => {
            const status = pendingVisitStatus(queued, s.visit_id) ?? s.status;
            return (
              <Link key={s.visit_id} href={{ pathname: "/visit/[id]", params: { id: s.visit_id } }} asChild>
                <Pressable accessibilityRole="button" style={({ pressed }) => [styles.stop, pressed && styles.pressed]}>
                  <Text style={[styles.stopNo, status === "completed" && styles.done]}>{i + 1}</Text>
                  <View style={styles.stopText}>
                    <Text style={[styles.stopTitle, status === "completed" && styles.doneText]} numberOfLines={1}>
                      {s.client_name ?? s.job_title}
                    </Text>
                    <Text style={styles.stopSub} numberOfLines={1}>{s.address ?? s.job_title}</Text>
                  </View>
                  <Text style={[styles.stopStatus, status === "in_progress" && styles.working]}>
                    {status === "completed" ? "Done" : status === "in_progress" ? "Working" : status === "skipped" ? "Not done" : ""}
                  </Text>
                </Pressable>
              </Link>
            );
          })
        )}
        {routeUrl && remaining.length > 1 && (
          <Pressable
            accessibilityRole="link"
            onPress={() => Linking.openURL(routeUrl)}
            style={({ pressed }) => [styles.routeBtn, pressed && styles.pressed]}
          >
            <Text style={styles.routeText}>Drive the route ({Math.min(remaining.length, 10)} stops)</Text>
          </Pressable>
        )}
      </ScrollView>

      <Pressable
        accessibilityRole="button"
        accessibilityLabel={onClock ? "Clock out" : "Clock in"}
        disabled={busy}
        onPress={onPunch}
        style={({ pressed }) => [styles.punch, onClock ? styles.punchOut : styles.punchIn, (pressed || busy) && styles.pressed]}
      >
        <Text style={[styles.punchText, onClock && styles.punchTextOut]}>{onClock ? "Clock out" : "Clock in"}</Text>
      </Pressable>
    </SafeAreaView>
  );
}

function labelFor(kind: string) {
  return ({ in: "Clock in", out: "Clock out", break_start: "Break start", break_end: "Break end",
    start_visit: "Start stop", complete_visit: "Finish stop", report_problem: "Couldn't do stop" } as Record<string, string>)[kind] ?? "Update";
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: color.daylight },
  body: { paddingHorizontal: 20, paddingTop: 8, paddingBottom: 24, gap: 14 },
  center: { flex: 1, justifyContent: "center", paddingHorizontal: 24, gap: 14 },
  top: { flexDirection: "row", justifyContent: "space-between", alignItems: "center" },
  company: { fontFamily: font.textBold, fontSize: 18, color: color.ink },
  title: { fontFamily: font.textBold, fontSize: 26, color: color.ink },
  lede: { fontFamily: font.text, fontSize: 17, color: color.inkSoft },
  link: { fontFamily: font.textMedium, fontSize: 16, color: color.turf },
  problem: { flexDirection: "row", gap: 12, alignItems: "flex-start", padding: 12, borderRadius: 10, backgroundColor: color.surface, borderWidth: 1, borderColor: color.error },
  problemText: { flex: 1, fontFamily: font.text, fontSize: 15, color: color.ink },

  clock: { borderRadius: 18, padding: 20, backgroundColor: color.surface, borderWidth: 1, borderColor: color.line },
  clockOn: { backgroundColor: color.ink, borderColor: color.ink },
  status: { fontFamily: font.textMedium, fontSize: 18, color: color.inkSoft },
  elapsed: { fontFamily: font.figure, fontSize: 84, lineHeight: 90, color: color.inkSoft, fontVariant: ["tabular-nums"] },
  elapsedOn: { color: color.hivis },
  since: { fontFamily: font.text, fontSize: 17 },
  onSoft: { color: "#B9C6B6" },
  message: { fontFamily: font.textMedium, fontSize: 16, color: color.ink },
  queued: { fontFamily: font.text, fontSize: 15, color: color.inkSoft },

  breakBtn: { minHeight: 52, borderRadius: 14, borderWidth: 1.5, borderColor: color.line, backgroundColor: color.surface, alignItems: "center", justifyContent: "center" },
  breakText: { fontFamily: font.textBold, fontSize: 17, color: color.ink },
  routeBtn: { minHeight: 56, borderRadius: 14, borderWidth: 1.5, borderColor: color.turf, alignItems: "center", justifyContent: "center", paddingHorizontal: 16 },
  routeText: { fontFamily: font.textBold, fontSize: 17, color: color.turf },
  stopsHead: { flexDirection: "row", justifyContent: "space-between", alignItems: "baseline", marginTop: 8 },
  h2: { fontFamily: font.textBold, fontSize: 22, color: color.ink },
  count: { fontFamily: font.textMedium, fontSize: 16, color: color.inkSoft },
  stop: {
    flexDirection: "row",
    alignItems: "center",
    gap: 14,
    minHeight: 72,
    paddingHorizontal: 16,
    borderRadius: 14,
    backgroundColor: color.surface,
    borderWidth: 1,
    borderColor: color.line,
  },
  stopNo: { fontFamily: font.figure, fontSize: 28, color: color.turf, width: 28, textAlign: "center" },
  done: { color: color.inkSoft },
  stopText: { flex: 1, gap: 2 },
  stopTitle: { fontFamily: font.textBold, fontSize: 18, color: color.ink },
  doneText: { color: color.inkSoft, textDecorationLine: "line-through" },
  stopSub: { fontFamily: font.text, fontSize: 15, color: color.inkSoft },
  stopStatus: { fontFamily: font.textBold, fontSize: 14, color: color.inkSoft },
  working: { color: color.ink, backgroundColor: color.hivis, paddingHorizontal: 8, paddingVertical: 2, borderRadius: 999, overflow: "hidden" },

  secondary: { minHeight: 52, borderRadius: 10, borderWidth: 1.5, borderColor: color.line, alignItems: "center", justifyContent: "center" },
  secondaryText: { fontFamily: font.textBold, fontSize: 17, color: color.ink },

  punch: { marginHorizontal: 16, marginVertical: 12, minHeight: 88, borderRadius: 20, alignItems: "center", justifyContent: "center" },
  punchIn: { backgroundColor: color.turf },
  punchOut: { backgroundColor: color.hivis },
  pressed: { opacity: 0.85 },
  punchText: { fontFamily: font.textBold, fontSize: 26, color: "#fff" },
  punchTextOut: { color: color.ink },
});

import { useCallback, useState } from "react";
import { Image, Linking, Platform, Pressable, ScrollView, StyleSheet, Text, TextInput, View } from "react-native";
import { useFocusEffect, useLocalSearchParams, useRouter } from "expo-router";
import { appleStopUrl, friendlyError, googleRouteUrl } from "@crew/shared";
import { supabase } from "../../lib/supabase";
import { companyToday, useCompany } from "../../lib/company";
import { pendingActions, pendingVisitStatus, perform } from "../../lib/actionQueue";
import type { Stop } from "../../lib/stops";
import { loadVisitPhotos, takeVisitPhoto, type PhotoKind, type VisitPhoto } from "../../lib/photos";
import { color, font } from "../../lib/theme";

function mapsUrl(stop: Stop) {
  const target = stop.latitude != null && stop.longitude != null ? { lat: stop.latitude, lon: stop.longitude } : stop.address ?? "";
  return Platform.OS === "ios" ? appleStopUrl(target) : googleRouteUrl([target])!;
}

export default function VisitScreen() {
  const { id } = useLocalSearchParams<{ id: string }>();
  const { company } = useCompany();
  const router = useRouter();
  const [stop, setStop] = useState<Stop | null>(null);
  const [status, setStatus] = useState<Stop["status"] | null>(null);
  const [notes, setNotes] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [photos, setPhotos] = useState<VisitPhoto[]>([]);
  const [uploading, setUploading] = useState(false);

  const load = useCallback(async () => {
    if (!company || !id) return;
    const today = companyToday(company.timezone);
    // Look a week either side so a stop opened from a notification still loads.
    const from = new Date(`${today}T12:00:00Z`);
    from.setUTCDate(from.getUTCDate() - 7);
    const to = new Date(`${today}T12:00:00Z`);
    to.setUTCDate(to.getUTCDate() + 7);
    const { data } = await supabase.rpc("schedule", {
      p_tenant_id: company.tenant_id,
      p_from: from.toISOString().slice(0, 10),
      p_to: to.toISOString().slice(0, 10),
    });
    const s = ((data ?? []) as Stop[]).find((x) => x.visit_id === id) ?? null;
    setStop(s);
    const queued = await pendingActions();
    setStatus(s ? (pendingVisitStatus(queued, s.visit_id) ?? s.status) : null);
    if (s) setPhotos(await loadVisitPhotos(s.visit_id).catch(() => []));
  }, [company, id]);

  async function addPhoto(kind: PhotoKind) {
    if (!company || !stop) return;
    setUploading(true);
    setMessage(null);
    try {
      if (await takeVisitPhoto(company.tenant_id, stop.visit_id, kind)) {
        setPhotos(await loadVisitPhotos(stop.visit_id));
        setMessage("Photo saved.");
      }
    } catch (err) {
      const m = err instanceof Error ? err.message : "";
      setMessage(m.includes("permission_denied") ? "Allow camera access in Settings to add photos."
        : m.toLowerCase().includes("network") ? "No signal. Try the photo again when you have service." : friendlyError(err));
    } finally {
      setUploading(false);
    }
  }

  useFocusEffect(
    useCallback(() => {
      load();
    }, [load]),
  );

  async function act(kind: "start_visit" | "complete_visit") {
    if (!company || !stop) return;
    setBusy(true);
    setMessage(null);
    try {
      const result = await perform(
        kind === "complete_visit"
          ? { kind, tenantId: company.tenant_id, visitId: stop.visit_id, notes: notes.trim() || undefined }
          : { kind, tenantId: company.tenant_id, visitId: stop.visit_id },
      );
      setStatus(kind === "complete_visit" ? "completed" : "in_progress");
      if (result === "queued") setMessage("No signal. Saved on this phone; it will send automatically.");
      else if (kind === "complete_visit") router.back();
    } catch (err) {
      setMessage(friendlyError(err));
    } finally {
      setBusy(false);
    }
  }

  if (!stop) {
    return (
      <View style={styles.center}>
        <Text style={styles.lede}>Loading this stop…</Text>
      </View>
    );
  }

  return (
    <ScrollView contentContainerStyle={styles.body} keyboardShouldPersistTaps="handled">
      <Text style={styles.client}>{stop.client_name ?? "Customer"}</Text>
      <Text style={styles.job}>{stop.job_title}</Text>

      {stop.address && (
        <Pressable accessibilityRole="link" onPress={() => Linking.openURL(mapsUrl(stop))} style={({ pressed }) => [styles.card, pressed && styles.pressed]}>
          <Text style={styles.cardLabel}>Address</Text>
          <Text style={styles.address}>{stop.address}</Text>
          <Text style={styles.action}>Get directions</Text>
        </Pressable>
      )}

      {stop.access_notes && (
        <View style={[styles.card, styles.notice]}>
          <Text style={styles.cardLabel}>Gate and access</Text>
          <Text style={styles.body16}>{stop.access_notes}</Text>
        </View>
      )}

      <View style={styles.row}>
        {stop.client_phone && (
          <Pressable accessibilityRole="button" onPress={() => Linking.openURL(`tel:${stop.client_phone}`)} style={styles.half}>
            <Text style={styles.halfText}>Call customer</Text>
          </Pressable>
        )}
        {stop.est_minutes != null && (
          <View style={[styles.half, styles.static]}>
            <Text style={styles.halfText}>About {stop.est_minutes} min</Text>
          </View>
        )}
      </View>

      {stop.assignees.length > 0 && <Text style={styles.lede}>Also on this stop: {stop.assignees.join(", ")}</Text>}

      {status !== "canceled" && (
        <View style={styles.card}>
          <Text style={styles.cardLabel}>Photos {photos.length > 0 ? `(${photos.length})` : ""}</Text>
          {photos.length > 0 && (
            <ScrollView horizontal contentContainerStyle={styles.thumbs} showsHorizontalScrollIndicator={false}>
              {photos.map((p) => (
                <View key={p.id} style={styles.thumbWrap}>
                  {p.url ? <Image source={{ uri: p.url }} style={styles.thumb} accessibilityLabel={`${p.kind} photo`} /> : <View style={styles.thumb} />}
                  <Text style={styles.thumbLabel}>{p.kind === "issue" ? "Problem" : p.kind === "before" ? "Before" : "After"}</Text>
                </View>
              ))}
            </ScrollView>
          )}
          <View style={styles.row}>
            {(["before", "after", "issue"] as PhotoKind[]).map((k) => (
              <Pressable key={k} accessibilityRole="button" disabled={uploading} onPress={() => addPhoto(k)}
                style={({ pressed }) => [styles.photoBtn, (pressed || uploading) && styles.pressed]}>
                <Text style={styles.photoBtnText}>{k === "issue" ? "Problem" : k === "before" ? "Before" : "After"}</Text>
              </Pressable>
            ))}
          </View>
          {uploading && <Text style={styles.lede}>Uploading…</Text>}
        </View>
      )}

      {status === "completed" ? (
        <View style={[styles.card, styles.doneCard]}>
          <Text style={styles.doneText}>Done</Text>
          {stop.completion_notes && <Text style={styles.body16}>{stop.completion_notes}</Text>}
        </View>
      ) : status === "skipped" || status === "canceled" ? (
        <View style={styles.card}>
          <Text style={styles.body16}>This stop was {status}{stop.status_reason ? `: ${stop.status_reason}` : "."}</Text>
        </View>
      ) : (
        <>
          <Text style={styles.cardLabel}>Notes for the office (optional)</Text>
          <TextInput
            accessibilityLabel="Notes for the office"
            multiline
            value={notes}
            onChangeText={setNotes}
            placeholder="e.g. Gate latch is loose, back fence damaged"
            placeholderTextColor={color.inkSoft}
            style={styles.input}
            maxLength={2000}
          />
          {message && <Text style={styles.message}>{message}</Text>}
          {status === "scheduled" && (
            <Pressable accessibilityRole="button" disabled={busy} onPress={() => act("start_visit")}
              style={({ pressed }) => [styles.secondary, (pressed || busy) && styles.pressed]}>
              <Text style={styles.secondaryText}>Start this stop</Text>
            </Pressable>
          )}
          <Pressable accessibilityRole="button" disabled={busy} onPress={() => act("complete_visit")}
            style={({ pressed }) => [styles.primary, (pressed || busy) && styles.pressed]}>
            <Text style={styles.primaryText}>Mark done</Text>
          </Pressable>
        </>
      )}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  center: { flex: 1, alignItems: "center", justifyContent: "center", backgroundColor: color.daylight },
  body: { padding: 20, gap: 14, backgroundColor: color.daylight, flexGrow: 1 },
  client: { fontFamily: font.textBold, fontSize: 28, color: color.ink },
  job: { fontFamily: font.textMedium, fontSize: 18, color: color.inkSoft, marginTop: -8 },
  card: { padding: 16, borderRadius: 14, backgroundColor: color.surface, borderWidth: 1, borderColor: color.line, gap: 4 },
  notice: { backgroundColor: "#FBF8E3", borderColor: "#E9E2A6" },
  cardLabel: { fontFamily: font.textBold, fontSize: 14, color: color.inkSoft },
  address: { fontFamily: font.textBold, fontSize: 20, color: color.ink },
  action: { fontFamily: font.textBold, fontSize: 16, color: color.turf, marginTop: 6 },
  body16: { fontFamily: font.text, fontSize: 16, color: color.ink },
  lede: { fontFamily: font.text, fontSize: 16, color: color.inkSoft },
  row: { flexDirection: "row", gap: 10 },
  half: { flex: 1, minHeight: 56, borderRadius: 12, borderWidth: 1.5, borderColor: color.line, alignItems: "center", justifyContent: "center", backgroundColor: color.surface },
  static: { borderStyle: "dashed" },
  halfText: { fontFamily: font.textBold, fontSize: 16, color: color.ink },
  input: {
    minHeight: 96,
    borderWidth: 1.5,
    borderColor: color.line,
    borderRadius: 12,
    backgroundColor: color.surface,
    padding: 14,
    fontFamily: font.text,
    fontSize: 17,
    color: color.ink,
    textAlignVertical: "top",
  },
  message: { fontFamily: font.textMedium, fontSize: 16, color: color.ink },
  secondary: { minHeight: 64, borderRadius: 16, borderWidth: 2, borderColor: color.turf, alignItems: "center", justifyContent: "center" },
  secondaryText: { fontFamily: font.textBold, fontSize: 20, color: color.turf },
  primary: { minHeight: 80, borderRadius: 18, backgroundColor: color.turf, alignItems: "center", justifyContent: "center" },
  primaryText: { fontFamily: font.textBold, fontSize: 24, color: "#fff" },
  doneCard: { backgroundColor: "#EEF3EA" },
  doneText: { fontFamily: font.textBold, fontSize: 22, color: color.turf },
  pressed: { opacity: 0.85 },
  thumbs: { gap: 10, paddingVertical: 6 },
  thumbWrap: { alignItems: "center", gap: 4 },
  thumb: { width: 96, height: 96, borderRadius: 10, backgroundColor: color.line },
  thumbLabel: { fontFamily: font.textMedium, fontSize: 13, color: color.inkSoft },
  photoBtn: { flex: 1, minHeight: 52, borderRadius: 12, borderWidth: 1.5, borderColor: color.turf, alignItems: "center", justifyContent: "center" },
  photoBtnText: { fontFamily: font.textBold, fontSize: 16, color: color.turf },
});

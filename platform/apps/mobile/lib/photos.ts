import AsyncStorage from "@react-native-async-storage/async-storage";
import * as ImagePicker from "expo-image-picker";
import * as ImageManipulator from "expo-image-manipulator";
import * as FileSystem from "expo-file-system/legacy";
import * as Crypto from "expo-crypto";
import { supabase } from "./supabase";

export type PhotoKind = "before" | "after" | "issue";

export interface VisitPhoto {
  id: string;
  kind: string;
  caption: string | null;
  storage_path: string;
  customer_visible: boolean;
  url?: string;
  pending?: boolean;
}

/** Photos waiting for signal. Files live in the app's own folder until sent. */
interface QueuedPhoto {
  tenantId: string;
  visitId: string;
  kind: PhotoKind;
  path: string; // storage path, decided once so retries never duplicate
  fileUri: string;
  at: string;
  owner?: string | null; // only sent while the person who took it is signed in
}

const QUEUE_KEY = "crew.photoQueue.v1";
const DIR = `${FileSystem.documentDirectory ?? ""}photo-queue/`;

async function loadQueue(): Promise<QueuedPhoto[]> {
  const raw = await AsyncStorage.getItem(QUEUE_KEY);
  return raw ? (JSON.parse(raw) as QueuedPhoto[]) : [];
}
async function saveQueue(q: QueuedPhoto[]) {
  await AsyncStorage.setItem(QUEUE_KEY, JSON.stringify(q));
}

export async function pendingPhotos(visitId?: string): Promise<QueuedPhoto[]> {
  const q = await loadQueue();
  return visitId ? q.filter((p) => p.visitId === visitId) : q;
}

/** Photos on a visit (crew see their own visits' photos; the database checks). */
export async function loadVisitPhotos(visitId: string): Promise<VisitPhoto[]> {
  const waiting = (await pendingPhotos(visitId)).map<VisitPhoto>((p) => ({
    id: p.path, kind: p.kind, caption: null, storage_path: p.path, customer_visible: false, url: p.fileUri, pending: true,
  }));
  const { data } = await supabase
    .from("visit_photos")
    .select("id, kind, caption, storage_path, customer_visible")
    .eq("visit_id", visitId)
    .is("hidden_at", null)
    .order("taken_at");
  const rows = (data ?? []) as VisitPhoto[];
  if (!rows.length) return waiting;
  const { data: signed } = await supabase.storage.from("visit-photos").createSignedUrls(rows.map((r) => r.storage_path), 600);
  return [...rows.map((r) => ({ ...r, url: signed?.find((s) => s.path === r.storage_path)?.signedUrl ?? undefined })), ...waiting];
}

/**
 * Take a photo for the visit. It's shrunk on the phone (about 1600 px, JPEG),
 * kept in the app's folder, and sent when there's signal. Returns false if the
 * person cancelled, "sent" or "queued" otherwise.
 */
export async function takeVisitPhoto(tenantId: string, visitId: string, kind: PhotoKind): Promise<false | "sent" | "queued"> {
  const perm = await ImagePicker.requestCameraPermissionsAsync();
  if (!perm.granted) throw new Error("camera_permission_denied");
  const result = await ImagePicker.launchCameraAsync({ mediaTypes: ["images"], quality: 0.8, exif: false });
  if (result.canceled || !result.assets[0]) return false;
  const asset = result.assets[0];

  const small = await ImageManipulator.manipulateAsync(
    asset.uri,
    asset.width > 1600 ? [{ resize: { width: 1600 } }] : [],
    { compress: 0.6, format: ImageManipulator.SaveFormat.JPEG },
  );
  await FileSystem.makeDirectoryAsync(DIR, { intermediates: true }).catch(() => {});
  const name = `${kind}-${Crypto.randomUUID()}.jpg`;
  const fileUri = `${DIR}${name}`;
  await FileSystem.moveAsync({ from: small.uri, to: fileUri });

  const path = `${tenantId}/${visitId}/${name}`;
  await withLock(async () => {
    const q = await loadQueue();
    q.push({ tenantId, visitId, kind, path, fileUri, at: new Date().toISOString(), owner: await currentUser() });
    await saveQueue(q);
  });
  await flushPhotos();
  return (await loadQueue()).some((p) => p.path === path) ? "queued" : "sent";
}

const isNetwork = (e: unknown) => /network|fetch|timeout|offline/i.test(e instanceof Error ? e.message : String((e as { message?: unknown })?.message ?? e));

const currentUser = async () => (await supabase.auth.getSession()).data.session?.user.id ?? null;

// One change to the photo list at a time; a second send waits for the first.
let lock: Promise<unknown> = Promise.resolve();
function withLock<T>(fn: () => Promise<T>): Promise<T> {
  const run = lock.then(fn, fn);
  lock = run.catch(() => undefined);
  return run;
}
let sending: Promise<{ failed: number }> | null = null;

/** Send waiting photos, oldest first; stop at the first connectivity failure. Concurrent calls share one run. */
export function flushPhotos(): Promise<{ failed: number }> {
  if (!sending) sending = drainPhotos().finally(() => { sending = null; });
  return sending;
}

async function drainPhotos(): Promise<{ failed: number }> {
  let failed = 0;
  const me = await currentUser();
  if (!me) return { failed };
  for (;;) {
    const p = (await loadQueue()).find((x) => x.owner === undefined || x.owner === me);
    if (!p) break;
    let drop = false;
    try {
      const body = await (await fetch(p.fileUri)).arrayBuffer();
      const up = await supabase.storage.from("visit-photos").upload(p.path, body, { contentType: "image/jpeg", upsert: false });
      // "Already exists" = an earlier try got through before losing signal.
      if (up.error && !/exists|duplicate/i.test(up.error.message)) throw up.error;
      const { error } = await supabase.rpc("add_visit_photo", { p_visit_id: p.visitId, p_path: p.path, p_kind: p.kind });
      if (error) throw error;
      await FileSystem.deleteAsync(p.fileUri, { idempotent: true });
      drop = true;
    } catch (e) {
      if (isRetry(e)) break;
      failed++; // refused for good (e.g. visit removed): drop it from the list, keep the file on the phone
      drop = true;
    }
    if (drop) {
      // Re-read under the lock so a photo taken meanwhile is never lost.
      await withLock(async () => saveQueue((await loadQueue()).filter((x) => x.path !== p.path)));
    }
  }
  return { failed };
}

function isRetry(e: unknown) {
  const err = e as { message?: unknown; status?: number; statusCode?: string };
  const msg = e instanceof Error ? e.message : String(err?.message ?? e);
  if (isNetwork(e) || /JWT|expired|not_authenticated|Internal Server|Bad Gateway|Service Unavailable/i.test(msg)) return true;
  const status = Number(err?.status ?? err?.statusCode ?? 0);
  return status >= 500 || status === 401 || status === 429;
}

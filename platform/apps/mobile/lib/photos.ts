import * as ImagePicker from "expo-image-picker";
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
}

/** Photos on a visit (crew see their own visits' photos; the database checks). */
export async function loadVisitPhotos(visitId: string): Promise<VisitPhoto[]> {
  const { data } = await supabase
    .from("visit_photos")
    .select("id, kind, caption, storage_path, customer_visible")
    .eq("visit_id", visitId)
    .is("hidden_at", null)
    .order("taken_at");
  const rows = (data ?? []) as VisitPhoto[];
  if (!rows.length) return rows;
  const { data: signed } = await supabase.storage.from("visit-photos").createSignedUrls(rows.map((r) => r.storage_path), 600);
  return rows.map((r) => ({ ...r, url: signed?.find((s) => s.path === r.storage_path)?.signedUrl ?? undefined }));
}

/**
 * Take a photo and attach it to the visit. Returns false if the person cancelled.
 * Needs a signal: the file goes straight to storage.
 */
export async function takeVisitPhoto(tenantId: string, visitId: string, kind: PhotoKind, fromLibrary = false): Promise<boolean> {
  const perm = fromLibrary ? await ImagePicker.requestMediaLibraryPermissionsAsync() : await ImagePicker.requestCameraPermissionsAsync();
  if (!perm.granted) throw new Error(fromLibrary ? "photos_permission_denied" : "camera_permission_denied");
  const opts: ImagePicker.ImagePickerOptions = { mediaTypes: ["images"], quality: 0.6, exif: false };
  const result = fromLibrary ? await ImagePicker.launchImageLibraryAsync(opts) : await ImagePicker.launchCameraAsync(opts);
  if (result.canceled || !result.assets[0]) return false;
  const asset = result.assets[0];
  const type = asset.mimeType ?? "image/jpeg";
  const ext = type === "image/png" ? "png" : type.includes("heic") ? "heic" : "jpg";
  const path = `${tenantId}/${visitId}/${kind}-${Crypto.randomUUID()}.${ext}`;
  const body = await (await fetch(asset.uri)).arrayBuffer();
  const { error: upErr } = await supabase.storage.from("visit-photos").upload(path, body, { contentType: type, upsert: false });
  if (upErr) throw upErr;
  const { error } = await supabase.rpc("add_visit_photo", { p_visit_id: visitId, p_path: path, p_kind: kind });
  if (error) throw error;
  return true;
}

import { useState } from "react";
import { Alert, Linking, Pressable, ScrollView, StyleSheet, Text, TextInput, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { friendlyError } from "@crew/shared";
import { supabase } from "../lib/supabase";
import { pendingActions } from "../lib/actionQueue";
import { pendingPhotos } from "../lib/photos";
import { color, font } from "../lib/theme";

const SITE = process.env.EXPO_PUBLIC_SITE_URL?.replace(/\/$/, "");

/** Sign out, or delete this sign-in. Work history stays with the company. */
export default function Account() {
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);

  async function remove() {
    setMessage(null);
    if (confirm.trim() !== "DELETE") return setMessage("Type DELETE to confirm.");
    const waiting = (await pendingActions()).length + (await pendingPhotos()).length;
    if (waiting > 0) {
      return setMessage("This phone still has work that hasn't been sent. Get signal and open Today so it sends, then try again.");
    }
    Alert.alert("Delete your account?", "Your access to every company ends now. Signing in again later starts a brand-new, empty account.", [
      { text: "Cancel", style: "cancel" },
      {
        text: "Delete",
        style: "destructive",
        onPress: async () => {
          setBusy(true);
          const { error } = await supabase.rpc("delete_my_account", { p_confirm: "DELETE" });
          if (error) {
            setBusy(false);
            return setMessage(friendlyError(error));
          }
          // Only this person's saved copies; anyone else's waiting work on a shared phone stays.
          const uid = (await supabase.auth.getSession()).data.session?.user.id;
          if (uid) await AsyncStorage.removeItem(`crew.companies.${uid}`).catch(() => {});
          await supabase.auth.signOut({ scope: "local" });
        },
      },
    ]);
  }

  async function signOut() {
    const waiting = (await pendingActions()).length + (await pendingPhotos()).length;
    if (waiting === 0) return supabase.auth.signOut();
    Alert.alert(
      "Updates still waiting",
      `${waiting} ${waiting === 1 ? "update hasn't" : "updates haven't"} sent yet. They stay on this phone and send the next time you sign in here.`,
      [{ text: "Stay signed in", style: "cancel" }, { text: "Sign out", onPress: () => supabase.auth.signOut() }],
    );
  }

  return (
    <SafeAreaView style={styles.safe} edges={["bottom"]}>
      <ScrollView contentContainerStyle={styles.body}>
        <Pressable onPress={signOut} accessibilityRole="button" style={styles.secondary}>
          <Text style={styles.secondaryText}>Sign out</Text>
        </Pressable>

        <View style={styles.danger}>
          <Text style={styles.heading}>Delete my account</Text>
          <Text style={styles.lede}>
            This removes your sign-in and your access to every company. The hours, stops and photos you recorded stay
            with your employer, because they're part of their payroll and job records.
          </Text>
          <TextInput
            value={confirm}
            onChangeText={setConfirm}
            placeholder="Type DELETE"
            autoCapitalize="characters"
            autoCorrect={false}
            style={styles.input}
            accessibilityLabel="Type DELETE to confirm"
          />
          {message && <Text style={styles.error}>{message}</Text>}
          <Pressable onPress={remove} disabled={busy} accessibilityRole="button" style={[styles.deleteButton, busy && styles.dim]}>
            <Text style={styles.deleteText}>{busy ? "Deleting…" : "Delete my account"}</Text>
          </Pressable>
        </View>
        {SITE && (
          <View style={styles.legal}>
            <Pressable accessibilityRole="link" onPress={() => Linking.openURL(`${SITE}/privacy`)} hitSlop={8}>
              <Text style={styles.link}>Privacy policy</Text>
            </Pressable>
            <Pressable accessibilityRole="link" onPress={() => Linking.openURL(`${SITE}/terms`)} hitSlop={8}>
              <Text style={styles.link}>Terms</Text>
            </Pressable>
          </View>
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: color.daylight },
  body: { padding: 20, gap: 24 },
  secondary: { minHeight: 52, borderRadius: 10, borderWidth: 1.5, borderColor: color.line, alignItems: "center", justifyContent: "center", backgroundColor: color.surface },
  secondaryText: { fontFamily: font.textBold, fontSize: 17, color: color.ink },
  danger: { gap: 12, paddingTop: 8 },
  heading: { fontFamily: font.textBold, fontSize: 20, color: color.ink },
  lede: { fontFamily: font.text, fontSize: 16, color: color.inkSoft, lineHeight: 22 },
  input: { minHeight: 52, borderWidth: 1.5, borderColor: color.line, borderRadius: 10, backgroundColor: color.surface, paddingHorizontal: 14, fontFamily: font.text, fontSize: 17, color: color.ink },
  error: { fontFamily: font.textMedium, fontSize: 15, color: color.error },
  deleteButton: { minHeight: 52, borderRadius: 10, backgroundColor: color.error, alignItems: "center", justifyContent: "center" },
  deleteText: { fontFamily: font.textBold, fontSize: 17, color: "#fff" },
  dim: { opacity: 0.6 },
  legal: { flexDirection: "row", gap: 24, justifyContent: "center", paddingTop: 8 },
  link: { fontFamily: font.textMedium, fontSize: 16, color: color.turf },
});

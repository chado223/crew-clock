import { useEffect, useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useLocalSearchParams, useRouter } from "expo-router";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { friendlyError } from "@crew/shared";
import { supabase } from "../../lib/supabase";
import { useCompany } from "../../lib/company";
import { PENDING_INVITE_KEY } from "../../lib/invite";
import { color, font } from "../../lib/theme";

/**
 * Opened from an invite link (crew://invite/<token>, or the web link on a phone).
 * Signed out: remember the invite, sign in, then come back here automatically.
 */
export default function InviteScreen() {
  const { token } = useLocalSearchParams<{ token: string }>();
  const router = useRouter();
  const { reload } = useCompany();
  const [email, setEmail] = useState<string | null | undefined>(undefined);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    supabase.auth.getUser().then(async ({ data }) => {
      if (!data.user) {
        if (token) await AsyncStorage.setItem(PENDING_INVITE_KEY, token);
        router.replace("/login");
        return;
      }
      setEmail(data.user.email ?? null);
    });
  }, [token, router]);

  async function accept() {
    if (!token) return;
    setBusy(true);
    setError(null);
    const { error } = await supabase.rpc("accept_invitation", { p_token: token });
    setBusy(false);
    if (error) {
      setError(friendlyError(error));
      return;
    }
    await AsyncStorage.removeItem(PENDING_INVITE_KEY);
    await reload();
    router.replace("/");
  }

  async function dismiss() {
    await AsyncStorage.removeItem(PENDING_INVITE_KEY);
    router.replace("/");
  }

  if (email === undefined) return <SafeAreaView style={styles.safe} />;

  return (
    <SafeAreaView style={styles.safe}>
      <View style={styles.body}>
        <Text style={styles.title}>Join your team</Text>
        <Text style={styles.lede}>
          You're signed in as <Text style={styles.strong}>{email}</Text>. Accept to join the company that invited you and start clocking in.
        </Text>
        {error && <Text style={styles.error}>{error}</Text>}
        <Pressable accessibilityRole="button" disabled={busy} onPress={accept} style={({ pressed }) => [styles.primary, (pressed || busy) && styles.pressed]}>
          <Text style={styles.primaryText}>Accept invite</Text>
        </Pressable>
        <Pressable accessibilityRole="button" onPress={dismiss} hitSlop={12}>
          <Text style={styles.link}>Not now</Text>
        </Pressable>
      </View>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: color.daylight },
  body: { flex: 1, justifyContent: "center", padding: 24, gap: 16 },
  title: { fontFamily: font.textBold, fontSize: 30, color: color.ink },
  lede: { fontFamily: font.text, fontSize: 17, color: color.inkSoft },
  strong: { fontFamily: font.textBold, color: color.ink },
  error: { fontFamily: font.textMedium, fontSize: 16, color: color.error },
  primary: { minHeight: 64, borderRadius: 16, backgroundColor: color.turf, alignItems: "center", justifyContent: "center" },
  primaryText: { fontFamily: font.textBold, fontSize: 20, color: "#fff" },
  link: { fontFamily: font.textMedium, fontSize: 16, color: color.turf, textAlign: "center" },
  pressed: { opacity: 0.85 },
});

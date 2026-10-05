import { useEffect, useState } from "react";
import { Stack, useRouter, useSegments } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { useFonts, Barlow_400Regular, Barlow_500Medium, Barlow_600SemiBold } from "@expo-google-fonts/barlow";
import { BarlowCondensed_600SemiBold } from "@expo-google-fonts/barlow-condensed";
import type { Session } from "@supabase/supabase-js";
import { supabase } from "../lib/supabase";
import { color } from "../lib/theme";

export default function RootLayout() {
  const [fontsLoaded] = useFonts({ Barlow_400Regular, Barlow_500Medium, Barlow_600SemiBold, BarlowCondensed_600SemiBold });
  const [session, setSession] = useState<Session | null | undefined>(undefined);
  const segments = useSegments();
  const router = useRouter();

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => setSession(data.session));
    const { data } = supabase.auth.onAuthStateChange((_event, s) => setSession(s));
    return () => data.subscription.unsubscribe();
  }, []);

  useEffect(() => {
    if (session === undefined) return;
    const onLogin = segments[0] === "login";
    if (!session && !onLogin) router.replace("/login");
    if (session && onLogin) router.replace("/");
  }, [session, segments, router]);

  if (!fontsLoaded || session === undefined) return null;

  return (
    <>
      <StatusBar style="dark" />
      <Stack screenOptions={{ headerShown: false, contentStyle: { backgroundColor: color.daylight } }} />
    </>
  );
}

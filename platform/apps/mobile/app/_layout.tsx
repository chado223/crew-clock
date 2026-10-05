import { useEffect, useState } from "react";
import { Stack, useRouter, useSegments } from "expo-router";
import { StatusBar } from "expo-status-bar";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { useFonts, Barlow_400Regular, Barlow_500Medium, Barlow_600SemiBold } from "@expo-google-fonts/barlow";
import { BarlowCondensed_600SemiBold } from "@expo-google-fonts/barlow-condensed";
import type { Session } from "@supabase/supabase-js";
import { supabase } from "../lib/supabase";
import { CompanyProvider } from "../lib/company";
import { color, font } from "../lib/theme";
import { PENDING_INVITE_KEY } from "../lib/invite";

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
    const area = segments[0];
    if (!session && area !== "login" && area !== "invite") {
      router.replace("/login");
      return;
    }
    if (session && area === "login") {
      // Finish an invite that was opened before signing in.
      AsyncStorage.getItem(PENDING_INVITE_KEY).then((token) => {
        if (token) router.replace({ pathname: "/invite/[token]", params: { token } });
        else router.replace("/");
      });
    }
  }, [session, segments, router]);

  if (!fontsLoaded || session === undefined) return null;

  return (
    <CompanyProvider key={session?.user.id ?? "signed-out"}>
      <StatusBar style="dark" />
      <Stack
        screenOptions={{
          headerShown: false,
          contentStyle: { backgroundColor: color.daylight },
          headerTitleStyle: { fontFamily: font.textBold },
          headerTintColor: color.turf,
        }}
      >
        <Stack.Screen name="visit/[id]" options={{ headerShown: true, title: "Stop", headerBackTitle: "Today" }} />
      </Stack>
    </CompanyProvider>
  );
}

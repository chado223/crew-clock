import { Tabs } from "expo-router";
import { color, font } from "../../lib/theme";

export default function TabsLayout() {
  return (
    <Tabs
      screenOptions={{
        headerShown: false,
        tabBarActiveTintColor: color.turf,
        tabBarInactiveTintColor: color.inkSoft,
        tabBarLabelStyle: { fontFamily: font.textBold, fontSize: 14 },
        tabBarIconStyle: { display: "none" },
        tabBarStyle: { minHeight: 64, paddingBottom: 10, paddingTop: 10, backgroundColor: color.surface, borderTopColor: color.line },
      }}
    >
      <Tabs.Screen name="index" options={{ title: "Today" }} />
      <Tabs.Screen name="week" options={{ title: "My week" }} />
    </Tabs>
  );
}

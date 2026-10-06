import AsyncStorage from "@react-native-async-storage/async-storage";

/**
 * Last good copy of what the crew member needs in the field (today's stops,
 * their open shift), so the app still shows the day with no signal.
 */
const KEY = (tenantId: string, name: string) => `crew.cache.${tenantId}.${name}`;

export async function saveCache<T>(tenantId: string, name: string, value: T) {
  try {
    await AsyncStorage.setItem(KEY(tenantId, name), JSON.stringify({ at: new Date().toISOString(), value }));
  } catch {
    /* storage full or unavailable: the screen still works online */
  }
}

export async function readCache<T>(tenantId: string, name: string): Promise<{ at: string; value: T } | null> {
  try {
    const raw = await AsyncStorage.getItem(KEY(tenantId, name));
    return raw ? (JSON.parse(raw) as { at: string; value: T }) : null;
  } catch {
    return null;
  }
}

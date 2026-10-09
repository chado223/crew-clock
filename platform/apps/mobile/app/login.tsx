import { useState } from "react";
import { KeyboardAvoidingView, Platform, Pressable, StyleSheet, Text, TextInput } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { supabase } from "../lib/supabase";
import { color, font } from "../lib/theme";

/** Email + 6-digit code. No passwords for crews to forget. */
export default function Login() {
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");
  const [step, setStep] = useState<"email" | "code">("email");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function sendCode() {
    const value = email.trim().toLowerCase();
    if (!/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(value)) return setError("Enter a valid email address.");
    setBusy(true);
    setError(null);
    const { error } = await supabase.auth.signInWithOtp({ email: value, options: { shouldCreateUser: true } });
    setBusy(false);
    if (error) return setError("We couldn't send the code. Wait a minute and try again.");
    setStep("code");
  }

  async function verify() {
    setBusy(true);
    setError(null);
    const { error } = await supabase.auth.verifyOtp({ email: email.trim().toLowerCase(), token: code.trim(), type: "email" });
    setBusy(false);
    if (error) setError("That code didn't work. Check it, or send a new one.");
  }

  return (
    <SafeAreaView style={styles.safe}>
      <KeyboardAvoidingView behavior={Platform.OS === "ios" ? "padding" : undefined} style={styles.body}>
        <Text style={styles.brand}>Crew</Text>
        <Text style={styles.title}>{step === "email" ? "Sign in" : "Enter your code"}</Text>
        <Text style={styles.lede}>
          {step === "email" ? "We'll email you a 6-digit code." : `We sent a code to ${email.trim().toLowerCase()}.`}
        </Text>

        {step === "email" ? (
          <TextInput
            accessibilityLabel="Work email"
            placeholder="you@company.com"
            placeholderTextColor={color.inkSoft}
            autoCapitalize="none"
            autoComplete="email"
            keyboardType="email-address"
            value={email}
            onChangeText={setEmail}
            style={styles.input}
          />
        ) : (
          <TextInput
            accessibilityLabel="Sign-in code from the email"
            keyboardType="number-pad"
            autoComplete="one-time-code"
            textContentType="oneTimeCode"
            maxLength={10} // the project sets 6; never cut off a longer code if that setting changes
            value={code}
            onChangeText={setCode}
            style={[styles.input, styles.codeInput]}
          />
        )}

        {error && <Text style={styles.error}>{error}</Text>}

        <Pressable
          accessibilityRole="button"
          disabled={busy}
          onPress={step === "email" ? sendCode : verify}
          style={({ pressed }) => [styles.button, (pressed || busy) && styles.pressed]}
        >
          <Text style={styles.buttonText}>{step === "email" ? "Send code" : "Sign in"}</Text>
        </Pressable>

        {step === "code" && (
          <Pressable accessibilityRole="button" onPress={() => { setStep("email"); setCode(""); }}>
            <Text style={styles.link}>Use a different email</Text>
          </Pressable>
        )}
      </KeyboardAvoidingView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safe: { flex: 1, backgroundColor: color.daylight },
  body: { flex: 1, justifyContent: "center", paddingHorizontal: 24, gap: 16 },
  brand: { fontFamily: font.figure, fontSize: 26, color: color.turf },
  title: { fontFamily: font.textBold, fontSize: 30, color: color.ink },
  lede: { fontFamily: font.text, fontSize: 17, color: color.inkSoft },
  input: {
    minHeight: 56,
    borderWidth: 1.5,
    borderColor: color.line,
    borderRadius: 10,
    backgroundColor: color.surface,
    paddingHorizontal: 16,
    fontFamily: font.text,
    fontSize: 18,
    color: color.ink,
  },
  codeInput: { fontFamily: font.figure, fontSize: 32, letterSpacing: 8, textAlign: "center" },
  error: { fontFamily: font.textMedium, fontSize: 16, color: color.error },
  button: { minHeight: 56, borderRadius: 10, backgroundColor: color.turf, alignItems: "center", justifyContent: "center" },
  pressed: { opacity: 0.8 },
  buttonText: { fontFamily: font.textBold, fontSize: 18, color: "#fff" },
  link: { fontFamily: font.textMedium, fontSize: 16, color: color.turf, textAlign: "center", paddingVertical: 8 },
});

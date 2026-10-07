"use client";
import { useEffect } from "react";

/**
 * One guard for every form in the app: a second submit of the same form within
 * a few seconds (double-click, impatient re-tap) is dropped before React or the
 * browser sees it, so "Add job", "Record payment", "Add expense" etc. can't
 * create duplicates. The button shows it's working meanwhile.
 */
const WINDOW_MS = 4000;

export function SubmitGuard() {
  useEffect(() => {
    const last = new WeakMap<HTMLFormElement, number>();
    function onSubmit(e: SubmitEvent) {
      const form = e.target as HTMLFormElement;
      if (form.dataset.allowRepeat !== undefined) return;
      const now = Date.now();
      const prev = last.get(form);
      if (prev && now - prev < WINDOW_MS) {
        e.preventDefault();
        e.stopPropagation();
        return;
      }
      last.set(form, now);
      const btn = e.submitter as HTMLButtonElement | null;
      if (btn) {
        btn.setAttribute("aria-busy", "true");
        setTimeout(() => btn.removeAttribute("aria-busy"), WINDOW_MS);
      }
    }
    document.addEventListener("submit", onSubmit, true);
    return () => document.removeEventListener("submit", onSubmit, true);
  }, []);
  return null;
}

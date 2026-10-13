import { test } from "node:test";
import assert from "node:assert/strict";
import { publicUrl, safeNext } from "../src/urls.ts";

test("same-site paths pass", () => {
  assert.equal(safeNext("/clients?status=lead"), "/clients?status=lead");
  assert.equal(safeNext("/portal#x"), "/portal#x");
});

test("anything that could leave the site falls back", () => {
  for (const bad of ["//evil.com", "/\t/evil.com", "/\n/evil.com", "/\\evil.com", "https://evil.com", "evil.com", "/%09/evil.com".replace("%09", "\t"), "", null, undefined]) {
    assert.equal(safeNext(bad as string), "/", String(bad));
  }
  assert.equal(safeNext("//evil.com", "/portal"), "/portal");
});


test("redirects use the public site URL, never the proxy's internal address (T-3)", () => {
  const site = "https://crew-clock-web-staging.onrender.com";
  // What Render hands the app as its own origin:
  const internal = "https://localhost:10000";
  assert.equal(publicUrl("/login?error=link&next=%2F", site, internal), `${site}/login?error=link&next=%2F`);
  assert.equal(publicUrl("/clients", site), `${site}/clients`);
  assert.equal(publicUrl("/", site + "/"), `${site}/`);            // trailing slash on the setting
  assert.equal(publicUrl("/x", site + "/some/path"), `${site}/x`); // only the origin of the setting is used
});

test("redirect paths can never leave the site", () => {
  const site = "https://crew-clock-web.onrender.com";
  for (const bad of ["//evil.com", "https://evil.com", "/\\evil.com", "/\t/evil.com", "", null]) {
    assert.equal(publicUrl(bad as string, site), `${site}/`, String(bad));
  }
});

test("development may fall back to the request origin; production must be configured", () => {
  assert.equal(publicUrl("/", undefined, "http://localhost:3000"), "http://localhost:3000/");
  assert.equal(publicUrl("/", "", "http://localhost:3000"), "http://localhost:3000/");
  assert.throws(() => publicUrl("/", undefined, undefined), /NEXT_PUBLIC_SITE_URL/);
  assert.throws(() => publicUrl("/", "not a url", undefined), /NEXT_PUBLIC_SITE_URL/);
  assert.throws(() => publicUrl("/", "/relative", undefined), /NEXT_PUBLIC_SITE_URL/);
});

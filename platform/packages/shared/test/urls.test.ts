import { test } from "node:test";
import assert from "node:assert/strict";
import { safeNext } from "../src/urls.ts";

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

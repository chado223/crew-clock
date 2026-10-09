import { test } from "node:test";
import assert from "node:assert/strict";
import { assertProjectForEnv } from "../src/projects.ts";

const prod = "https://kymnehbnmqvpzwtxtizm.supabase.co";
const staging = "https://newivmnolbzhjypusmob.supabase.co";
const old = "https://iwowjrnrbjiydckhjsfi.supabase.co";

test("each environment only accepts its own project", () => {
  assert.equal(assertProjectForEnv(prod, "production"), "kymnehbnmqvpzwtxtizm");
  assert.equal(assertProjectForEnv(staging, "staging"), "newivmnolbzhjypusmob");
  assert.throws(() => assertProjectForEnv(staging, "production"));
  assert.throws(() => assertProjectForEnv(prod, "staging"));
  assert.throws(() => assertProjectForEnv(prod, undefined), /Development builds may not use the production project/);
  assert.throws(() => assertProjectForEnv(old, "production"), /retired/);
  assert.throws(() => assertProjectForEnv(old, "development"), /retired/);
  assert.throws(() => assertProjectForEnv(undefined, "production"));
  assert.throws(() => assertProjectForEnv("https://evil.example.com", "production"));
});

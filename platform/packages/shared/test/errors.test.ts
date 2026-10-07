import { test } from "node:test";
import assert from "node:assert/strict";
import { friendlyError } from "../src/errors.ts";

test("database keys become plain messages; app sentences pass through; unknown stays generic", () => {
  assert.equal(friendlyError({ message: "forbidden" }), "You don't have permission to do that in this company.");
  assert.equal(friendlyError({ message: "punch_too_old: ask a manager to add this time" }), "That punch is more than 3 days old. Ask a manager to add it.");
  assert.equal(friendlyError("Enter a description, quantity and price."), "Enter a description, quantity and price.");
  assert.match(friendlyError({ message: "relation x does not exist" }), /Something went wrong/);
  assert.match(friendlyError("weird_key"), /Something went wrong/);
});

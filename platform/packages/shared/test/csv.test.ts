import { test } from "node:test";
import assert from "node:assert/strict";
import { mapImport, parseCsv, toCsv } from "../src/csv.ts";

test("parses quotes, commas, newlines, BOM and CRLF", () => {
  const rows = parseCsv('﻿Name,Address,Notes\r\n"Smith, Jo","12 Oak St","Gate code ""42""\nside door"\r\n\r\nLee,5 Elm,\r\n');
  assert.deepEqual(rows, [["Name", "Address", "Notes"], ["Smith, Jo", "12 Oak St", 'Gate code "42"\nside door'], ["Lee", "5 Elm", ""]]);
});

test("round trip, and spreadsheet formulas are neutralized", () => {
  const out = toCsv([["a", 'b "c"', "x,y"], ["=SUM(A1)", 3, null]]);
  assert.equal(out, 'a,"b ""c""","x,y"\r\n\'=SUM(A1),3,\r\n');
  assert.deepEqual(parseCsv(out)[0], ["a", 'b "c"', "x,y"]);
});

test("maps the headers people actually use", () => {
  const m = mapImport(parseCsv("Customer Name,E-mail,Street Address,ZIP,Mow Day,How often,Favorite color\nAnn,ann@x.com,1 A St,37865,Tue,weekly,blue\n"));
  assert.deepEqual(m.rows[0], { name: "Ann", email: "ann@x.com", address_line1: "1 A St", postal_code: "37865", day: "Tue", frequency: "weekly" });
  assert.deepEqual(m.ignored, ["Favorite color"]);
  assert.equal(m.mapped.address_line1, "Street Address");
});

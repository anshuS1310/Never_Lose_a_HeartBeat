import test from "node:test";
import assert from "node:assert/strict";
import {computeWindow} from "../src/processing.js";

test("five-second ECG context gives 60 bpm and correct statistics", () => {
  const context = Array.from({length: 2801}, () => [0, 0, 0]);
  for (let index = 0; index < context.length; index += 400)
    context[index] = [0, 1000, 0];
  const second = Array.from({length: 400}, () => [3, 4]);
  const result = computeWindow(6, context.slice(2400, 2800), second,
    context.map(row => row[1]), 0, null);
  assert.equal(result.heart_rate, 60);
  assert.equal(result.magnitude, 5);
  assert.deepEqual(result.mean_2, [3, 4]);
  assert.equal(result.complete, true);
});

test("a missing ECG sample suppresses heart rate without inventing data", () => {
  const context = Array.from({length: 2801}, () => [0, 0, 0]);
  for (let index = 0; index < context.length; index += 400)
    context[index] = [0, 1000, 0];
  context[2600] = null;
  const second = Array.from({length: 400}, () => [3, 4]);
  const result = computeWindow(6, context.slice(2400, 2800), second,
    context, 0, null);
  assert.equal(result.heart_rate, null);
  assert.equal(result.missing_1, 1);
  assert.equal(result.complete, false);
});

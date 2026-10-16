export const RATE = 400;
const rounded = value => value == null ? null : Math.round(value * 10) / 10;

function stats(rows, channels) {
  const count = rows.reduce((n, row) => n + (row === null ? 0 : 1), 0);
  if (!count) return [Array(channels).fill(null), Array(channels).fill(null), Array(channels).fill(null)];
  const sums = Array(channels).fill(0);
  const lows = Array(channels).fill(Infinity);
  const highs = Array(channels).fill(-Infinity);
  for (const row of rows) {
    if (row === null) continue;
    for (let c = 0; c < channels; c++) {
      sums[c] += row[c];
      lows[c] = Math.min(lows[c], row[c]);
      highs[c] = Math.max(highs[c], row[c]);
    }
  }
  return [sums.map(sum => rounded(sum / count)), lows, highs];
}

export function scanPeaks(context, contextStart, first, last, previousPeak = null) {
  const peaks = [];
  const queue = [];
  let head = 0;
  let missing = 0;
  const at = index => {
    const offset = index - contextStart;
    return offset >= 0 && offset < context.length ? context[offset] : null;
  };
  const ecg = row => Array.isArray(row) ? row[1] : row;
  for (let i = first - 800; i < first; i++) {
    const row = at(i);
    if (row === null) missing++;
    else {
      while (queue.length > head && queue[queue.length - 1][1] <= ecg(row)) queue.pop();
      queue.push([i, ecg(row)]);
    }
  }
  for (let i = first; i <= last; i++) {
    const row = at(i), before = at(i - 1), after = at(i + 1);
    if (missing === 0 && row !== null && before !== null && after !== null &&
        queue.length > head && ecg(row) > ecg(before) && ecg(row) > ecg(after) &&
        ecg(row) > queue[head][1] / 2 && (previousPeak === null || i - previousPeak >= 100)) {
      peaks.push(i);
      previousPeak = i;
    }
    const outgoing = at(i - 800);
    if (outgoing === null) missing--;
    else if (queue.length > head && queue[head][0] === i - 800) head++;
    if (row === null) missing++;
    else {
      while (queue.length > head && queue[queue.length - 1][1] <= ecg(row)) queue.pop();
      queue.push([i, ecg(row)]);
    }
    if (head > 1024) { queue.splice(0, head); head = 0; }
  }
  return peaks;
}

export function computeWindow(window, sensor1, sensor2, context1 = null,
                              contextStart = null, previousPeak = null, mode = "online") {
  if (sensor1.length !== RATE || sensor2.length !== RATE) throw new Error("bad window length");
  const [mean1, min1, max1] = stats(sensor1, 3);
  const [mean2, min2, max2] = stats(sensor2, 2);
  let magnitudeSum = 0, magnitudeCount = 0;
  for (const row of sensor2) if (row !== null) {
    magnitudeSum += Math.hypot(row[0], row[1]);
    magnitudeCount++;
  }
  let heartRate = null;
  if (window >= 4) {
    if (context1 === null || contextStart === null) throw new Error("missing ECG context");
    const first = RATE * (window - 4), last = RATE * (window + 1) - 1;
    const five = context1.slice(first - contextStart, last - contextStart + 1);
    if (five.length === 5 * RATE && five.every(row => row !== null)) {
      const peaks = scanPeaks(context1, contextStart, first, last, previousPeak);
      if (peaks.length >= 2)
        heartRate = rounded(60 * RATE * (peaks.length - 1) / (peaks[peaks.length - 1] - peaks[0]));
    }
  }
  const missing1 = RATE - sensor1.reduce((n, row) => n + (row === null ? 0 : 1), 0);
  const missing2 = RATE - sensor2.reduce((n, row) => n + (row === null ? 0 : 1), 0);
  return {window, mode, missing_1: missing1, missing_2: missing2,
    complete: missing1 === 0 && missing2 === 0,
    mean_1: mean1, min_1: min1, max_1: max1,
    mean_2: mean2, min_2: min2, max_2: max2,
    magnitude: magnitudeCount ? rounded(magnitudeSum / magnitudeCount) : null,
    heart_rate: heartRate};
}

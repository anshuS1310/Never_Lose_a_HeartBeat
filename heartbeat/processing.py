"""The reference window algorithm used by the device and mirrored by the Worker.

All inputs are indexed arrays. A missing sample is None; values are never imputed.
"""
from collections import deque
from math import floor, hypot

RATE = 400


def rounded(value):
    # JavaScript Math.round on nonnegative statistics has the same tie behavior.
    return None if value is None else floor(value * 10 + 0.5) / 10


def channel_stats(rows, channels):
    present = [row for row in rows if row is not None]
    if not present:
        return [None] * channels, [None] * channels, [None] * channels
    means = [rounded(sum(row[c] for row in present) / len(present)) for c in range(channels)]
    lows = [min(row[c] for row in present) for c in range(channels)]
    highs = [max(row[c] for row in present) for c in range(channels)]
    return means, lows, highs


def scan_peaks(context, context_start, first, last, previous_peak=None):
    """Find accepted peaks in [first,last], given 800 predecessors and one successor.

    context[k] belongs to sample context_start+k. This routine runs in O(n) time.
    """
    peaks = []
    q = deque()  # decreasing channel-2 values in the prior 800 samples
    ecg = lambda row: row if isinstance(row, int) else row[1]
    missing = 0
    begin = first - 800
    for index in range(begin, first):
        row = context[index - context_start] if index >= context_start else None
        if row is None:
            missing += 1
        else:
            value = ecg(row)
            while q and q[-1][1] <= value:
                q.pop()
            q.append((index, value))
    for index in range(first, last + 1):
        offset = index - context_start
        current = context[offset] if 0 <= offset < len(context) else None
        successor = context[offset + 1] if 0 <= offset + 1 < len(context) else None
        predecessor = context[offset - 1] if 0 <= offset - 1 < len(context) else None
        if (missing == 0 and current is not None and successor is not None
                and predecessor is not None and q and ecg(current) > ecg(predecessor)
                and ecg(current) > ecg(successor) and ecg(current) > q[0][1] / 2
                and (previous_peak is None or index - previous_peak >= 100)):
            peaks.append(index)
            previous_peak = index
        outgoing = context[index - 800 - context_start] if index - 800 >= context_start else None
        if outgoing is None:
            missing -= 1
        elif q and q[0][0] == index - 800:
            q.popleft()
        if current is None:
            missing += 1
        else:
            value = ecg(current)
            while q and q[-1][1] <= value:
                q.pop()
            q.append((index, value))
    return peaks


def compute_window(window, sensor1, sensor2, context1=None, context_start=None,
                   previous_peak=None, mode="offline"):
    if len(sensor1) != RATE or len(sensor2) != RATE:
        raise ValueError("window arrays must contain exactly 400 positions")
    mean1, min1, max1 = channel_stats(sensor1, 3)
    mean2, min2, max2 = channel_stats(sensor2, 2)
    magnitudes = [hypot(*row) for row in sensor2 if row is not None]
    magnitude = rounded(sum(magnitudes) / len(magnitudes)) if magnitudes else None
    heart_rate = None
    if window >= 4:
        first = RATE * (window - 4)
        last = RATE * (window + 1) - 1
        if context1 is None or context_start is None:
            raise ValueError("heart-rate context is required after window 3")
        five_seconds = context1[first - context_start:last - context_start + 1]
        if len(five_seconds) == 5 * RATE and all(row is not None for row in five_seconds):
            peaks = scan_peaks(context1, context_start, first, last, previous_peak)
            if len(peaks) >= 2:
                heart_rate = rounded(60 * RATE * (len(peaks) - 1) / (peaks[-1] - peaks[0]))
    return {
        "window": window, "mode": mode,
        "missing_1": RATE - sum(row is not None for row in sensor1),
        "missing_2": RATE - sum(row is not None for row in sensor2),
        "complete": all(row is not None for row in sensor1) and all(row is not None for row in sensor2),
        "mean_1": mean1, "min_1": min1, "max_1": max1,
        "mean_2": mean2, "min_2": min2, "max_2": max2,
        "magnitude": magnitude, "heart_rate": heart_rate,
    }

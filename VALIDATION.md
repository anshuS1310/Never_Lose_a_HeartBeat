# Validation and measured limits

The checks below used this repository's synthetic simulator. The device environment was a Debian 13 ARM64 QEMU VM with four vCPUs, 2 GiB allocated memory, and an ext4 guest disk. Cloud checks used a Cloudflare Worker with D1. These are development measurements, not clinical validation or a claim about physical Raspberry Pi performance.

| Scenario | Observed result |
| --- | --- |
| One-hour ARM64 signal run | 1,440,000 samples per sensor, 3,600 frames uploaded, and zero recorded sample losses. Four startup windows were offline; the next 3,596 were online. [Run report](evidence/hour-arm64-report.json). |
| One-hour resource monitor | Combined mock and pipeline averaged 27.17% of the four-core device, with 44.7 MB peak RSS, 5,644.12 MB disk writes, and 95.68 MB guest network sent including SSH traffic. [Resource report](evidence/hour-arm64-resources.json). |
| Cloud outage and recovery | Offline switch in 0.24 s, online switch in 1.71 s, 7,200 samples per sensor, eight offline results, and all 18 frames uploaded and read back. [Report](evidence/network-arm64.json). |
| Abrupt QEMU stop and restart | SQLite integrity check returned `ok`; 11,240 and 11,257 saved ramp samples matched their expected values. The previous session's unknown tail was recorded as an open-ended `mock_restarted` loss. [Report](evidence/power-cut-arm64.json). |
| Two-hour-equivalent backlog | 7,200 precommitted offline frames uploaded in 173.78 s; their local results were marked synced and 7,200 distinct cloud results were read back. [Run report](evidence/backlog-arm64.json). |
| Backlog cloud-copy verification | All 2,880,000 cloud samples per sensor matched the ramp formula in order, with zero loss records. [Checker output](evidence/backlog-arm64-cloud-check.json). |
| Clock jumps | After guest clock jumps of +2 hours and -2 hours, 12,000 samples per sensor and 30 windows remained complete with zero recorded loss. [Report](evidence/clock-arm64.json). |
| Full disk on an isolated ext4 volume | SQLite integrity remained `ok`; exact saved ramp samples and explicit finite `buffer_overflow` records accounted for all 18,000 expected indexes per sensor. [Report](evidence/disk-full-arm64.json). |

The one-hour resource run preceded the offline frame-cache optimization; the cache was used in the subsequent backlog and disk-full checks. The backlog result starts **after** two hours' worth of windows were precommitted; it does not represent a continuous two-hour outage. During the hour run, guest clock drift relative to the cloud made cross-clock latency unsuitable for a strict five-second claim. A full disk can still exceed the reserve and the simulator's ten-second buffer, causing recorded loss. Sudden power loss preserves committed samples only if the underlying storage honors flushes; unread simulator data can disappear when the simulator itself loses power.

The project has not been validated on physical sensor hardware. Reproduce these tests in the intended deployment environment before relying on their timing or durability figures.

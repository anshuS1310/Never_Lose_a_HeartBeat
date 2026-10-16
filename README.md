# Heartbeat Edge Pipeline

An offline-capable biosignal pipeline for a small ARM64 Linux device. It reads two synthetic sensors over TCP, validates each response, saves samples in SQLite, calculates one-second results locally when the cloud is unavailable, and uploads frames to a Cloudflare Worker with D1 storage when connectivity returns.

The repository includes a synthetic sensor simulator so the system can be run and tested without hardware. It is a software prototype, not a medical device.

## What it does

- Collects two 400 Hz sensor streams with three and two channels respectively.
- Checks the literal JSON data CRC before committing a response.
- Stores samples, cursors, and reported loss ranges in a SQLite database using WAL and `synchronous=FULL`.
- Computes window statistics, magnitude, and an ECG-style heart-rate estimate locally or in the Worker.
- Retries idempotent cloud uploads and catches up from a local backlog after an outage.
- Provides `status`, `results`, and device/cloud `export` commands.

See [DESIGN.md](DESIGN.md) for the data model and failure behavior, and [VALIDATION.md](VALIDATION.md) for measured tests and limitations.

## Quick start: local offline run

Use Linux or WSL with Python 3.10 or newer. The device application uses the Python standard library; no Python package installation is needed. Start the pipeline **before** the simulator so it can capture the complete session.

Terminal 1:

```bash
cd heartbeat-edge-pipeline
mkdir -p data
export PIPELINE_DB="$PWD/data/heartbeat.db"
python3 -u pipeline.py run --mock-port 7100
```

Terminal 2:

```bash
cd heartbeat-edge-pipeline
python3 -u tools/dev_mock.py --mode ramp --duration 30 --port 7100
```

The simulator prints an eight-character session ID. It keeps listening after generating 30 seconds of samples, so leave it running until the checks finish. In Terminal 3, replace `SESSION_ID` with that ID:

```bash
cd heartbeat-edge-pipeline
export PIPELINE_DB="$PWD/data/heartbeat.db"
python3 pipeline.py status
python3 pipeline.py results --session SESSION_ID
set -o pipefail
python3 pipeline.py export --session SESSION_ID --from device | python3 tools/verify_ramp_export.py --expected 12000
```

For a fresh run, `samples_saved` reaches 24,000 and the ramp verifier reports 12,000 samples per sensor, no recorded loss, and `status: ok`. Without cloud configuration, `mode` is `offline`, `samples_in_cloud` is zero, and `backlog` equals the saved sample count. Stop the simulator and pipeline with Ctrl+C. Use a new database path for another isolated run.

## Cloudflare Worker and D1

Cloud deployment is optional. You need a Cloudflare account and Node.js compatible with the pinned Wrangler dependency. From `cloud/`:

```bash
npm ci
npx wrangler login
npx wrangler d1 create heartbeat-edge-data
bash setup.sh D1_DATABASE_ID
```

Replace `D1_DATABASE_ID` with the ID printed by the create command. `setup.sh` writes a local `wrangler.toml`, applies `schema.sql` to the remote database, deploys the Worker, and prompts for its `API_TOKEN` secret. It refuses to replace an existing `wrangler.toml`. Use a long random token and keep it private.

On the device, copy `deploy/pipeline.env.example` to the ignored `device.env`, enter the deployed HTTPS Worker URL and the same token, then load it before running:

```bash
cp deploy/pipeline.env.example device.env
# Edit PIPELINE_CLOUD_URL and PIPELINE_TOKEN in device.env.
set -a
. ./device.env
set +a
export PIPELINE_DB="$PWD/data/heartbeat.db"
python3 -u pipeline.py run --mock-port 7100
```

After a new simulator session finishes, `python3 pipeline.py status` should show `samples_in_cloud` catching up to `samples_saved` and `backlog` reaching zero. To check the cloud copy, run `python3 pipeline.py export --session SESSION_ID --from cloud | python3 tools/verify_ramp_export.py --expected 12000` for a complete 30-second ramp session. Cloud endpoints require the token; opening the Worker URL in a browser without authorization returns an authentication error.

Cloudflare free-tier limits can interrupt uploads. The device retains its committed data and retries when service is available, subject to local disk capacity. Do not commit `device.env`, `cloud/.dev.vars`, `cloud/.cloud-secret`, or `cloud/wrangler.toml`.

## ARM64 VM

The `vm/` scripts create a Debian ARM64 VM with four vCPUs and 2 GiB of RAM. On an Ubuntu or WSL host, install QEMU AArch64, AAVMF, `cloud-image-utils`, `curl`, and OpenSSH. Keep the VM disk on a Linux filesystem rather than a Windows-mounted path:

```bash
export VM_STORAGE="$HOME/heartbeat-vm"
bash vm/prepare.sh
bash vm/run.sh
```

In another shell:

```bash
ssh -i "$VM_STORAGE/id_ed25519" -p 2222 heartbeatdev@127.0.0.1
```

The VM account uses key-based SSH and has no console password. Copy the checkout into the VM before running the device application there. `deploy/` contains optional systemd examples for a persistent installation; adapt the sensor source and paths to your device. The simulator and pipeline must run on persistent storage for power-cut tests; Debian's `/tmp` is memory-backed in this VM.

## Tests

```bash
python3 -m unittest discover -s tests -v
python3 tests/fault_smoke.py
python3 tests/integration_smoke.py
cd cloud && npm test
```

With a deployed Worker and `device.env` loaded, `TMPDIR=/var/tmp python3 tests/network_smoke.py` simulates a cloud outage and verifies offline processing, reconnection, and catch-up. Other scripts under `tests/` exercise disk-full, clock-change, power-cut, latency, backlog, and resource scenarios. Some require an ARM64 VM, `sudo`, or a live Worker; read their headers before running them.

## Limits

The simulator is synthetic and the heart-rate output is an estimate from its generated ECG-like signal. This project has been tested on an emulated ARM64 VM and Cloudflare, not validated on a physical Raspberry Pi or for clinical use. See [VALIDATION.md](VALIDATION.md) for exact measurements and caveats. Development assistance is recorded in [AI_DISCLOSURE.md](AI_DISCLOSURE.md).

"""
Robot Monitoring agent.

Start it from the folder that holds this file:

    python3 Agent.py          (Windows: py Agent.py)

It listens on http://localhost:8765. dashboard.html (served separately, e.g.
with  python3 -m http.server 8000) reads the Arduino over USB in Chrome and
sends readings here to be checked. That needs nothing but Python: no packages,
no API key.

Optional extras:
    Claude explanations   pip install anthropic, then set ANTHROPIC_API_KEY.
                          Without a key, built-in rules check the readings.
    Solana records        pip install solana solders, then add --chain.
                          Warnings are written to Solana devnet (public, permanent).
                          The first run creates agent-wallet.json; keep it private.

Other options:
    --listen PORT       use a different port (default 8765)
    --port COM3         read the Arduino directly instead of through the browser
                        (needs: pip install pyserial)
    --check-every N     check every N readings (default 10, or 30 with --port)
    --rules             use the built-in rules even if an API key is set
    --rpc URL           Solana RPC endpoint for --chain (default devnet)    
"""

import argparse
import errno
import hashlib
import json
import os
import sys
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from statistics import median
from urllib.parse import urlparse

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-5-5")
WINDOW = 30                 # readings kept in memory and sent to Claude
CHECK_EVERY = 30            # serial mode: ask Claude every N readings
CHECK_EVERY_LISTEN = 10     # listen mode default
STALE_SECONDS = 10          # serial mode: no data for this long is an alert
CHAIN_REPEAT_SECONDS = 60   # re-log an unchanged non-OK status at most this often
MAX_BODY = 64 * 1024
DEFAULT_RPC = "https://api.devnet.solana.com"
WALLET_FILE = "agent-wallet.json"
MEMO_PROGRAM_ID = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"

SYSTEM = (
    "You monitor an Arduino project. You receive its most recent readings as a JSON "
    "array, oldest first. Each reading has a timestamp t (Unix seconds, added on "
    "arrival) plus whatever fields the Arduino sent. Work out what each field most "
    "likely is from its name and values: for example button_pressed is a push button "
    "(true while held), and integers from 0 to 1023 are raw 10-bit analog readings "
    "(a potentiometer, light sensor, etc.) with no physical unit.\n"
    "Judge whether the behaviour looks normal. Flag a button stuck pressed for a "
    "long time, rapid chatter or bouncing, sensors that flatline, spike, drift, sit "
    "at 0 or 1023, fields that disappear, and gaps in the timestamps. Normal button "
    "presses and normal sensor variation are ok. Separate likely wiring or code "
    "problems from real-world events, and say which you think it is.\n"
    "Use only the data provided. Report with the submit_assessment tool."
)

ASSESS_TOOL = {
    "name": "submit_assessment",
    "description": "Report the assessment of the recent Arduino readings.",
    "input_schema": {
        "type": "object",
        "properties": {
            "status": {
                "type": "string",
                "enum": ["ok", "warning", "critical"],
                "description": "ok = normal; warning = needs attention; critical = act now.",
            },
            "reason": {
                "type": "string",
                "description": "One plain-language sentence, under 120 characters.",
            },
            "details": {
                "type": "string",
                "description": "2-3 sentences: what the readings show and what to check or do.",
            },
            "sensors": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Each field and what it most likely is, e.g. 'a: analog input, likely a potentiometer'.",
            },
        },
        "required": ["status", "reason", "details", "sensors"],
    },
}


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(msg):
    print(f"{time.strftime('%H:%M:%S')}  {msg}", flush=True)


# ---------- Solana ----------

class Chain:
    """Writes short memos to Solana. Every failure is reported, never fatal."""

    def __init__(self, rpc_url):
        from solana.rpc.api import Client
        from solders.instruction import AccountMeta, Instruction
        from solders.keypair import Keypair
        from solders.message import Message
        from solders.pubkey import Pubkey
        from solders.transaction import Transaction

        self._AccountMeta, self._Instruction = AccountMeta, Instruction
        self._Message, self._Transaction = Message, Transaction
        self._memo_program = Pubkey.from_string(MEMO_PROGRAM_ID)
        self.rpc_url = rpc_url
        self.client = Client(rpc_url)
        self.cluster = "devnet" if "devnet" in rpc_url else "testnet" if "testnet" in rpc_url else "mainnet-beta"
        self.wallet = self._load_wallet(Keypair)

    def _load_wallet(self, Keypair):
        if os.path.exists(WALLET_FILE):
            with open(WALLET_FILE) as f:
                return Keypair.from_bytes(bytes(json.load(f)))
        kp = Keypair()
        with open(WALLET_FILE, "w") as f:
            json.dump(list(bytes(kp)), f)
        try:
            os.chmod(WALLET_FILE, 0o600)
        except OSError:
            pass
        log(f"Created wallet {kp.pubkey()} in {WALLET_FILE} (keep this file private)")
        return kp

    @property
    def address(self):
        return str(self.wallet.pubkey())

    def ensure_funded(self):
        try:
            bal = self.client.get_balance(self.wallet.pubkey()).value
        except Exception as e:
            log(f"Couldn't check the wallet balance ({e}). Solana logging may fail.")
            return
        if bal >= 10_000:
            log(f"Wallet {self.address} has {bal / 1e9:.4f} SOL on {self.cluster}.")
            return
        if self.cluster != "devnet":
            log(f"Wallet {self.address} is empty. Send it some SOL to enable logging.")
            return
        log("Wallet is empty. Requesting 1 devnet SOL…")
        try:
            self.client.request_airdrop(self.wallet.pubkey(), 1_000_000_000)
            for _ in range(15):
                time.sleep(2)
                if self.client.get_balance(self.wallet.pubkey()).value > 0:
                    log("Airdrop received.")
                    return
            log("Airdrop not confirmed yet. It may still arrive.")
        except Exception as e:
            log(f"Airdrop failed ({e}). The devnet faucet is often rate-limited.")
            log(f"Get devnet SOL at https://faucet.solana.com for {self.address}, "
                "then restart. The agent keeps running meanwhile.")

    def log_memo(self, result, readings):
        """Write status, reason and a hash of the evidence. Returns the signature."""
        digest = hashlib.sha256(json.dumps(list(readings), sort_keys=True).encode()).hexdigest()[:16]
        memo = json.dumps(
            {"s": result["status"], "r": result["reason"][:120], "h": digest, "t": int(time.time())},
            separators=(",", ":"),
        ).encode()
        ix = self._Instruction(
            self._memo_program, memo,
            [self._AccountMeta(self.wallet.pubkey(), is_signer=True, is_writable=False)],
        )
        blockhash = self.client.get_latest_blockhash().value.blockhash
        msg = self._Message.new_with_blockhash([ix], self.wallet.pubkey(), blockhash)
        tx = self._Transaction([self.wallet], msg, blockhash)
        return str(self.client.send_transaction(tx).value)


# ---------- Agent ----------

def clean_result(raw):
    """Never let a malformed reply reach the dashboard or the chain."""
    raw = raw if isinstance(raw, dict) else {}
    status = raw.get("status") if raw.get("status") in ("ok", "warning", "critical") else "warning"
    reason = str(raw.get("reason") or "The agent returned an incomplete assessment.").strip()[:200]
    details = str(raw.get("details") or "").strip()[:800]
    sensors = [str(s)[:120] for s in raw.get("sensors", []) if s][:12] if isinstance(raw.get("sensors"), list) else []
    return {"status": status, "reason": reason, "details": details, "sensors": sensors}


def is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def rules_assess(readings):
    """Checks the readings with fixed rules. Same result shape as Claude's."""
    readings = [r for r in readings if isinstance(r, dict) and is_number(r.get("t"))]
    if len(readings) < 3:
        return {"status": "ok", "reason": "Not enough readings yet to judge.",
                "details": "", "sensors": []}

    issues = []   # (rank, status, reason, advice)
    sensors = []
    t_last = readings[-1]["t"]

    def flag(status, reason, advice):
        issues.append((2 if status == "critical" else 1, status, reason, advice))

    # Gaps in the data.
    ts = [r["t"] for r in readings]
    diffs = [b - a for a, b in zip(ts, ts[1:]) if b > a]
    if diffs:
        typical = median(diffs)
        gap = max(diffs)
        if gap > max(5.0, typical * 5):
            flag("warning", f"Readings stopped for {gap:.0f}s at one point.",
                 "Check the USB cable and that the sketch isn't freezing or resetting.")

    fields = []
    for r in readings:
        for k in r:
            if k != "t" and k not in fields:
                fields.append(k)

    for f in fields:
        pts = [(r["t"], r[f]) for r in readings if f in r]
        vals = [v for _, v in pts]

        # A field that was sent before but not in the latest readings.
        if len(readings) >= 8 and all(f not in r for r in readings[-5:]):
            flag("warning", f"'{f}' stopped appearing in the readings.",
                 "Check the sketch still prints it and the sensor is still connected.")

        if all(isinstance(v, bool) for v in vals):
            sensors.append(f"{f}: on/off input, likely a push button" if "button" in f.lower()
                           else f"{f}: on/off input")
            # How long it has been continuously on.
            start = None
            for t, v in reversed(pts):
                if not v:
                    break
                start = t
            held = (t_last - start) if (start is not None and pts[-1][1]) else 0
            if held >= 30:
                flag("critical", f"{f} has been on for {held:.0f}s without release.",
                     "Check whether the button is stuck or something is pressing it, or whether the input is wired so it reads pressed all the time.")
            elif held >= 8:
                flag("warning", f"{f} has been held on for {held:.0f}s.",
                     "If nobody is holding it, check for a stuck button or a short to the input pin.")
            # Rapid switching (chatter / bouncing) in the last 10 seconds.
            recent = [v for t, v in pts if t >= t_last - 10]
            changes = sum(1 for a, b in zip(recent, recent[1:]) if a != b)
            if changes >= 6:
                flag("warning", f"{f} switched on/off {changes} times in 10s.",
                     "That looks like contact bounce or a loose wire. Use INPUT_PULLUP and add a short debounce in the sketch.")
            continue

        nums = [(t, float(v)) for t, v in pts if is_number(v)]
        if not nums:
            sensors.append(f"{f}: text or other non-numeric value")
            continue
        xs = [v for _, v in nums]
        analog = all(v.is_integer() and 0 <= v <= 1023 for v in xs)
        sensors.append(f"{f}: raw analog input (0-1023), e.g. a potentiometer or light sensor"
                       if analog else f"{f}: numeric reading")
        latest = xs[-1]

        # Pinned at 0 or full scale.
        tail = xs[-5:]
        railed = analog and len(tail) == 5 and len(set(tail)) == 1 and tail[0] in (0, 1023)
        if railed:
            flag("warning", f"{f} is stuck at {int(tail[0])}, the end of its range.",
                 "A reading pinned at 0 or 1023 usually means a loose or shorted wire, or nothing connected to that pin.")

        # Flatlined: a long run of identical values after the field was varying.
        if not railed and len(xs) >= 25 and len(set(xs[-20:])) == 1 and len(set(xs[:-20])) > 2:
            flag("warning", f"{f} hasn't changed in the last 20 readings (stuck at {latest:g}).",
                 "If it's a knob nobody is touching, that's fine. Otherwise check the sensor's connection; a frozen value often means it stopped responding.")

        # Sudden spike in the newest value.
        prev = xs[:-1]
        if len(prev) >= 8 and not railed:
            med = median(prev)
            mad = median(abs(v - med) for v in prev)
            floor = 20.0 if analog else 1e-9
            scale = max(1.4826 * mad, floor)
            recent_med = median(prev[-5:])  # a new steady level (e.g. a knob turned) isn't a spike
            if (abs(latest - med) > 8 * scale and abs(latest - med) > (50 if analog else 0)
                    and abs(latest - recent_med) > 8 * scale):
                flag("warning", f"{f} jumped to {latest:g} (usually about {med:g}).",
                     "A one-off jump is often electrical noise or a loose wire; check it if it keeps happening.")

        # Slow, steady drift across the window.
        if len(nums) >= 10 and nums[-1][0] - nums[0][0] >= 20:
            t0 = nums[0][0]
            tx = [t - t0 for t, _ in nums]
            mt, mx = sum(tx) / len(tx), sum(xs) / len(xs)
            sxx = sum((t - mt) ** 2 for t in tx)
            if sxx > 0:
                slope = sum((t - mt) * (v - mx) for t, v in zip(tx, xs)) / sxx
                fit = [mx + slope * (t - mt) for t in tx]
                ss_res = sum((v - p) ** 2 for v, p in zip(xs, fit))
                ss_tot = sum((v - mx) ** 2 for v in xs) or 1e-9
                change = slope * (tx[-1] - tx[0])
                # Real drift is still going; a knob turned and then left alone has stopped.
                recent = xs[-1] - xs[-10]
                still_moving = recent * change > 0 and abs(recent) > max(30 if analog else 0, 0.15 * abs(change))
                if abs(change) > (150 if analog else 0) and 1 - ss_res / ss_tot > 0.8 and still_moving:
                    flag("warning", f"{f} is drifting steadily {'up' if change > 0 else 'down'} ({change:+.0f} over {tx[-1]:.0f}s).",
                         "If you aren't adjusting it, check for a warming component, a failing sensor, or a loose connection.")

    if not issues:
        return {"status": "ok",
                "reason": (f"All {len(fields)} fields look" if len(fields) != 1 else f"'{fields[0]}' looks")
                          + f" normal over the last {len(readings)} readings.",
                "details": "No stuck button, bouncing, flatlined, maxed-out, spiking or drifting values, and no gaps in the data.",
                "sensors": sensors}

    issues.sort(key=lambda i: -i[0])
    _, status, reason, advice = issues[0]
    others = " ".join(i[2] for i in issues[1:])
    details = advice + (f" Also: {others}" if others else "")
    return {"status": status, "reason": reason[:200], "details": details[:800], "sensors": sensors}


def assess(claude, readings):
    msg = claude.messages.create(
        model=MODEL,
        max_tokens=800,
        system=SYSTEM,
        tools=[ASSESS_TOOL],
        tool_choice={"type": "tool", "name": ASSESS_TOOL["name"]},
        messages=[{"role": "user", "content": json.dumps(list(readings))}],
    )
    block = next((b for b in msg.content if getattr(b, "type", "") == "tool_use"), None)
    return clean_result(block.input if block else None)


class Monitor:
    def __init__(self, check, engine, chain, check_every):
        self.check = check          # function(readings) -> result dict
        self.engine = engine        # "claude" or "rules"
        self.chain = chain
        self.check_every = check_every
        self.readings = deque(maxlen=WINDOW)
        self.count = 0
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.last_logged = (None, 0.0)  # (status, time) of the last on-chain memo
        self.result = {
            "status": "pending",
            "reason": f"No assessment yet. The first one runs after {check_every} readings.",
            "details": "",
            "sensors": [],
            "assessed_at": None,
            "readings_assessed": 0,
            "signature": None,
            "chain_error": None,
            "agent_error": None,
        }
        self.assessing = False
        threading.Thread(target=self._worker, daemon=True).start()

    # Called for every reading. Returns immediately; checks run in the background.
    def ingest(self, reading):
        with self.lock:
            reading = dict(reading)
            reading["t"] = round(time.time(), 1)
            self.readings.append(reading)
            self.count += 1
            due = self.count % self.check_every == 0
        if due:
            self.wake.set()

    def status(self):
        with self.lock:
            out = dict(self.result)
            out.update(
                events_received=self.count,
                assessing=self.assessing,
                check_every=self.check_every,
                engine=self.engine,
                model=MODEL if self.engine == "claude" else None,
                chain_enabled=self.chain is not None,
                cluster=self.chain.cluster if self.chain else None,
                wallet=self.chain.address if self.chain else None,
            )
            return out

    def _worker(self):
        while True:
            self.wake.wait()
            self.wake.clear()
            with self.lock:
                snapshot = list(self.readings)
                self.assessing = True
            try:
                result = self.check(snapshot)
            except Exception as e:
                what = "Claude request" if self.engine == "claude" else "Check"
                log(f"{what} failed: {e}")
                with self.lock:
                    self.assessing = False
                    self.result = {**self.result, "agent_error": f"Last {what.lower()} failed: {e}"[:300]}
                continue
            result.update(assessed_at=now_iso(), readings_assessed=len(snapshot),
                          signature=None, chain_error=None, agent_error=None)
            if result["status"] == "ok":
                log(f"OK: {result['reason']}")
            else:
                self.alert(result, snapshot)
            with self.lock:
                self.result = result
                self.assessing = False

    def alert(self, result, snapshot):
        """Prints the alert and records it on Solana (fills in signature / chain_error)."""
        log(f"{result['status'].upper()}: {result['reason']}")
        if not self.chain:
            return
        last_status, last_time = self.last_logged
        if result["status"] == last_status and time.time() - last_time < CHAIN_REPEAT_SECONDS:
            result["chain_error"] = f"Same status logged under {CHAIN_REPEAT_SECONDS}s ago; not logged again."
            return
        try:
            sig = self.chain.log_memo(result, snapshot)
            self.last_logged = (result["status"], time.time())
            result["signature"] = sig
            log(f"  logged on Solana ({self.chain.cluster}): {sig}")
        except Exception as e:
            result["chain_error"] = f"Solana logging failed: {e}"[:300]
            log(f"  Solana logging failed: {e}")


# ---------- HTTP (listen mode) ----------

def make_handler(monitor):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body=b"{}", content_type="application/json"):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            # Lets Chrome pages served from another origin reach this local server.
            self.send_header("Access-Control-Allow-Private-Network", "true")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json(self, code, obj):
            self._send(code, json.dumps(obj).encode())

        def do_OPTIONS(self):
            self._send(204, b"")

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/status":
                self._json(200, monitor.status())
            elif path in ("/", "/health"):
                self._json(200, {"agent": "running", "endpoints": ["GET /status", "POST /event"]})
            elif path == "/favicon.ico":
                self._send(204, b"", "image/x-icon")
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self):
            if urlparse(self.path).path != "/event":
                return self._json(404, {"error": "not found"})
            try:
                length = int(self.headers.get("Content-Length", 0))
                if length <= 0 or length > MAX_BODY:
                    raise ValueError("empty or too large")
                reading = json.loads(self.rfile.read(length))
                if not isinstance(reading, dict):
                    raise ValueError("not a JSON object")
            except (ValueError, json.JSONDecodeError) as e:
                return self._json(400, {"error": f"send one JSON object per request ({e})"})
            monitor.ingest(reading)
            self._json(200, {"ok": True, "events_received": monitor.count})

        def log_message(self, *args):
            pass

    return Handler


# ---------- Serial mode ----------

def run_serial(monitor, port, baud):
    try:
        import serial
    except ImportError:
        sys.exit("Missing package: run  pip install pyserial")
    last_seen = time.time()
    outage_reported = False
    with serial.Serial(port, baud, timeout=2) as ser:
        log(f"Monitoring {port} at {baud} baud.")
        while True:
            line = ser.readline().decode(errors="ignore").strip()
            if not line:
                if not outage_reported and time.time() - last_seen > STALE_SECONDS:
                    result = {"status": "critical", "reason": f"No data from Arduino for {STALE_SECONDS}s",
                              "details": "Check the USB cable, the sketch, and the baud rate.",
                              "sensors": [], "assessed_at": now_iso(),
                              "readings_assessed": len(monitor.readings),
                              "signature": None, "chain_error": None}
                    monitor.alert(result, list(monitor.readings))
                    with monitor.lock:
                        monitor.result = result
                    outage_reported = True  # one alert per outage, not one every 10s
                continue
            try:
                reading = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(reading, dict):
                continue
            if outage_reported:
                log("Data is arriving again.")
            last_seen = time.time()
            outage_reported = False
            monitor.ingest(reading)


def main():
    ap = argparse.ArgumentParser(description="Arduino monitoring agent")
    ap.add_argument("--port", help="serial port; the agent reads the Arduino directly (needs pyserial)")
    ap.add_argument("--baud", type=int, default=115200)
    ap.add_argument("--listen", type=int, metavar="PORT",
                    help="port that receives readings from dashboard.html (default 8765)")
    ap.add_argument("--check-every", type=int,
                    help=f"check every N readings (default {CHECK_EVERY_LISTEN}, or {CHECK_EVERY} with --port)")
    ap.add_argument("--rules", action="store_true",
                    help="use the built-in rules instead of Claude, even if an API key is set")
    ap.add_argument("--chain", action="store_true",
                    help="record warnings on Solana (needs: pip install solana solders)")
    ap.add_argument("--no-chain", action="store_true", help=argparse.SUPPRESS)  # old flag; off is now the default
    ap.add_argument("--rpc", default=os.environ.get("SOLANA_RPC_URL", DEFAULT_RPC),
                    help="Solana RPC URL for --chain (default devnet)")
    args = ap.parse_args()
    if args.port and args.listen:
        ap.error("choose either --port or --listen, not both")
    if not args.port and not args.listen:
        args.listen = 8765
    if args.check_every is not None and args.check_every < 1:
        ap.error("--check-every must be at least 1")

    # Pick who checks the readings: Claude if a key is set, otherwise the built-in rules.
    engine, check = "rules", rules_assess
    if args.rules:
        log("Using the built-in rules (--rules).")
    elif not os.environ.get("ANTHROPIC_API_KEY"):
        log("No ANTHROPIC_API_KEY set, so readings are checked with the built-in rules (no AI).")
    else:
        try:
            import anthropic
            claude = anthropic.Anthropic()
            engine, check = "claude", lambda readings: assess(claude, readings)
        except ImportError:
            log("ANTHROPIC_API_KEY is set but the anthropic package isn't installed "
                "(pip install anthropic). Using the built-in rules for now.")

    chain = None
    if args.chain:
        try:
            chain = Chain(args.rpc)
        except ImportError:
            sys.exit("Solana recording needs two packages: python3 -m pip install solana solders "
                     "(Windows: py -m pip install solana solders). Or run without --chain.")
        if chain.cluster == "mainnet-beta":
            log("WARNING: logging to Solana mainnet costs real SOL for every alert.")
        chain.ensure_funded()

    monitor = Monitor(check, engine, chain,
                      args.check_every or (CHECK_EVERY_LISTEN if args.listen else CHECK_EVERY))
    checker = MODEL if engine == "claude" else "the built-in rules"
    summary = (f"Checking every {monitor.check_every} readings with {checker}. "
               f"Solana recording: {'on, ' + chain.cluster if chain else 'off (add --chain to turn it on)'}.")

    if args.listen:
        try:
            server = ThreadingHTTPServer(("127.0.0.1", args.listen), make_handler(monitor))
        except OSError as e:
            if e.errno in (errno.EADDRINUSE, 10048):
                sys.exit(f"Port {args.listen} is already in use, probably by an agent running in another "
                         f"terminal window. Close that window (or press Ctrl+C in it) and try again.")
            raise
        log(f"Agent running at http://localhost:{args.listen}  (the page sends readings here)")
        log(summary)
        log("Leave this window open. Press Ctrl+C to stop.")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            log("Stopped.")
    else:
        log(summary)
        try:
            run_serial(monitor, args.port, args.baud)
        except KeyboardInterrupt:
            log("Stopped.")


if __name__ == "__main__":
    main()

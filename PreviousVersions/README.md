# NEPSE Auto Trader — Naasa Securities

Real-time trading client for Naasa Securities (NEPSE). Connects to the Naasa WebSocket feed, watches LTP ticks for configured symbols, and fires orders via the Naasa REST API when conditions are met.

---

## Dependencies

### Linux (Ubuntu/Debian)

```bash
sudo apt-get install -y \
    build-essential g++ pkg-config \
    libcurl4-openssl-dev \
    libwebsockets-dev \
    nlohmann-json3-dev \
    zlib1g-dev
```

### Windows (vcpkg)

```powershell
vcpkg install curl libwebsockets nlohmann-json zlib
```

---

## Build

### Linux

```bash
g++ -std=c++17 -O3 -march=native -flto -DNDEBUG \
    -fomit-frame-pointer -funroll-loops \
    -o trader main_updated.cpp \
    $(pkg-config --cflags --libs libcurl libwebsockets) \
    -lz -lpthread
```

### Windows (MSVC Developer Command Prompt)

```bat
cl /std:c++17 /O2 /EHsc main_updated.cpp ^
   /I"C:\vcpkg\installed\x64-windows\include" ^
   /link /LIBPATH:"C:\vcpkg\installed\x64-windows\lib" ^
   libcurl.lib websockets.lib zlib.lib
```

---

## Run

```bash
./trader
```

On startup the program asks how to authenticate with Naasa:

```
[a] Auto-login   — enter your Keycloak email + password interactively
[m] Manual login — paste a live .AspNetCore.Session cookie + sessionNo
```

### Auto-login mode

Enter your Naasa portal email and password. The program logs in through Keycloak, stores the session cookie in `/tmp/.naasa_ck`, and refreshes it automatically via `ValidateSessionNo` every 10 minutes.

### Manual mode

Obtain the values from your browser (DevTools → Application → Cookies for `x.naasasecurities.com.np`):

| Prompt | Where to find it |
|---|---|
| `.AspNetCore.Session` cookie value | DevTools → Cookies |
| `sessionNo` | Network → `ValidateSessionNo` response → `sessionNo` field |

The `sessionNo` is a space-separated string: `"UserId    token"` — paste it exactly including the spaces.

---

## Runtime Commands

Once logged in and the WebSocket feed is connected, type commands at the prompt:

| Command | Description |
|---|---|
| `add` | Add a new stock watch (prompted interactively) |
| `list` | List all active watches |
| `stop <id>` | Stop a specific watch by its 8-char ID |
| `stopall` | Stop all active watches |
| `fire <SYM>` | Blind-fire orders at bid ceiling without waiting for a WS tick |
| `summary` | Print full order history and P&L summary |
| `stats` | Show order latency stats (p50 / p95 / p99) |
| `time` | Show current Nepal time and market open/close status |
| `naasa` | Re-enter Naasa credentials after a web-login kick (`104^`) |
| `help` | Show this command list |
| `quit` / `exit` | Shutdown gracefully and print final summary |

### Adding a watch (`add`)

```
Symbol (e.g. UPPER)        : HIDCLP
Qty (normal)               : 10
Qty (circuit) [Enter=same] : 20
Max spend (Rs)             : 5000
```

- **Symbol** — stock ticker exactly as listed on NEPSE (auto-uppercased)
- **Qty (normal)** — shares to buy per order under normal tick conditions
- **Qty (circuit)** — shares to buy when a circuit-breaker tick is detected (Enter to use same as normal)
- **Max spend** — hard cap in NPR; no further orders are placed once this is exceeded

---

## How It Works

1. **Authentication** — logs into Keycloak SSO, stores `.AspNetCore.Session` cookie in `/tmp/.naasa_ck`
2. **WebSocket** — connects to `wss://x.naasasecurities.com.np:8006/WebSocket/Connect`, subscribes to watched symbols + NEPSE index
3. **Tick processing** — incoming `101^` frames are base64-decoded and zlib-decompressed; LTP changes trigger order logic
4. **Order POST** — `POST https://x.naasasecurities.com.np/MarketOrder/Order` with JSON payload; success requires HTTP 200 **and** `ErrorCode == 0` in the response body
5. **Keepalive** — GET `/MarketOrder/Order` every 5 min to keep the ASP.NET session alive; `ValidateSessionNo` every 10 min to refresh the WS session token
6. **Session kick** — if `104^User logged in from another source` is received on the WS, the `naasa` command lets you re-authenticate without restarting

---

## Cookie File

The session cookie is written to `/tmp/.naasa_ck` in Netscape cookie-jar format by libcurl. Each order handle reads this file on every request (`CURLOPT_COOKIEFILE`), so a session refresh automatically applies to all in-flight order handles without restart.

On Windows, change `NAASA_COOKIE_FILE` in `main_updated.cpp` to a writable path such as `C:\Temp\.naasa_ck` and rebuild.

---

## Notes

- Market hours: **11:00 – 15:00 NST** (UTC+5:45). Orders outside this window are rejected by the exchange.
- The pre-market warmup at **10:59:55 NST** pings the order endpoint on all active watch handles to prime TCP connections before the open.
- Orders that fail with `ErrorCode != 0` log the `Message` field from the JSON response (e.g. `"Insufficient balance"`).
- The `fire` command is useful for IPO subscriptions or situations where you want to post immediately at the current bid without waiting for a live tick.

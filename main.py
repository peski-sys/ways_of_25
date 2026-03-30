import requests
import time
import ast
import getpass
import random
import string
import threading

BASE_URL = "https://tms.swetasecurities.com/atsweb"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
    "accept-language": "en-US,en;q=0.9",
    "accept-encoding": "gzip, deflate, br, zstd",
}

stop_poll    = threading.Event()
poll_lock    = threading.Lock()
pause_print  = threading.Event()  # set = suppress poll output


def parse_response(resp):
    try:
        return resp.json()
    except Exception:
        try:
            return ast.literal_eval(resp.text.strip())
        except Exception:
            return {"raw": resp.text.strip()}


def poll_loop(session, username):
    while not stop_poll.is_set():
        ts = int(time.time() * 1000)

        with poll_lock:
            r_market = session.get(f"{BASE_URL}/home", params={
                "action": "marketStatus",
                "dojo.preventCache": ts,
            })
            market_data = parse_response(r_market)

            r_sess = session.get(f"{BASE_URL}/login", params={
                "action": "checkUserSession",
                "format": "json",
                "txtUserName": username,
                "dojo.preventCache": ts,
            })
            sess_data = parse_response(r_sess)

        if not pause_print.is_set():
            print(f"[poll] market={market_data.get('data', market_data)} | session={sess_data.get('data', sess_data)}", flush=True)
        time.sleep(1)


def gen_duplicate_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=10))


def main():
    username = input("Login ID: ").strip()
    password = getpass.getpass("Password: ").strip()

    session = requests.Session()
    session.headers.update(HEADERS)

    # ------------------------------------------------------------------
    # Steps 1+2+3: GET /atsweb → redirects → JSESSIONID set
    # ------------------------------------------------------------------
    print("\n[1/4] Connecting...")
    r = session.get(f"{BASE_URL}", allow_redirects=True)
    jsid = session.cookies.get("JSESSIONID", "NOT FOUND")
    print(f"      Final URL  : {r.url}")
    print(f"      Status     : {r.status_code}")
    print(f"      JSESSIONID : {jsid}")

    # ------------------------------------------------------------------
    # Step 4: getLoginPageParams
    # ------------------------------------------------------------------
    r = session.get(f"{BASE_URL}/login", params={
        "action": "getLoginPageParams",
        "format": "json",
        "dojo.preventCache": int(time.time() * 1000),
    })
    print(f"\n[2/4] getLoginPageParams: {r.status_code}")

    # ------------------------------------------------------------------
    # Step 5: POST login
    # ------------------------------------------------------------------
    print(f"\n[3/4] Logging in...")
    r = session.post(f"{BASE_URL}/login", data={
        "action": "login",
        "format": "json",
        "txtUserName": username,
        "txtPassword": password,
    })
    login_data = parse_response(r)
    print(f"      Status   : {r.status_code}")
    print(f"      Response : {login_data}")

    if str(login_data.get("code")) != "0":
        print("\nLogin failed. Exiting.")
        return

    # ------------------------------------------------------------------
    # Step 6: getBOIDUserDetails — fetch account info
    # ------------------------------------------------------------------
    print(f"\n[4/4] Fetching account details...")
    r = session.get(f"{BASE_URL}/order", params={
        "action": "getBOIDUserDetails",
        "format": "json",
        "dojo.preventCache": int(time.time() * 1000),
    })
    boid_data = parse_response(r)
    print(f"      Status   : {r.status_code}")

    user_list = boid_data.get("data", {}).get("userids", [])
    if not user_list:
        print("Failed to fetch account details. Exiting.")
        return

    acc = user_list[0]
    acntid    = acc["clientacntid"]
    client_code = acc["clientCode"]
    last_name   = acc["lastName"]
    nic         = acc["nic"]
    broker_id   = acc["brokerId"]
    client_acc  = f"{client_code} ( {last_name}-{nic})"

    print("\n" + "=" * 60)
    print("  ACCOUNT INFO")
    print("=" * 60)
    print(f"  Name       : {acc.get('clientTitle', '')} {last_name}")
    print(f"  Username   : {acc['username']}")
    print(f"  Client Code: {client_code}")
    print(f"  Account ID : {acntid}")
    print(f"  BOID       : {acc['boid']}")
    print(f"  Broker     : {broker_id}")
    print(f"  Exchange   : {acc['exchangeId']}")
    print(f"  Phone      : {acc['telephone']}")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Start background poll loop
    # ------------------------------------------------------------------
    poller = threading.Thread(target=poll_loop, args=(session, username), daemon=True)
    poller.start()
    print("\n[Poll loop started in background — running every 1s]\n")

    # ------------------------------------------------------------------
    # Order input
    # ------------------------------------------------------------------
    pause_print.set()
    print("-" * 60)
    print("  PLACE BUY ORDER")
    print("-" * 60)
    symbol       = input("  Symbol (e.g. HIDCLP)  : ").strip().upper()
    market_price = input("  Market Price (LTP)    : ").strip()
    price        = input("  Your Bid Price        : ").strip()
    quantity     = input("  Quantity              : ").strip()
    print("-" * 60)
    pause_print.clear()

    order_payload = {
        "action":             "submitOrder",
        "market":             "NEPSE",
        "broker":             broker_id,
        "format":             "json",
        "clientOrderId":      "cseOrderId",
        "brokerClient":       "1",
        "orderStatus":        "Open",
        "filledQty":          "",
        "acntid":             acntid,
        "oldPrice":           "",
        "oldQty":             "",
        "remainder":          "",
        "orderplacedate":     "",
        "marketPrice":        market_price,
        "oldDisclose":        "",
        "txtContraBroker":    "110",
        "txtapprovalReason":  "",
        "txtsenttoapproval":  "no",
        "txtCompId":          "",
        "txtOdrStatus":       "",
        "duplicateOrderId":   gen_duplicate_id(),
        "product":            "web",
        "clientAcc":          client_acc,
        "assetSelect":        "1",
        "actionSelect":       "1",
        "txtSecurity":        symbol,
        "cmbTypeOfOrder":     "1",
        "spnQuantity":        quantity,
        "spnPrice":           price,
        "cmbTif":             "16",
        "cmbTifDays":         "1",
        "cmbBoard":           "1",
        "hiddenSpnCseFee":    "0.02",
        "txtContraBroker_":   "110",
        "brokerClientVal":    "1",
        "confirm":            "1",
    }

    print(f"\n  Submitting order: BUY {quantity} x {symbol} @ {price} (LTP={market_price})...")

    with poll_lock:
        t_start = time.perf_counter()
        r_order = session.post(f"{BASE_URL}/order", data=order_payload)
        t_end   = time.perf_counter()

    rtt_ms = (t_end - t_start) * 1000
    order_resp = parse_response(r_order)

    print(f"\n{'=' * 60}")
    print(f"  ORDER RESPONSE")
    print(f"{'=' * 60}")
    print(f"  Status   : {r_order.status_code}")
    print(f"  Response : {order_resp}")
    print(f"  RTT      : {rtt_ms:.2f} ms")
    print(f"{'=' * 60}\n")

    pause_print.set()
    input("Press Enter to exit...")
    stop_poll.set()


if __name__ == "__main__":
    main()

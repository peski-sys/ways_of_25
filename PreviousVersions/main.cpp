// =============================================================================
// NEPSE Auto Trader — Naasa Securities feed + orderssdasdasdas
// Feed:   wss://x.naasasecurities.com.np:8006/WebSocket/Connect
// Orders: https://x.naasasecurities.com.np/MarketOrder/Order
//
// ── Linux build (Ubuntu 22.04) ────────────────────────────────────────────────
//   g++ -std=c++17 -O3 -march=native -o trader main.cpp \
//       $(pkg-config --cflags --libs libcurl libwebsockets) \
//       -lz -lpthread
//
// ── Linux dependencies ────────────────────────────────────────────────────────
//   apt-get install build-essential g++ pkg-config \
//     libcurl4-openssl-dev libwebsockets-dev zlib1g-dev
//
// ── Windows build (MSVC — Developer Command Prompt) ──────────────────────────
//   cl /std:c++17 /O2 /EHsc main.cpp ^
//      /I"C:\vcpkg\installed\x64-windows\include" ^
//      /link /LIBPATH:"C:\vcpkg\installed\x64-windows\lib" ^
//      libcurl.lib websockets.lib zlib.lib
// =============================================================================

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cmath>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#  include <windows.h>
#else
#  include <termios.h>
#  include <unistd.h>
#  include <pthread.h>
#  include <sched.h>
#endif

#include <curl/curl.h>
#include <libwebsockets.h>
#include <zlib.h>

// ── ANSI palette ──────────────────────────────────────────────────────────────
static const char* R_   = "\033[0m";
static const char* B_   = "\033[1m";
static const char* DM_  = "\033[2m";
static const char* RD_  = "\033[31m";
static const char* YL_  = "\033[33m";
static const char* CY_  = "\033[36m";
static const char* BGN_ = "\033[92m";
static const char* BCY_ = "\033[96m";

// ── Naasa endpoints ───────────────────────────────────────────────────────────
static const char* NAASA_BASE          = "https://x.naasasecurities.com.np";
static const char* NAASA_WS_HOST       = "x.naasasecurities.com.np";
static const int   NAASA_WS_PORT       = 8006;
static const char* NAASA_VALIDATE_URL  = "https://x.naasasecurities.com.np/Login/ValidateSessionNo";
static const char* NAASA_ORDER_URL     = "https://x.naasasecurities.com.np/MarketOrder/Order";
static const char* NAASA_SERVER_TIME_URL = "https://x.naasasecurities.com.np/Home/ServerTime";
static const char* NAASA_COOKIE_FILE   = "/tmp/.naasa_ck";

static const int   NST_OFFSET_SECS    = 20700; // UTC+5:45

// ── Pre-baked JSON payload constants ─────────────────────────────────────────
// Payload = HEAD_PREFIX + symbol + HEAD_SUFFIX + qty + MID + price + TAIL
// HEAD and TAIL are stored per-watch (HEAD embeds the symbol).
// MID and TAIL are global constants — same for every order.
static constexpr char PAYLOAD_HEAD_PREFIX[] =
    "{\"TradingAccount\":\"CNC\",\"Exchange\":\"NEPSE\",\"Scrip\":\"";
static constexpr char PAYLOAD_HEAD_SUFFIX[] = "\",\"Quantity\":\"";
static constexpr char PAYLOAD_MID[]         = "\",\"Price\":\"";
static constexpr char PAYLOAD_TAIL[]        =
    "\",\"Market\":\"0\",\"OrderTerms\":\"DAY\","
    "\"BuySellIndicator\":\"B\",\"BuySellType\":\"Buy\","
    "\"DeliveryTerms\":\"D\",\"MarketSegment\":\"RL\","
    "\"OrderCategory\":\"NORMAL\",\"OrderType\":\"NORMAL\","
    "\"AccRefCode\":\"SELF\",\"TermValidity\":\"\","
    "\"ProductType\":\"CASH\",\"DisclosedQuantity\":\"\","
    "\"isSquareOff\":0}";

// Byte lengths excluding the NUL terminator
static constexpr int PAYLOAD_HEAD_PREFIX_LEN = (int)(sizeof(PAYLOAD_HEAD_PREFIX) - 1);
static constexpr int PAYLOAD_HEAD_SUFFIX_LEN = (int)(sizeof(PAYLOAD_HEAD_SUFFIX) - 1);
static constexpr int PAYLOAD_MID_LEN         = (int)(sizeof(PAYLOAD_MID)         - 1);
static constexpr int PAYLOAD_TAIL_LEN        = (int)(sizeof(PAYLOAD_TAIL)        - 1);

// ── Pre-built Naasa order headers — static fields only, no Cookie ─────────────
// Cookie is injected at watch creation into each watch's order_hdrs / keepalive_hdrs.
static curl_slist* g_naasa_order_hdrs = nullptr;

// ── Naasa session state ───────────────────────────────────────────────────────
static CURL*       g_naasa_curl = nullptr;
static std::mutex  g_naasa_curl_mutex;
static std::string       g_naasa_user_id;
static std::string       g_naasa_session_no;
static std::mutex        g_naasa_session_mutex;
static std::atomic<bool> g_naasa_kicked{false};
// Raw .AspNetCore.Session cookie — protected by g_naasa_session_mutex.
static std::string g_naasa_asp_cookie;

// Stored only in auto-login mode for heartbeat re-login.
static std::string g_naasa_auto_email;
static std::string g_naasa_auto_password;

// ── Login / DNS ───────────────────────────────────────────────────────────────
static std::atomic<bool> g_logged_in{false};
static curl_slist*       g_dns_pins = nullptr;

// ── Helpers: cookie jar ───────────────────────────────────────────────────────
static std::string get_naasa_cookie(const std::string& name) {
    struct curl_slist* list = nullptr;
    curl_easy_getinfo(g_naasa_curl, CURLINFO_COOKIELIST, &list);
    std::string result;
    for (auto* c = list; c; c = c->next) {
        std::vector<std::string> parts;
        std::istringstream ss(c->data);
        std::string p;
        while (std::getline(ss, p, '\t')) parts.push_back(p);
        if (parts.size() >= 7 && parts[5] == name) { result = parts[6]; break; }
    }
    curl_slist_free_all(list);
    return result;
}

static std::string build_naasa_cookie_header() {
    struct curl_slist* list = nullptr;
    curl_easy_getinfo(g_naasa_curl, CURLINFO_COOKIELIST, &list);
    std::map<std::string, std::string> jar;
    for (auto* c = list; c; c = c->next) {
        std::vector<std::string> parts;
        std::istringstream ss(c->data);
        std::string p;
        while (std::getline(ss, p, '\t')) parts.push_back(p);
        if (parts.size() >= 7) jar[parts[5]] = parts[6];
    }
    curl_slist_free_all(list);
    std::string out;
    for (auto& [k, v] : jar) { if (!out.empty()) out += "; "; out += k + "=" + v; }
    return out;
}

// ── HttpResponse ──────────────────────────────────────────────────────────────
struct HttpResponse {
    long        status       = 0;
    std::string body;
    CURLcode    curl_code    = CURLE_OK;
    std::string redirect_url;
};

static size_t write_cb(char* ptr, size_t size, size_t nmemb, void* ud);
static void   apply_conn_opts(CURL* c);

// ── Naasa HTTP request — unified helper ───────────────────────────────────────
static HttpResponse naasa_request(const std::string& url,
                                   const char* method,
                                   const std::string& post_body = "") {
    std::string asp_cookie_snap;
    {
        std::lock_guard<std::mutex> sl(g_naasa_session_mutex);
        asp_cookie_snap = g_naasa_asp_cookie;
    }

    std::lock_guard<std::mutex> lk(g_naasa_curl_mutex);
    std::string body;
    body.reserve(65536);
    curl_easy_reset(g_naasa_curl);
    curl_easy_setopt(g_naasa_curl, CURLOPT_URL,            url.c_str());
    curl_easy_setopt(g_naasa_curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEFILE,     NAASA_COOKIE_FILE);
    curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEJAR,      NAASA_COOKIE_FILE);
    std::string naasa_cookie_hdr;
    if (!asp_cookie_snap.empty()) {
        naasa_cookie_hdr = ".AspNetCore.Session=" + asp_cookie_snap;
        curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIE, naasa_cookie_hdr.c_str());
    }
    curl_easy_setopt(g_naasa_curl, CURLOPT_TCP_NODELAY,    1L);
    apply_conn_opts(g_naasa_curl);
    curl_easy_setopt(g_naasa_curl, CURLOPT_CONNECTTIMEOUT, 30L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_TIMEOUT,        60L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(g_naasa_curl, CURLOPT_WRITEFUNCTION,  write_cb);
    curl_easy_setopt(g_naasa_curl, CURLOPT_WRITEDATA,      &body);

    curl_slist* hdrs = nullptr;
    const char* ua = "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";

    if (std::string_view(method) == "GET") {
        curl_easy_setopt(g_naasa_curl, CURLOPT_FOLLOWLOCATION, 1L);
        hdrs = curl_slist_append(hdrs, ua);
        hdrs = curl_slist_append(hdrs, "Accept-Language: en-US,en;q=0.9");
    } else if (std::string_view(method) == "POST_FORM") {
        curl_easy_setopt(g_naasa_curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POST,           1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDS,     post_body.c_str());
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDSIZE,  (long)post_body.size());
        hdrs = curl_slist_append(hdrs, "Content-Type: application/x-www-form-urlencoded");
        hdrs = curl_slist_append(hdrs, ua);
    } else { // POST_JSON_EMPTY
        curl_easy_setopt(g_naasa_curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POST,           1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDS,     "");
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDSIZE,  0L);
        hdrs = curl_slist_append(hdrs, "Content-Type: application/json; charset=utf-8");
        hdrs = curl_slist_append(hdrs, "Accept: application/json, text/javascript, */*; q=0.01");
        hdrs = curl_slist_append(hdrs, "X-Requested-With: XMLHttpRequest");
        hdrs = curl_slist_append(hdrs, (std::string("Origin: ") + NAASA_BASE).c_str());
        hdrs = curl_slist_append(hdrs, ua);
    }

    curl_easy_setopt(g_naasa_curl, CURLOPT_HTTPHEADER, hdrs);
    CURLcode res = curl_easy_perform(g_naasa_curl);
    curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIELIST, "FLUSH");
    long http_code = 0;
    curl_easy_getinfo(g_naasa_curl, CURLINFO_RESPONSE_CODE, &http_code);
    char* redir_raw = nullptr;
    curl_easy_getinfo(g_naasa_curl, CURLINFO_REDIRECT_URL, &redir_raw);
    std::string redir_url = redir_raw ? redir_raw : "";
    curl_slist_free_all(hdrs);
    return {http_code, body, res, redir_url};
}

// ── Session parsing — no heap-allocating JSON parser ─────────────────────────
// Parses {"sessionNo":"UserId    token"} and stores globals. Returns true on success.
static bool parse_and_store_session_no(const std::string& body) {
    static constexpr char KEY[] = "\"sessionNo\":\"";
    const char* p = strstr(body.c_str(), KEY);
    if (!p) return false;
    p += sizeof(KEY) - 1;
    const char* e = strchr(p, '"');
    if (!e || e == p) return false;
    std::string_view sno(p, (size_t)(e - p));
    auto sp = sno.find(' ');
    std::string uid(sp != std::string_view::npos ? sno.substr(0, sp) : sno);
    std::string sno_str(sno);
    std::lock_guard<std::mutex> lk(g_naasa_session_mutex);
    g_naasa_user_id    = std::move(uid);
    g_naasa_session_no = std::move(sno_str);
    return true;
}

// ── Keycloak form action extraction ───────────────────────────────────────────
static std::string extract_form_action(const std::string& html) {
    const std::string tag = "action=\"";
    size_t p = 0;
    while ((p = html.find(tag, p)) != std::string::npos) {
        p += tag.size();
        size_t end = html.find('"', p);
        if (end == std::string::npos) break;
        std::string url = html.substr(p, end - p);
        if (url.find("authenticate") != std::string::npos) {
            size_t a;
            while ((a = url.find("&amp;")) != std::string::npos)
                url.replace(a, 5, "&");
            return url;
        }
        p = end + 1;
    }
    return "";
}

static std::string url_encode(const std::string& s);
static void        sys_log(const std::string& msg, const std::string& level);

// ── Keycloak login ────────────────────────────────────────────────────────────
static void naasa_do_login(const std::string& email, const std::string& password) {
    std::remove(NAASA_COOKIE_FILE);
    sys_log("Naasa: connecting to login page...", "sys");
    auto r1 = naasa_request(std::string(NAASA_BASE) + "/", "GET");
    std::string form_action = extract_form_action(r1.body);
    if (form_action.empty()) {
        std::string snippet = r1.body.size() > 800 ? r1.body.substr(0, 800) : r1.body;
        throw std::runtime_error(
            "Naasa: could not find Keycloak form action\n"
            "  HTTP status : " + std::to_string(r1.status) + "\n"
            "  Curl error  : " + curl_easy_strerror(r1.curl_code) + "\n"
            "  Redirect URL: " + (r1.redirect_url.empty() ? "(none)" : r1.redirect_url) + "\n"
            "  Body snippet: " + (snippet.empty() ? "(empty)" : snippet));
    }
    sys_log("Naasa: submitting credentials...", "sys");
    std::string form_body = "username=" + url_encode(email)
                          + "&password=" + url_encode(password)
                          + "&credentialId=&login=Sign+In";
    naasa_request(form_action, "POST_FORM", form_body);

    sys_log("Naasa: verifying session cookie...", "sys");
    std::string session_cookie;
    {
        std::lock_guard<std::mutex> lk(g_naasa_curl_mutex);
        session_cookie = get_naasa_cookie(".AspNetCore.Session");
    }
    if (session_cookie.empty())
        throw std::runtime_error("Naasa: login failed — .AspNetCore.Session cookie not set");

    sys_log("Naasa: fetching WS session number...", "sys");
    auto r2 = naasa_request(NAASA_VALIDATE_URL, "POST_JSON_EMPTY");
    if (!parse_and_store_session_no(r2.body))
        throw std::runtime_error("Naasa: could not parse ValidateSessionNo: " + r2.body);
}

static bool naasa_validate_session() {
    auto r = naasa_request(NAASA_VALIDATE_URL, "POST_JSON_EMPTY");
    if (!parse_and_store_session_no(r.body)) {
        bool is_html = r.body.size() > 5 && r.body.compare(0, 5, "<!DOC") == 0;
        sys_log("ValidateSessionNo  status=" + std::to_string(r.status)
                + (is_html ? "  body=<HTML error page>" : "  body=" + r.body.substr(0, 200)), "err");
        return false;
    }
    return true;
}

// ── Watch ─────────────────────────────────────────────────────────────────────
struct OrderRecord {
    double price;
    int    qty;
};

struct Watch {
    std::string              id;
    std::string              symbol;
    int                      qty       = 0;
    int                      endingQty = 0;
    double                   maxSpend  = 0.0;
    double                   spent     = 0.0;    // protected by lat_mutex
    std::vector<OrderRecord> orders;              // protected by lat_mutex

    std::atomic<bool>   stop{false};
    std::atomic<double> last_ltp{-1.0};
    std::atomic<bool>   first_order_done{false};
    std::atomic<double> open_price{0.0};

    // Latency samples — protected by lat_mutex
    std::mutex           lat_mutex;
    std::vector<int64_t> tick_to_order_us;
    std::vector<int64_t> api_rtt_us;

    // ── Order curl handle ─────────────────────────────────────────────────────
    // Owned exclusively by place_order / cmd_fire. Never touched by heartbeat.
    CURL*       order_curl  = nullptr;
    std::mutex  order_curl_mutex;
    // Complete pre-built header list: Cookie + all static order headers.
    // Rebuilt under order_curl_mutex whenever the session cookie changes.
    curl_slist* order_hdrs  = nullptr;

    // ── Keepalive curl handle ─────────────────────────────────────────────────
    // Owned exclusively by the heartbeat thread. Zero contention with order_curl.
    CURL*       keepalive_curl  = nullptr;
    std::mutex  keepalive_curl_mutex;
    curl_slist* keepalive_hdrs  = nullptr; // Cookie + Accept + X-Requested-With

    // ── Zero-alloc payload template ───────────────────────────────────────────
    // Head = PAYLOAD_HEAD_PREFIX + symbol + PAYLOAD_HEAD_SUFFIX
    // Built once at watch creation. At order time: memcpy head, to_chars qty,
    // memcpy PAYLOAD_MID, to_chars price, memcpy PAYLOAD_TAIL.
    char payload_head[256] = {};
    int  payload_head_len  = 0;

    std::thread worker;

    ~Watch() {
        if (order_curl)     { curl_easy_cleanup(order_curl);     order_curl     = nullptr; }
        if (keepalive_curl) { curl_easy_cleanup(keepalive_curl); keepalive_curl = nullptr; }
        curl_slist_free_all(order_hdrs);
        curl_slist_free_all(keepalive_hdrs);
    }
};

static std::unordered_map<std::string, std::shared_ptr<Watch>> g_watches;
static std::mutex                                               g_watches_mutex;

// ── Shutdown flag ─────────────────────────────────────────────────────────────
static std::atomic<bool> g_shutdown{false};

// ── Print ─────────────────────────────────────────────────────────────────────
static std::mutex g_print_mutex;

static void cprint(const std::string& text) {
    std::lock_guard<std::mutex> lk(g_print_mutex);
    std::cout << text << "\n" << std::flush;
}

// ── Nepal time ────────────────────────────────────────────────────────────────
static struct tm to_nst(time_t base) {
    time_t t = base + NST_OFFSET_SECS;
    struct tm result{};
#ifdef _WIN32
    gmtime_s(&result, &t);
#else
    gmtime_r(&t, &result);
#endif
    return result;
}

static std::string nepal_ts() {
    auto now = std::chrono::system_clock::now();
    struct tm tm_nst = to_nst(std::chrono::system_clock::to_time_t(now));
    char buf[16];
    strftime(buf, sizeof(buf), "%H:%M:%S", &tm_nst);
    return buf;
}

static void sys_log(const std::string& msg, const std::string& level) {
    std::string tag;
    if      (level == "sys") tag = std::string(DM_)  + "[SYS]" + R_;
    else if (level == "err") tag = std::string(RD_)  + "[ERR]" + R_;
    else if (level == "ok")  tag = std::string(BGN_) + "[OK ]" + R_;
    else                     tag = "[   ]";
    cprint(std::string(DM_) + nepal_ts() + R_ + "  " + tag + "  " + msg);
}

static void watch_log(const std::string& wid, const std::string& msg,
                      const std::string& level = "info") {
    std::string color;
    if      (level == "error")   color = RD_;
    else if (level == "warning") color = YL_;
    std::string id8 = wid.size() > 8 ? wid.substr(0, 8) : wid;
    cprint(std::string(DM_) + nepal_ts() + R_ +
           "  " + BCY_ + "[" + id8 + "]" + R_ +
           "  " + color + msg + (color.empty() ? "" : R_));
}

// ── Base64 decode ─────────────────────────────────────────────────────────────
static void base64_decode_into(std::string_view in, std::vector<uint8_t>& out) {
    static const int8_t T[256] = {
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
        52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1,
        -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
        -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
    };
    out.clear();
    out.reserve(in.size() * 3 / 4);
    int val = 0, valb = -8;
    for (unsigned char c : in) {
        int8_t d = T[(uint8_t)c];
        if (d < 0) continue;
        val = (val << 6) | d;
        valb += 6;
        if (valb >= 0) {
            out.push_back((uint8_t)((val >> valb) & 0xFF));
            valb -= 8;
        }
    }
}

// ── zlib decompress ───────────────────────────────────────────────────────────
static void zlib_decompress_into(const std::vector<uint8_t>& compressed, std::string& out) {
    out.clear();
    if (compressed.empty()) return;

    thread_local z_stream zs   = {};
    thread_local bool     init = false;

    if (!init) {
        if (inflateInit(&zs) != Z_OK)
            throw std::runtime_error("inflateInit failed");
        init = true;
    } else {
        inflateReset(&zs);
    }

    zs.next_in  = const_cast<Bytef*>(compressed.data());
    zs.avail_in = (uInt)compressed.size();

    out.reserve(compressed.size() * 4);
    char buf[32768];
    int ret;
    do {
        zs.next_out  = (Bytef*)buf;
        zs.avail_out = sizeof(buf);
        ret = inflate(&zs, Z_NO_FLUSH);
        if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR)
            throw std::runtime_error("inflate failed");
        out.append(buf, sizeof(buf) - zs.avail_out);
    } while (ret != Z_STREAM_END);
}

// ── URL encode ────────────────────────────────────────────────────────────────
static std::string url_encode(const std::string& s) {
    std::string out;
    out.reserve(s.size() * 3);
    for (unsigned char c : s) {
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            out += (char)c;
        } else {
            char buf[4];
            snprintf(buf, sizeof(buf), "%%%02X", c);
            out += buf;
        }
    }
    return out;
}

// ── One-pass field tokenizer ──────────────────────────────────────────────────
// Splits s by delim into sv_out[0..max_out-1]. Returns number of fields stored.
// Stops filling after max_out fields but continues scanning to get the true count.
// Single pass over the string — replaces all nth_field / count_fields calls.
static int tokenize(std::string_view s, char delim,
                    std::string_view* sv_out, int max_out) {
    int n = 0;
    size_t start = 0;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || s[i] == delim) {
            if (n < max_out)
                sv_out[n] = s.substr(start, i - start);
            ++n;
            start = i + 1;
        }
    }
    return n;
}

// ── UUID ─────────────────────────────────────────────────────────────────────
static std::string generate_uuid() {
    thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> d16(0, 15);
    std::uniform_int_distribution<int> d4(8, 11);
    std::ostringstream ss;
    ss << std::hex;
    for (int i = 0; i < 8;  i++) ss << d16(gen); ss << '-';
    for (int i = 0; i < 4;  i++) ss << d16(gen); ss << '-';
    ss << '4';
    for (int i = 0; i < 3;  i++) ss << d16(gen); ss << '-';
    ss << d4(gen);
    for (int i = 0; i < 3;  i++) ss << d16(gen); ss << '-';
    for (int i = 0; i < 12; i++) ss << d16(gen);
    return ss.str();
}

// ── Nepal time helpers ────────────────────────────────────────────────────────
static int nepal_secs_since_midnight() {
    struct tm tm_nst = to_nst(std::chrono::system_clock::to_time_t(
                                  std::chrono::system_clock::now()));
    return tm_nst.tm_hour * 3600 + tm_nst.tm_min * 60 + tm_nst.tm_sec;
}

static bool is_market_open() {
    int s = nepal_secs_since_midnight();
    return s > (11 * 3600) && s < (15 * 3600);
}

// ── libcurl helpers ───────────────────────────────────────────────────────────
static size_t write_cb(char* ptr, size_t size, size_t nmemb, void* ud) {
    auto* buf = static_cast<std::string*>(ud);
    buf->append(ptr, size * nmemb);
    return size * nmemb;
}

// Permanent connection options — set ONCE at handle creation, never again.
// TCP+TLS+HTTP/2 connection cache survives across all requests on the same handle.
static void apply_conn_opts(CURL* c) {
    curl_easy_setopt(c, CURLOPT_NOSIGNAL,        1L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS,   5000L);
    curl_easy_setopt(c, CURLOPT_TCP_KEEPALIVE,   1L);
    curl_easy_setopt(c, CURLOPT_TCP_KEEPIDLE,   10L);  // probe after 10s idle (IIS closes ~15s)
    curl_easy_setopt(c, CURLOPT_TCP_KEEPINTVL,   5L);
    curl_easy_setopt(c, CURLOPT_MAXAGE_CONN,   1200L);
    curl_easy_setopt(c, CURLOPT_FORBID_REUSE,    0L);
    curl_easy_setopt(c, CURLOPT_FRESH_CONNECT,   0L);
    curl_easy_setopt(c, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
}

// ── Header list builders ──────────────────────────────────────────────────────
// Build the complete order header list: Cookie + all static order headers.
// Called at watch creation and on cookie refresh. Caller holds order_curl_mutex.
static curl_slist* build_order_hdrs_list(const std::string& cookie_val) {
    curl_slist* h = nullptr;
    std::string ck = "Cookie: .AspNetCore.Session=" + cookie_val;
    h = curl_slist_append(h, ck.c_str());
    for (curl_slist* p = g_naasa_order_hdrs; p; p = p->next)
        h = curl_slist_append(h, p->data);
    return h;
}

// Build the keepalive header list: Cookie + minimal GET headers.
// Called at watch creation and on cookie refresh. Caller holds keepalive_curl_mutex.
static curl_slist* build_keepalive_hdrs_list(const std::string& cookie_val) {
    curl_slist* h = nullptr;
    std::string ck = "Cookie: .AspNetCore.Session=" + cookie_val;
    h = curl_slist_append(h, ck.c_str());
    h = curl_slist_append(h, "Accept: application/json, text/javascript, */*; q=0.01");
    h = curl_slist_append(h, "X-Requested-With: XMLHttpRequest");
    return h;
}

// Rebuild pre-baked header lists for a single watch after a cookie change.
static void rebuild_watch_order_hdrs(Watch& w, const std::string& cookie_val) {
    {
        std::lock_guard<std::mutex> lk(w.order_curl_mutex);
        curl_slist_free_all(w.order_hdrs);
        w.order_hdrs = build_order_hdrs_list(cookie_val);
    }
    {
        std::lock_guard<std::mutex> lk(w.keepalive_curl_mutex);
        curl_slist_free_all(w.keepalive_hdrs);
        w.keepalive_hdrs = build_keepalive_hdrs_list(cookie_val);
    }
}

// Rebuild pre-baked header lists for all active watches.
// Called after any session cookie update.
static void rebuild_all_watch_order_hdrs() {
    std::string cv;
    {
        std::lock_guard<std::mutex> lk(g_naasa_session_mutex);
        cv = g_naasa_asp_cookie;
    }
    std::lock_guard<std::mutex> lk(g_watches_mutex);
    for (auto& [wid, w] : g_watches)
        rebuild_watch_order_hdrs(*w, cv);
}

// ── Zero-alloc payload builder ────────────────────────────────────────────────
// Writes the complete JSON order payload into buf[] with no heap allocation.
// Uses the pre-baked watch->payload_head (which embeds the symbol) and the
// global PAYLOAD_MID / PAYLOAD_TAIL constants.
// Returns byte count written (does NOT NUL-terminate).
static size_t build_order_payload(const Watch& w, int qty, double price,
                                   char* buf, size_t buf_size) {
    char* p   = buf;
    char* end = buf + buf_size;

    memcpy(p, w.payload_head, (size_t)w.payload_head_len);
    p += w.payload_head_len;

    auto r1 = std::to_chars(p, end - 64, qty);
    p = r1.ptr;

    memcpy(p, PAYLOAD_MID, (size_t)PAYLOAD_MID_LEN);
    p += PAYLOAD_MID_LEN;

    int r2_len = snprintf(p, (size_t)(end - 4 - p), "%.1f", price);
    if (r2_len > 0) p += r2_len;

    memcpy(p, PAYLOAD_TAIL, (size_t)PAYLOAD_TAIL_LEN);
    p += PAYLOAD_TAIL_LEN;

    return (size_t)(p - buf);
}

// ── Order placement ───────────────────────────────────────────────────────────
// Hot path. Called directly from the WS receive callback with Watch& in hand —
// no ID lookup, no map search, no session mutex on this path.
static void print_totals();

static void place_order(Watch& w,
                        double bid_price, int qty,
                        std::chrono::steady_clock::time_point tick_time) {
    // Build payload — stack only, zero heap allocations.
    static thread_local char order_buf[512];
    const size_t payload_len = build_order_payload(w, qty, bid_price,
                                                    order_buf, sizeof(order_buf));

    auto do_post = [&]() -> HttpResponse {
        std::lock_guard<std::mutex> wl(w.order_curl_mutex);
        static thread_local std::string resp_body;
        resp_body.clear();

        // order_hdrs is pre-built (Cookie + static headers) — no string construction.
        // No curl_easy_reset() — permanent options and HTTP/2 connection survive.
        curl_easy_setopt(w.order_curl, CURLOPT_URL,           NAASA_ORDER_URL);
        curl_easy_setopt(w.order_curl, CURLOPT_POST,          1L);
        curl_easy_setopt(w.order_curl, CURLOPT_POSTFIELDS,    order_buf);
        curl_easy_setopt(w.order_curl, CURLOPT_POSTFIELDSIZE, (long)payload_len);
        curl_easy_setopt(w.order_curl, CURLOPT_HTTPHEADER,    w.order_hdrs);
        curl_easy_setopt(w.order_curl, CURLOPT_WRITEDATA,     &resp_body);
        curl_easy_perform(w.order_curl);

        long code = 0;
        curl_easy_getinfo(w.order_curl, CURLINFO_RESPONSE_CODE, &code);
        return {code, resp_body};
    };

    auto t_send = std::chrono::steady_clock::now();
    HttpResponse r = do_post();
    auto t_recv = std::chrono::steady_clock::now();

    int64_t rtt_us = std::chrono::duration_cast<std::chrono::microseconds>(t_recv - t_send).count();
    int64_t t2o_us = std::chrono::duration_cast<std::chrono::microseconds>(t_send - tick_time).count();
    {
        std::lock_guard<std::mutex> ll(w.lat_mutex);
        w.tick_to_order_us.push_back(t2o_us);
        w.api_rtt_us.push_back(rtt_us);
    }

    // ── Order success: HTTP 200 AND "ErrorCode":0 ─────────────────────────────
    // strstr check — no JSON parse tree, no heap allocation.
    bool success = false;
    if (r.status == 200) {
        const char* p = strstr(r.body.c_str(), "\"ErrorCode\":");
        if (p) {
            p += 12;
            while (*p == ' ') ++p;
            success = (*p == '0') && (p[1] == ',' || p[1] == '}' || p[1] == ' ');
        }
    }

    if (success) {
        double spent;
        {
            std::lock_guard<std::mutex> ll(w.lat_mutex);
            w.orders.push_back({bid_price, qty});
            w.spent += bid_price * qty;
            spent = w.spent;
        }
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(1);
        ss << BGN_ << "  ★ ORDER PLACED" << R_
           << "  watch=" << BCY_ << w.id << R_
           << "  price=" << B_ << bid_price << R_
           << "  qty=" << B_ << qty << R_
           << "  spent=" << BGN_ << "Rs " << spent << R_
           << " / Rs " << w.maxSpend;
        cprint(ss.str());
        print_totals();

        if (spent >= w.maxSpend) {
            cprint(std::string(YL_) + "  [BUDGET HIT]" + R_
                   + "  watch=" + w.id + " — stopping");
            w.stop.store(true);
        }
        return;
    }

    // Failure — extract Message field without JSON parser
    std::string msg;
    const char* mp = strstr(r.body.c_str(), "\"Message\":\"");
    if (mp) {
        mp += 11;
        const char* me = strchr(mp, '"');
        if (me) msg.assign(mp, me);
    }
    if (msg.empty()) msg = r.body.size() > 300 ? r.body.substr(0, 300) : r.body;
    watch_log(w.id, "Order failed (HTTP " + std::to_string(r.status) + "): " + msg, "error");
}

// ── Naasa WebSocket (libwebsockets) ───────────────────────────────────────────
struct NaasaWsCtx {
    std::string            watch_id;
    std::shared_ptr<Watch> watch_ptr;
    std::string            symbol;
    std::string            sub_msg;
    std::string            cookie_header;
    bool         subscribed     = false;
    bool         send_keepalive = false;
    bool         disconnected   = false;
    struct lws*  wsi            = nullptr;
    std::string  rx_buf;
    std::chrono::steady_clock::time_point last_rx{std::chrono::steady_clock::now()};
};

static int naasa_ws_callback(struct lws* wsi, enum lws_callback_reasons reason,
                              void* user, void* in, size_t len);

static struct lws_protocols naasa_ws_protocols[] = {
    {"naasa-feed", naasa_ws_callback, sizeof(NaasaWsCtx*), 131072, 0, nullptr, 0},
    { nullptr, nullptr, 0, 0 },
};

static int naasa_ws_callback(struct lws* wsi, enum lws_callback_reasons reason,
                               void* user, void* in, size_t len)
{
    NaasaWsCtx** pctx = static_cast<NaasaWsCtx**>(user);
    NaasaWsCtx*  ctx  = pctx ? *pctx : nullptr;

    switch (reason) {

    case LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER: {
        if (!ctx) break;
        unsigned char** p   = reinterpret_cast<unsigned char**>(in);
        unsigned char*  end = (*p) + len;
        if (!ctx->cookie_header.empty()) {
            if (lws_add_http_header_by_name(wsi,
                    reinterpret_cast<const unsigned char*>("Cookie:"),
                    reinterpret_cast<const unsigned char*>(ctx->cookie_header.c_str()),
                    (int)ctx->cookie_header.size(), p, end))
                return -1;
        }
        const char* origin = NAASA_BASE;
        if (lws_add_http_header_by_name(wsi,
                reinterpret_cast<const unsigned char*>("Origin:"),
                reinterpret_cast<const unsigned char*>(origin),
                (int)strlen(origin), p, end))
            return -1;
        const char* ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";
        if (lws_add_http_header_by_name(wsi,
                reinterpret_cast<const unsigned char*>("User-Agent:"),
                reinterpret_cast<const unsigned char*>(ua),
                (int)strlen(ua), p, end))
            return -1;
        break;
    }

    case LWS_CALLBACK_CLIENT_ESTABLISHED:
        if (ctx) {
            ctx->last_rx = std::chrono::steady_clock::now();
            watch_log(ctx->watch_id, "WS connected", "info");
            lws_callback_on_writable(wsi);
        }
        break;

    case LWS_CALLBACK_CLIENT_WRITEABLE: {
        if (!ctx) break;
        if (!ctx->subscribed) {
            const std::string& msg = ctx->sub_msg;
            std::vector<unsigned char> buf(LWS_PRE + msg.size());
            memcpy(buf.data() + LWS_PRE, msg.c_str(), msg.size());
            lws_write(wsi, buf.data() + LWS_PRE, msg.size(), LWS_WRITE_TEXT);
            watch_log(ctx->watch_id, "Subscribed  → " + msg, "info");
            ctx->subscribed = true;
        } else if (ctx->send_keepalive) {
            unsigned char buf[LWS_PRE + 1] = {};
            lws_write(wsi, buf + LWS_PRE, 0, LWS_WRITE_TEXT);
            ctx->send_keepalive = false;
        }
        break;
    }

    case LWS_CALLBACK_CLIENT_RECEIVE: {
        if (!ctx || !in || len == 0) break;

        auto t_recv = std::chrono::steady_clock::now();
        ctx->last_rx = t_recv;

        if (ctx->watch_ptr->stop.load()) {
            lws_close_reason(wsi, LWS_CLOSE_STATUS_NORMAL, nullptr, 0);
            return -1;
        }

        ctx->rx_buf.append((const char*)in, len);
        if (!lws_is_final_fragment(wsi)) break;

        std::string raw = std::move(ctx->rx_buf);
        ctx->rx_buf.clear();
        if (raw.empty()) break;

        // Peek at the numeric prefix before the first '^'
        auto caret = raw.find('^');
        if (caret == std::string::npos) break;

        std::string_view prefix(raw.data(), caret);

        if (prefix == "104") {
            if (raw.find("User logged in") != std::string::npos) {
                g_naasa_kicked.store(true);
                watch_log(ctx->watch_id,
                    "Session stolen by web login — type 'naasa' command to restore", "error");
            } else {
                watch_log(ctx->watch_id, "Server control: " + raw + " — reconnecting", "warning");
            }
            return -1;
        }

        // Decompress payload
        thread_local std::vector<uint8_t> tl_b64_buf;
        thread_local std::string          tl_decomp_buf;
        try {
            std::string_view payload(raw.data() + caret + 1, raw.size() - caret - 1);
            base64_decode_into(payload, tl_b64_buf);
            zlib_decompress_into(tl_b64_buf, tl_decomp_buf);
        } catch (...) {
            break;
        }
        const std::string& decoded = tl_decomp_buf;

        // ── Tick processing — one-pass tokenizer, zero allocations ────────────
        std::string_view sv(decoded);
        while (!sv.empty()) {
            auto nl   = sv.find('\n');
            std::string_view line = sv.substr(0, nl == std::string_view::npos ? sv.size() : nl);
            sv = (nl == std::string_view::npos) ? std::string_view{} : sv.substr(nl + 1);
            if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
            if (line.empty()) continue;

            // One-pass split of the outer '$'-delimited message
            std::string_view outer[6];
            if (tokenize(line, '$', outer, 6) < 5) continue;
            if (outer[0] != "1") continue;

            std::string_view ver_sym = outer[2];
            if (ver_sym.size() < 5 || ver_sym.substr(0, 4) != "25.1") continue;

            auto bang = ver_sym.find('!');
            if (bang == std::string_view::npos) continue;
            auto sym = ver_sym.substr(bang + 1);
            if (sym != ctx->symbol) continue;

            // One-pass split of the inner '^'-delimited data block
            // We need fields 0 (LTP) and 14 (prev_close) — stop after 15.
            std::string_view inner[16];
            if (tokenize(outer[4], '^', inner, 16) < 15) continue;

            char* ep;
            double ltp = std::strtod(inner[0].data(), &ep);
            if (ep == inner[0].data()) continue;
            double prev_close = std::strtod(inner[14].data(), &ep);
            if (ep == inner[14].data()) continue;

            ctx->watch_ptr->open_price.store(prev_close, std::memory_order_relaxed);

            double circuit   = std::floor(prev_close * 1.10 * 10.0) / 10.0;
            double bid_price = std::min(std::floor(ltp * 1.02 * 10.0) / 10.0, circuit);
            bool   at_circuit = bid_price >= circuit;

            if (!is_market_open()) {
                char tbuf[96];
                snprintf(tbuf, sizeof(tbuf), "Tick  LTP=%.1f  bid=%.1f  circuit=%.1f%s",
                         ltp, bid_price, circuit, at_circuit ? "  [CIRCUIT]" : "");
                watch_log(ctx->watch_id, tbuf, "info");
                continue;
            }

            Watch& w = *ctx->watch_ptr;
            if (w.stop.load(std::memory_order_acquire)) break;

            int qty = at_circuit ? w.endingQty : w.qty;

            // ── Lock-free trigger decision ────────────────────────────────────
            // first_order_done and last_ltp are both std::atomic — no mutex needed.
            // compare_exchange gives atomic test-and-set for the first-order flag.
            bool expected = false;
            bool is_first = w.first_order_done.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_acquire);
            double prev_ltp = w.last_ltp.exchange(ltp, std::memory_order_acq_rel);

            if (is_first || (prev_ltp >= 0.0 && prev_ltp != ltp)) {
                place_order(w, bid_price, qty, t_recv);
            }
        }
        break;
    }

    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
        if (ctx) {
            ctx->disconnected = true;
            watch_log(ctx->watch_id, "Naasa WS connection error", "error");
        }
        return -1;

    case LWS_CALLBACK_CLIENT_CLOSED:
    case LWS_CALLBACK_CLOSED:
        if (ctx) {
            ctx->disconnected = true;
            watch_log(ctx->watch_id, "WS disconnected", "warning");
        }
        break;

    default:
        break;
    }
    return 0;
}

static void run_watch_socket(const std::string& watch_id) {
    std::shared_ptr<Watch> watch_ptr_snap;
    std::string symbol;
    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        auto it = g_watches.find(watch_id);
        if (it == g_watches.end() || it->second->stop.load()) return;
        watch_ptr_snap = it->second;
        symbol         = watch_ptr_snap->symbol;
    }

    std::string sub_msg = "ADD^3^25.1!" + symbol + "^25.1!NEPSE^25.1!SENSIND^";
    int reconnect_delay = 1;

    while (true) {
        if (watch_ptr_snap->stop.load()) break;

        std::string user_id, session_no, cookie_val;
        {
            std::lock_guard<std::mutex> lk(g_naasa_session_mutex);
            user_id    = g_naasa_user_id;
            session_no = g_naasa_session_no;
        }

        if (g_naasa_kicked.load()) {
            watch_log(watch_id, "Waiting for session restore — type 'naasa' command", "warning");
            std::this_thread::sleep_for(std::chrono::seconds(5));
            continue;
        }
        if (user_id.empty() || session_no.empty()) {
            watch_log(watch_id, "Naasa session not ready — waiting 500ms", "warning");
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        {
            std::lock_guard<std::mutex> lk(g_naasa_curl_mutex);
            cookie_val = build_naasa_cookie_header();
        }

        std::string ws_path = std::string("/WebSocket/Connect?UserId=") + user_id
                            + "&Password=" + url_encode(session_no)
                            + "&protocol=WSS&ClientIP=&Source=1";

        NaasaWsCtx ctx;
        ctx.watch_id       = watch_id;
        ctx.watch_ptr      = watch_ptr_snap;
        ctx.symbol         = symbol;
        ctx.sub_msg        = sub_msg;
        ctx.cookie_header  = cookie_val;
        ctx.subscribed     = false;
        ctx.send_keepalive = false;
        NaasaWsCtx* pctx = &ctx;

        lws_context_creation_info info{};
        info.port        = CONTEXT_PORT_NO_LISTEN;
        info.protocols   = naasa_ws_protocols;
        info.options     = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;
        info.ka_time     = 20;
        info.ka_probes   = 3;
        info.ka_interval = 5;

        struct lws_context* lws_ctx = lws_create_context(&info);
        if (!lws_ctx) {
            watch_log(watch_id, "lws_create_context failed — retry in "
                      + std::to_string(reconnect_delay) + "s", "error");
            std::this_thread::sleep_for(std::chrono::seconds(reconnect_delay));
            reconnect_delay = std::min(reconnect_delay * 2, 30);
            continue;
        }

        lws_client_connect_info ccinfo{};
        ccinfo.context        = lws_ctx;
        ccinfo.address        = NAASA_WS_HOST;
        ccinfo.port           = NAASA_WS_PORT;
        ccinfo.path           = ws_path.c_str();
        ccinfo.host           = NAASA_WS_HOST;
        ccinfo.origin         = NAASA_WS_HOST;
        ccinfo.protocol       = naasa_ws_protocols[0].name;
        ccinfo.ssl_connection = LCCSCF_USE_SSL
                              | LCCSCF_ALLOW_SELFSIGNED
                              | LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
        ccinfo.userdata       = &pctx;
        ccinfo.pwsi           = &ctx.wsi;

        struct lws* wsi = lws_client_connect_via_info(&ccinfo);
        if (!wsi) {
            watch_log(watch_id, "Naasa WS connect failed — retry in "
                      + std::to_string(reconnect_delay) + "s", "error");
            lws_context_destroy(lws_ctx);
            std::this_thread::sleep_for(std::chrono::seconds(reconnect_delay));
            reconnect_delay = std::min(reconnect_delay * 2, 30);
            continue;
        }

        reconnect_delay = 1;
        auto last_keepalive     = std::chrono::steady_clock::now();
        auto last_session_touch = std::chrono::steady_clock::now();

        while (true) {
            if (watch_ptr_snap->stop.load()) { lws_context_destroy(lws_ctx); goto done; }

            int ret = lws_service(lws_ctx, 1);
            if (ret < 0 || ctx.disconnected) break;

            auto now = std::chrono::steady_clock::now();

            // WS keepalive: send empty frame every 1s
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_keepalive).count() >= 1) {
                ctx.send_keepalive = true;
                if (ctx.wsi) lws_callback_on_writable(ctx.wsi);
                last_keepalive = now;
            }

            // Watchdog: no data for 30s → reconnect
            if (ctx.subscribed &&
                std::chrono::duration_cast<std::chrono::seconds>(now - ctx.last_rx).count() >= 30) {
                watch_log(watch_id, "Watchdog: no data for 30s — reconnecting", "warning");
                break;
            }

            // Session touch: reconnect every 5 min to reset ASP.NET session idle timer
            if (ctx.subscribed &&
                std::chrono::duration_cast<std::chrono::minutes>(now - last_session_touch).count() >= 5) {
                watch_log(watch_id, "Session touch: reconnecting to reset ASP.NET session timer", "info");
                break;
            }
        }
        lws_context_destroy(lws_ctx);

        if (watch_ptr_snap->stop.load()) break;
        watch_log(watch_id, "Reconnecting...", "warning");
        reconnect_delay = 1;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
done:;
}

// ── Heartbeat + market-close threads ─────────────────────────────────────────
static std::atomic<bool> g_hb_stop{false};
static std::thread g_hb_thread;
static std::thread g_mc_thread;

static void heartbeat_loop() {
    auto last_naasa_keepalive   = std::chrono::steady_clock::now();
    auto last_asp_session_touch = std::chrono::steady_clock::now();
    // HTTP keepalive on each watch's dedicated keepalive_curl every 5s.
    // Uses keepalive_curl — zero contention with place_order on order_curl.
    auto last_order_warmup      = std::chrono::steady_clock::now();
    bool warmup_done            = false;

    while (!g_hb_stop.load()) {

        // ASP.NET session keepalive: GET /MarketOrder/Order every 5 min
        if (std::chrono::duration_cast<std::chrono::minutes>(
                std::chrono::steady_clock::now() - last_asp_session_touch).count() >= 5) {
            naasa_request(std::string(NAASA_BASE) + "/MarketOrder/Order", "GET");
            last_asp_session_touch = std::chrono::steady_clock::now();
        }

        // HTTP/2 connection keepalive: GET /Home/ServerTime every 5s on each
        // watch's keepalive_curl. Keeps the TCP+TLS+HTTP/2 multiplex to
        // x.naasasecurities.com.np:443 alive without ever touching order_curl.
        if (std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - last_order_warmup).count() >= 5) {
            last_order_warmup = std::chrono::steady_clock::now();

            std::vector<std::shared_ptr<Watch>> snaps;
            {
                std::lock_guard<std::mutex> lk(g_watches_mutex);
                for (auto& [wid, w] : g_watches)
                    if (!w->stop.load() && w->keepalive_curl) snaps.push_back(w);
            }
            for (auto& w : snaps) {
                std::lock_guard<std::mutex> wl(w->keepalive_curl_mutex);
                std::string body;
                // No reset — permanent options intact, HTTP/2 connection reused.
                curl_easy_setopt(w->keepalive_curl, CURLOPT_URL,        NAASA_SERVER_TIME_URL);
                curl_easy_setopt(w->keepalive_curl, CURLOPT_HTTPGET,    1L);
                curl_easy_setopt(w->keepalive_curl, CURLOPT_HTTPHEADER, w->keepalive_hdrs);
                curl_easy_setopt(w->keepalive_curl, CURLOPT_WRITEDATA,  &body);
                curl_easy_perform(w->keepalive_curl);
            }
        }

        // ValidateSessionNo every 10 min — refreshes WS sessionNo
        if (std::chrono::duration_cast<std::chrono::minutes>(
                std::chrono::steady_clock::now() - last_naasa_keepalive).count() >= 10) {
            sys_log("Naasa: session keep-alive HTTP POST...", "sys");
            bool ok = naasa_validate_session();
            if (ok) {
                // Re-read cookie in case server issued a refreshed one; rebuild all headers.
                {
                    std::lock_guard<std::mutex> lk1(g_naasa_curl_mutex);
                    std::string cv = get_naasa_cookie(".AspNetCore.Session");
                    if (!cv.empty()) {
                        std::lock_guard<std::mutex> lk2(g_naasa_session_mutex);
                        g_naasa_asp_cookie = cv;
                    }
                }
                rebuild_all_watch_order_hdrs();
                sys_log("Naasa: session keep-alive OK", "ok");
            } else {
                sys_log("Naasa: session keep-alive failed — session expired.", "err");
                if (!g_naasa_auto_email.empty()) {
                    sys_log("Naasa: attempting auto re-login...", "sys");
                    bool relogin_ok = false;
                    try {
                        naasa_do_login(g_naasa_auto_email, g_naasa_auto_password);
                        {
                            std::lock_guard<std::mutex> lk1(g_naasa_curl_mutex);
                            std::string cv = get_naasa_cookie(".AspNetCore.Session");
                            if (!cv.empty()) {
                                std::lock_guard<std::mutex> lk2(g_naasa_session_mutex);
                                g_naasa_asp_cookie = cv;
                            }
                        }
                        rebuild_all_watch_order_hdrs();
                        relogin_ok = true;
                    } catch (const std::exception& ex) {
                        sys_log(std::string("Naasa: auto re-login failed: ") + ex.what(), "err");
                    } catch (...) {
                        sys_log("Naasa: auto re-login failed (unknown error)", "err");
                    }
                    if (relogin_ok) {
                        sys_log("Naasa: auto re-login OK — session restored", "ok");
                        g_naasa_kicked.store(false);
                    } else {
                        cprint(std::string(RD_) + B_
                               + "  !! Naasa auto re-login failed — type 'naasa' to restore manually !!"
                               + R_);
                    }
                } else {
                    cprint(std::string(RD_) + B_
                           + "  !! Naasa session expired — type 'naasa' and paste a fresh sessionNo !!"
                           + R_);
                }
            }
            last_naasa_keepalive = std::chrono::steady_clock::now();
        }

        // Pre-market warmup: at 10:59:55 NST fire a GET on each watch's keepalive_curl
        {
            int nsecs = nepal_secs_since_midnight();
            if (nsecs >= 11 * 3600 + 1) warmup_done = false;

            if (!warmup_done && nsecs >= 10 * 3600 + 59 * 60 + 55 && nsecs < 11 * 3600) {
                warmup_done = true;
                std::vector<std::shared_ptr<Watch>> active;
                {
                    std::lock_guard<std::mutex> lk(g_watches_mutex);
                    for (auto& [wid, w] : g_watches)
                        if (!w->stop.load() && w->keepalive_curl) active.push_back(w);
                }
                if (!active.empty())
                    sys_log("Pre-market warmup: touching connections for "
                            + std::to_string(active.size()) + " watch(es)", "sys");
                for (auto& w : active) {
                    std::lock_guard<std::mutex> wl(w->keepalive_curl_mutex);
                    std::string body;
                    curl_easy_setopt(w->keepalive_curl, CURLOPT_URL,        NAASA_SERVER_TIME_URL);
                    curl_easy_setopt(w->keepalive_curl, CURLOPT_HTTPGET,    1L);
                    curl_easy_setopt(w->keepalive_curl, CURLOPT_HTTPHEADER, w->keepalive_hdrs);
                    curl_easy_setopt(w->keepalive_curl, CURLOPT_WRITEDATA,  &body);
                    curl_easy_perform(w->keepalive_curl);
                    sys_log("Pre-market warmup done for watch " + w->id, "ok");
                }
            }
        }

        for (int i = 0; i < 50 && !g_hb_stop.load(); i++)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// ── Market close monitor ──────────────────────────────────────────────────────
static void print_totals() {
    double total_spent  = 0.0;
    double total_budget = 0.0;
    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        for (auto& [wid, w] : g_watches) {
            std::lock_guard<std::mutex> ll(w->lat_mutex);
            total_spent  += w->spent;
            total_budget += w->maxSpend;
        }
    }
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1);
    ss << DM_ << "  ── Totals:  spent=" << BGN_ << "Rs " << total_spent << R_
       << DM_ << "  /  budget=Rs " << total_budget << R_;
    cprint(ss.str());
}

static void market_close_monitor() {
    while (!g_hb_stop.load()) {
        if (nepal_secs_since_midnight() >= 15 * 3600) {
            std::vector<std::string> active;
            {
                std::lock_guard<std::mutex> lk(g_watches_mutex);
                for (auto& [wid, w] : g_watches)
                    if (!w->stop.load()) active.push_back(wid);
            }
            if (!active.empty()) {
                sys_log("Market closed at 15:00 — stopping "
                        + std::to_string(active.size()) + " watch(es)", "sys");
                {
                    std::lock_guard<std::mutex> lk(g_watches_mutex);
                    for (auto& wid : active) {
                        auto it = g_watches.find(wid);
                        if (it != g_watches.end())
                            it->second->stop.store(true);
                    }
                }
                print_totals();
            }
            break;
        }
        for (int i = 0; i < 200 && !g_hb_stop.load(); i++)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// ── CLI helpers ───────────────────────────────────────────────────────────────
static void stop_watch(const std::string& wid) {
    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        auto it = g_watches.find(wid);
        if (it == g_watches.end()) return;
        it->second->stop.store(true);
    }
    cprint(std::string(YL_) + "  [WATCH STOPPED]" + R_ + "  " + wid);
    print_totals();
}

static void cmd_add_watch() {
    if (!g_logged_in.load()) {
        cprint(std::string(RD_) + "  Not logged in." + R_); return;
    }
    std::string symbol_str, qty_str, eq_str, spend_str;

    std::cout << "  Symbol (e.g. UPPER)       : " << std::flush;
    std::getline(std::cin, symbol_str);
    symbol_str.erase(0, symbol_str.find_first_not_of(" \t"));
    symbol_str.erase(symbol_str.find_last_not_of(" \t\r\n") + 1);
    for (auto& c : symbol_str) c = (char)std::toupper((unsigned char)c);

    std::cout << "  Qty (normal)              : " << std::flush;
    std::getline(std::cin, qty_str);

    std::cout << "  Qty (circuit) [Enter=same]: " << std::flush;
    std::getline(std::cin, eq_str);

    std::cout << "  Max spend (Rs)            : " << std::flush;
    std::getline(std::cin, spend_str);

    int    qty, ending_qty;
    double max_spend;
    try {
        qty        = std::stoi(qty_str);
        ending_qty = eq_str.empty() ? qty : std::stoi(eq_str);
        max_spend  = std::stod(spend_str);
    } catch (...) {
        cprint(std::string(RD_) + "  Invalid input." + R_); return;
    }
    if (symbol_str.empty() || qty <= 0 || ending_qty <= 0 || max_spend <= 0) {
        cprint(std::string(RD_) + "  Invalid parameters." + R_); return;
    }

    std::string uuid = generate_uuid();
    std::string wid  = uuid.substr(0, 8);

    auto watch       = std::make_shared<Watch>();
    watch->id        = wid;
    watch->symbol    = symbol_str;
    watch->qty       = qty;
    watch->endingQty = ending_qty;
    watch->maxSpend  = max_spend;

    // ── Pre-bake payload head (embeds symbol — constant for this watch) ────────
    {
        char* p = watch->payload_head;
        memcpy(p, PAYLOAD_HEAD_PREFIX, (size_t)PAYLOAD_HEAD_PREFIX_LEN); p += PAYLOAD_HEAD_PREFIX_LEN;
        memcpy(p, symbol_str.c_str(),   symbol_str.size());               p += symbol_str.size();
        memcpy(p, PAYLOAD_HEAD_SUFFIX, (size_t)PAYLOAD_HEAD_SUFFIX_LEN); p += PAYLOAD_HEAD_SUFFIX_LEN;
        watch->payload_head_len = (int)(p - watch->payload_head);
    }

    // ── Snapshot current cookie ───────────────────────────────────────────────
    std::string cv;
    {
        std::lock_guard<std::mutex> sl(g_naasa_session_mutex);
        cv = g_naasa_asp_cookie;
    }

    // ── Order curl handle ─────────────────────────────────────────────────────
    watch->order_curl = curl_easy_init();
    if (!watch->order_curl) {
        cprint(std::string(RD_) + "  curl_easy_init failed (order handle)." + R_); return;
    }
    curl_easy_setopt(watch->order_curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(watch->order_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(watch->order_curl, CURLOPT_TCP_NODELAY,    1L);
    curl_easy_setopt(watch->order_curl, CURLOPT_RESOLVE,        g_dns_pins);
    curl_easy_setopt(watch->order_curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(watch->order_curl, CURLOPT_WRITEFUNCTION,  write_cb);
    apply_conn_opts(watch->order_curl);
    watch->order_hdrs = build_order_hdrs_list(cv);

    // ── Keepalive curl handle ─────────────────────────────────────────────────
    watch->keepalive_curl = curl_easy_init();
    if (!watch->keepalive_curl) {
        cprint(std::string(RD_) + "  curl_easy_init failed (keepalive handle)." + R_); return;
    }
    curl_easy_setopt(watch->keepalive_curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(watch->keepalive_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(watch->keepalive_curl, CURLOPT_TCP_NODELAY,    1L);
    curl_easy_setopt(watch->keepalive_curl, CURLOPT_RESOLVE,        g_dns_pins);
    curl_easy_setopt(watch->keepalive_curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(watch->keepalive_curl, CURLOPT_WRITEFUNCTION,  write_cb);
    apply_conn_opts(watch->keepalive_curl);
    watch->keepalive_hdrs = build_keepalive_hdrs_list(cv);

    // ── Initial warmup: establish TCP+TLS+HTTP/2 on both handles immediately ──
    {
        std::string dummy;
        curl_easy_setopt(watch->order_curl,     CURLOPT_URL,        NAASA_SERVER_TIME_URL);
        curl_easy_setopt(watch->order_curl,     CURLOPT_HTTPGET,    1L);
        curl_easy_setopt(watch->order_curl,     CURLOPT_HTTPHEADER, watch->order_hdrs);
        curl_easy_setopt(watch->order_curl,     CURLOPT_WRITEDATA,  &dummy);
        curl_easy_perform(watch->order_curl);

        curl_easy_setopt(watch->keepalive_curl, CURLOPT_URL,        NAASA_SERVER_TIME_URL);
        curl_easy_setopt(watch->keepalive_curl, CURLOPT_HTTPGET,    1L);
        curl_easy_setopt(watch->keepalive_curl, CURLOPT_HTTPHEADER, watch->keepalive_hdrs);
        curl_easy_setopt(watch->keepalive_curl, CURLOPT_WRITEDATA,  &dummy);
        curl_easy_perform(watch->keepalive_curl);
    }

    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        g_watches[wid] = watch;
    }

    watch->worker = std::thread(run_watch_socket, wid);

#ifndef _WIN32
    // SCHED_FIFO prevents OS preemption between tick receive and order dispatch.
    // CPU affinity pins the thread to core 0 (the only vCPU on this VPS).
    // Both ops silently degrade if the process lacks CAP_SYS_NICE.
    {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(0, &cpuset);
        pthread_setaffinity_np(watch->worker.native_handle(), sizeof(cpuset), &cpuset);

        struct sched_param sp{};
        sp.sched_priority = 10;
        pthread_setschedparam(watch->worker.native_handle(), SCHED_FIFO, &sp);
    }
#endif

    std::ostringstream ss;
    ss << std::fixed << std::setprecision(0);
    ss << "\n  " << BGN_ << "[+] Watch " << B_ << wid << R_ << BGN_ << " started" << R_
       << "  symbol=" << B_ << symbol_str << R_
       << "  qty=" << qty << "  endQty=" << ending_qty
       << "  budget=Rs " << max_spend;
    cprint(ss.str());
    print_totals();
}

static void cmd_list_watches() {
    std::lock_guard<std::mutex> lk(g_watches_mutex);
    if (g_watches.empty()) { cprint(std::string(DM_) + "  No watches." + R_); return; }

    std::ostringstream hdr;
    hdr << "\n  " << B_ << std::left << std::setw(10) << "ID"
        << std::setw(10) << "Stock"
        << std::right << std::setw(5) << "Qty"
        << std::setw(6)  << "EQty"
        << std::setw(15) << "Spent"
        << std::setw(15) << "Budget"
        << "  St" << R_;
    cprint(hdr.str());
    cprint("  " + std::string(63, '-'));

    for (auto& [wid, w] : g_watches) {
        std::string active = !w->stop.load()
            ? (std::string(BGN_) + "ON" + R_)
            : (std::string(DM_)  + "off" + R_);

        double spent;
        { std::lock_guard<std::mutex> ll(w->lat_mutex); spent = w->spent; }
        std::ostringstream row;
        row << std::fixed << std::setprecision(1);
        row << "  " << BCY_ << std::left << std::setw(10) << wid << R_
            << std::setw(10) << w->symbol
            << std::right << std::setw(5) << w->qty
            << std::setw(6) << w->endingQty
            << "  Rs " << std::setw(9) << spent
            << "  Rs " << std::setw(9) << w->maxSpend
            << "  " << active;
        cprint(row.str());
    }
    cprint("");
}

static void cmd_print_summary() {
    cprint(std::string("\n") + B_ + std::string(55, '=') + R_);
    cprint(std::string(B_) + "  ORDER SUMMARY" + R_);
    cprint(std::string(B_) + std::string(55, '=') + R_);

    int    total_orders = 0;
    double total_spent  = 0.0;

    std::lock_guard<std::mutex> lk(g_watches_mutex);
    for (auto& [wid, w] : g_watches) {
        cprint("  Watch " + std::string(BCY_) + wid + R_
               + "  (" + B_ + w->symbol + R_ + ")");
        std::vector<OrderRecord> snap;
        {
            std::lock_guard<std::mutex> ll(w->lat_mutex);
            snap = w->orders;
        }
        if (snap.empty()) {
            cprint(std::string(DM_) + "    no orders placed" + R_);
        } else {
            for (auto& o : snap) {
                double line_total = o.price * o.qty;
                std::ostringstream row;
                row << std::fixed << std::setprecision(1);
                row << "    " << BGN_ << "BUY" << R_
                    << "  qty=" << o.qty
                    << "  @  Rs " << o.price
                    << "  =  Rs " << line_total;
                cprint(row.str());
                total_orders++;
                total_spent += line_total;
            }
        }
    }

    cprint("  " + std::string(45, '-'));
    cprint("  Total orders : " + std::string(B_) + std::to_string(total_orders) + R_);
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(1);
    ss << "  Total spent  : " << BGN_ << "Rs " << total_spent << R_;
    cprint(ss.str());
    cprint(std::string(B_) + std::string(55, '=') + R_ + "\n");
}

static int64_t percentile(std::vector<int64_t> v, int p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    size_t idx = (size_t)(((double)p / 100.0) * (double)(v.size() - 1) + 0.5);
    if (idx >= v.size()) idx = v.size() - 1;
    return v[idx];
}

static void cmd_print_stats() {
    cprint(std::string("\n") + B_ + "  Latency Stats" + R_);
    std::lock_guard<std::mutex> lk(g_watches_mutex);
    if (g_watches.empty()) { cprint(std::string(DM_) + "  No watches." + R_); return; }

    for (auto& [wid, w] : g_watches) {
        std::vector<int64_t> t2o, rtt;
        {
            std::lock_guard<std::mutex> ll(w->lat_mutex);
            t2o = w->tick_to_order_us;
            rtt = w->api_rtt_us;
        }
        cprint("  " + std::string(BCY_) + wid + R_
               + "  (" + w->symbol + ")  orders=" + std::to_string(t2o.size()));
        if (t2o.empty()) {
            cprint(std::string(DM_) + "    no orders placed yet" + R_);
            continue;
        }
        {
            std::ostringstream ss;
            ss << "    Tick→Order  p50=" << B_ << percentile(t2o, 50) << R_ << "µs"
               << "  p95=" << B_ << percentile(t2o, 95) << R_ << "µs"
               << "  p99=" << B_ << percentile(t2o, 99) << R_ << "µs";
            cprint(ss.str());
        }
        {
            std::ostringstream ss;
            ss << "    API RTT     p50=" << B_ << (percentile(rtt, 50) / 1000) << R_ << "ms"
               << "  p95=" << B_ << (percentile(rtt, 95) / 1000) << R_ << "ms"
               << "  p99=" << B_ << (percentile(rtt, 99) / 1000) << R_ << "ms";
            cprint(ss.str());
        }
    }
    cprint("");
}

static void cmd_print_time() {
    int secs = nepal_secs_since_midnight();
    auto now = std::chrono::system_clock::now();
    struct tm tm_nst = to_nst(std::chrono::system_clock::to_time_t(now));
    char date_buf[32];
    strftime(date_buf, sizeof(date_buf), "%Y-%m-%d  %H:%M:%S", &tm_nst);

    std::string status, countdown;
    if (secs > (11 * 3600) && secs < (15 * 3600)) {
        int left = (15 * 3600) - secs;
        int h = left / 3600, m = (left % 3600) / 60, s = left % 60;
        std::ostringstream cs;
        cs << "  closes in " << B_ << std::setfill('0')
           << std::setw(2) << h << ":" << std::setw(2) << m << ":" << std::setw(2) << s << R_;
        status    = std::string(BGN_) + "OPEN" + R_;
        countdown = cs.str();
    } else if (secs <= (11 * 3600)) {
        int left = (11 * 3600) - secs;
        int h = left / 3600, m = (left % 3600) / 60, s = left % 60;
        std::ostringstream cs;
        cs << "  opens in  " << B_ << std::setfill('0')
           << std::setw(2) << h << ":" << std::setw(2) << m << ":" << std::setw(2) << s << R_;
        status    = std::string(YL_) + "CLOSED" + R_;
        countdown = cs.str();
    } else {
        status    = std::string(RD_) + "CLOSED" + R_;
        countdown = std::string("  ") + DM_ + "market closed for today" + R_;
    }

    cprint("\n  " + std::string(B_) + "Nepal Time" + R_ + "   "
           + BCY_ + date_buf + R_ + "  (NST UTC+5:45)");
    cprint("  " + std::string(B_) + "Market" + R_ + "       " + status + countdown + "\n");
}

// ── fire command ──────────────────────────────────────────────────────────────
static long fire_shot(CURL* fire_curl, const char* payload, size_t payload_len,
                      curl_slist* hdrs, std::string& out_body, double& rtt_ms) {
    out_body.clear();
    curl_easy_setopt(fire_curl, CURLOPT_POSTFIELDS,    payload);
    curl_easy_setopt(fire_curl, CURLOPT_POSTFIELDSIZE, (long)payload_len);
    curl_easy_setopt(fire_curl, CURLOPT_WRITEDATA,     &out_body);
    curl_easy_setopt(fire_curl, CURLOPT_HTTPHEADER,    hdrs);
    auto t0 = std::chrono::steady_clock::now();
    curl_easy_perform(fire_curl);
    rtt_ms = std::chrono::duration<double, std::milli>(
                 std::chrono::steady_clock::now() - t0).count();
    long code = 0;
    curl_easy_getinfo(fire_curl, CURLINFO_RESPONSE_CODE, &code);
    return code;
}

static void cmd_fire(const std::string& sym) {
    std::shared_ptr<Watch> watch;
    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        for (auto& [id, w] : g_watches) {
            if (w->symbol == sym && !w->stop.load()) { watch = w; break; }
        }
    }
    if (!watch) {
        cprint(std::string(RD_) + "  No active watch for '" + sym + "'. Add a watch first." + R_);
        return;
    }

    double open = watch->open_price.load(std::memory_order_relaxed);
    if (open <= 0.0) {
        cprint(std::string(YL_) + "  No WS tick yet — entering manual price mode." + R_);
        std::cout << "  Enter open / prev-close price: " << std::flush;
        std::string open_str;
        std::getline(std::cin, open_str);
        open_str.erase(0, open_str.find_first_not_of(" \t"));
        open_str.erase(open_str.find_last_not_of(" \t\r\n") + 1);
        try { open = std::stod(open_str); } catch (...) {}
        if (open <= 0.0) {
            cprint(std::string(RD_) + "  Invalid open price — aborted." + R_); return;
        }
    }

    double circuit = std::floor(open * 1.10 * 10.0) / 10.0;
    {
        std::ostringstream inf;
        inf << std::fixed << std::setprecision(1);
        inf << "  Open: " << open << "  →  Circuit ceiling: " << BCY_ << circuit << R_;
        cprint(inf.str());
    }

    std::cout << "  Enter future LTP (price you will sell at): " << std::flush;
    std::string ltp_str;
    std::getline(std::cin, ltp_str);
    ltp_str.erase(0, ltp_str.find_first_not_of(" \t"));
    ltp_str.erase(ltp_str.find_last_not_of(" \t\r\n") + 1);
    double future_ltp = 0.0;
    try { future_ltp = std::stod(ltp_str); } catch (...) {}
    if (future_ltp <= 0.0) {
        cprint(std::string(RD_) + "  Invalid LTP — aborted." + R_); return;
    }

    double bid = std::min(std::floor(future_ltp * 1.02 * 10.0) / 10.0, circuit);
    int    qty = watch->endingQty;

    {
        std::ostringstream ss;
        ss << std::fixed << std::setprecision(1);
        ss << "\n  " << B_ << "FIRE PREVIEW" << R_
           << "  " << BCY_ << sym << R_
           << "  open=" << open
           << "  future_ltp=" << future_ltp
           << "  circuit=" << circuit
           << "  bid=" << B_ << bid << R_
           << "  qty=" << qty;
        cprint(ss.str());
    }

    std::cout << "  " << YL_ << "Press ENTER to start firing, or Ctrl+C to abort..." << R_ << std::flush;
    { std::string dummy; std::getline(std::cin, dummy); }

    // Build payload once — reused for all shots
    char fire_buf[512];
    const size_t fire_payload_len = build_order_payload(*watch, qty, bid,
                                                         fire_buf, sizeof(fire_buf));

    for (int i = 3; i >= 1; --i) {
        cprint(std::string("  ") + YL_ + "Firing in " + std::to_string(i) + "s...  ← switch to Naasa tab now" + R_);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    cprint(std::string("  ") + RD_ + B_ + "★ FIRE!  ← CLICK SELL NOW" + R_);

    const int MAX_SHOTS = 7;
    std::string shot_body;
    shot_body.reserve(256);

    for (int shot = 1; shot <= MAX_SHOTS; ++shot) {
        double rtt_ms = 0.0;
        long   status;
        {
            std::lock_guard<std::mutex> wl(watch->order_curl_mutex);
            curl_easy_setopt(watch->order_curl, CURLOPT_URL,  NAASA_ORDER_URL);
            curl_easy_setopt(watch->order_curl, CURLOPT_POST, 1L);
            status = fire_shot(watch->order_curl, fire_buf, fire_payload_len,
                               watch->order_hdrs, shot_body, rtt_ms);
        }

        // Check success
        bool ok = false;
        if (status == 200) {
            const char* p = strstr(shot_body.c_str(), "\"ErrorCode\":");
            if (p) {
                p += 12;
                while (*p == ' ') ++p;
                ok = (*p == '0') && (p[1] == ',' || p[1] == '}' || p[1] == ' ');
            }
        }

        {
            std::ostringstream ls;
            ls << std::fixed << std::setprecision(1);
            ls << "  Shot " << shot << "/" << MAX_SHOTS << "  HTTP " << status
               << "  RTT=" << rtt_ms << "ms";
            if (ok) ls << "  " << BGN_ << "SUCCESS" << R_;
            cprint(ls.str());
        }
        if (!shot_body.empty()) cprint(std::string("  JSON: ") + shot_body);

        if (ok) {
            double spent;
            {
                std::lock_guard<std::mutex> ll(watch->lat_mutex);
                watch->orders.push_back({bid, qty});
                watch->spent += bid * (double)qty;
                spent = watch->spent;
            }
            // Atomic stores so WS callback sees consistent state
            watch->first_order_done.store(true,       std::memory_order_release);
            watch->last_ltp.store(future_ltp,          std::memory_order_release);
            cprint(std::string("  ") + BGN_ + "Order accepted — fire sequence stopped." + R_);
            print_totals();
            if (spent >= watch->maxSpend) {
                cprint(std::string(YL_) + "  [BUDGET HIT]" + R_
                       + "  watch=" + watch->id + " — stopping");
                watch->stop.store(true);
            }
            return;
        }
    }
    cprint(std::string("  ") + YL_ + "Fire sequence done — no success received." + R_);
}

static void cmd_print_help() {
    cprint(std::string("\n  ") + B_ + "Commands:" + R_ + "\n"
        + "    " + CY_ + "add" + R_               + "            Add a new stock watch\n"
        + "    " + CY_ + "list" + R_              + "           List all watches\n"
        + "    " + CY_ + "time" + R_              + "           Show Nepal time and market status\n"
        + "    " + CY_ + "stop <id>" + R_         + "      Stop a specific watch by ID\n"
        + "    " + CY_ + "stopall" + R_           + "        Stop all active watches\n"
        + "    " + CY_ + "summary" + R_           + "        Print full order summary\n"
        + "    " + CY_ + "stats" + R_             + "          Show latency stats (p50/p95/p99)\n"
        + "    " + CY_ + "help" + R_              + "           Show this help\n"
        + "    " + CY_ + "naasa" + R_             + "          Restore Naasa session after web-login kick\n"
        + "    " + CY_ + "fire <sym>" + R_        + "     Blind-fire orders at bid ceiling (no WS tick needed)\n"
        + "    " + CY_ + "quit" + R_ + " / " + CY_ + "exit" + R_ + "    Shutdown and print summary\n");
}

// ── Password input (no echo) ──────────────────────────────────────────────────
static std::string read_password(const std::string& prompt) {
    std::cout << prompt << std::flush;
    std::string pw;
#ifdef _WIN32
    HANDLE hStdin = GetStdHandle(STD_INPUT_HANDLE);
    DWORD  mode   = 0;
    GetConsoleMode(hStdin, &mode);
    SetConsoleMode(hStdin, mode & ~ENABLE_ECHO_INPUT);
    std::getline(std::cin, pw);
    SetConsoleMode(hStdin, mode);
#else
    struct termios oldt, newt;
    tcgetattr(STDIN_FILENO, &oldt);
    newt = oldt;
    newt.c_lflag &= ~(tcflag_t)ECHO;
    tcsetattr(STDIN_FILENO, TCSANOW, &newt);
    std::getline(std::cin, pw);
    tcsetattr(STDIN_FILENO, TCSANOW, &oldt);
#endif
    std::cout << "\n";
    return pw;
}

// ── Shutdown ──────────────────────────────────────────────────────────────────
static void shutdown_all() {
    cprint(std::string("\n") + YL_ + "  Shutting down..." + R_);
    g_hb_stop.store(true);

    std::vector<std::shared_ptr<Watch>> snaps;
    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        for (auto& [wid, w] : g_watches) {
            w->stop.store(true);
            snaps.push_back(w);
        }
    }
    for (auto& w : snaps)
        if (w->worker.joinable()) w->worker.join();

    if (g_hb_thread.joinable()) g_hb_thread.join();
    if (g_mc_thread.joinable()) g_mc_thread.join();

    cmd_print_summary();
}

static void sig_handler(int) {
    g_shutdown.store(true);
#ifndef _WIN32
    ::close(STDIN_FILENO);
#endif
}

// ── Banner ────────────────────────────────────────────────────────────────────
static void print_banner() {
    cprint(std::string("\n") + B_ + BCY_
        + "  ╔══════════════════════════════════════════════════╗\n"
          "  ║      NEPSE Auto Trader  ─  CLI Mode              ║\n"
          "  ║      Feed + Orders: Naasa Securities             ║\n"
          "  ║      x.naasasecurities.com.np                   ║\n"
          "  ╚══════════════════════════════════════════════════╝" + R_);
}

// ── Login flow ────────────────────────────────────────────────────────────────
static bool do_login_flow() {
    cprint(std::string("\n") + B_ + "  Naasa Securities login (feed + orders)" + R_);
    cprint(std::string(DM_)
           + "  [a] Auto   — enter email/password, this machine fetches session\n"
           + "  [m] Manual — paste session values obtained from naasa_session.py on another machine"
           + R_);
    std::cout << "  Mode [a/m] : " << std::flush;
    std::string naasa_mode_line;
    std::getline(std::cin, naasa_mode_line);
    bool naasa_manual = (!naasa_mode_line.empty() &&
                         (naasa_mode_line[0] == 'm' || naasa_mode_line[0] == 'M'));

    if (naasa_manual) {
        cprint(std::string(DM_)
               + "  Run  python naasa_session.py  on your Windows machine and paste the values below."
               + R_);
        std::string asp_cookie, session_no;
        std::cout << "  .AspNetCore.Session : " << std::flush;
        std::getline(std::cin, asp_cookie);
        {
            auto s = asp_cookie.find_first_not_of(" \t");
            auto e = asp_cookie.find_last_not_of(" \t\r\n");
            asp_cookie = (s == std::string::npos) ? "" : asp_cookie.substr(s, e - s + 1);
        }
        std::cout << "  sessionNo           : " << std::flush;
        std::getline(std::cin, session_no);
        {
            auto s = session_no.find_first_not_of(" \t");
            auto e = session_no.find_last_not_of(" \t\r\n");
            session_no = (s == std::string::npos) ? "" : session_no.substr(s, e - s + 1);
        }
        if (asp_cookie.empty() || session_no.empty()) {
            cprint(std::string(RD_) + "  Naasa manual login failed — empty values." + R_);
            return false;
        }

        {
            std::lock_guard<std::mutex> lk(g_naasa_curl_mutex);
            std::remove(NAASA_COOKIE_FILE);
            curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEFILE, NAASA_COOKIE_FILE);
            curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEJAR,  NAASA_COOKIE_FILE);
            std::string cookie_line =
                "x.naasasecurities.com.np\tFALSE\t/\tFALSE\t0\t.AspNetCore.Session\t" + asp_cookie;
            curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIELIST, cookie_line.c_str());
            curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIELIST, "FLUSH");
        }
        {
            std::lock_guard<std::mutex> lk(g_naasa_session_mutex);
            auto sp = session_no.find(' ');
            g_naasa_user_id    = (sp != std::string::npos) ? session_no.substr(0, sp) : session_no;
            g_naasa_session_no = session_no;
            g_naasa_asp_cookie = asp_cookie;
        }

        sys_log("Naasa: activating session via ValidateSessionNo...", "sys");
        if (!naasa_validate_session()) {
            cprint(std::string(RD_) + B_
                   + "  WARNING: ValidateSessionNo failed — session not valid from this IP."
                   + R_);
            cprint(std::string(YL_)
                   + "  The session cookie is likely IP-bound to the machine that ran naasa_session.py.\n"
                   + "  Orders will fail from this VPS (different IP).\n"
                   + "  Use [a] Auto login so this machine creates its own session."
                   + R_);
        } else {
            std::lock_guard<std::mutex> lk1(g_naasa_curl_mutex);
            std::string cv = get_naasa_cookie(".AspNetCore.Session");
            if (!cv.empty()) {
                std::lock_guard<std::mutex> lk2(g_naasa_session_mutex);
                g_naasa_asp_cookie = cv;
            }
        }
        cprint(std::string("  ") + BGN_ + "Naasa manual session OK" + R_);

    } else {
        std::string naasa_email;
        std::cout << "  Naasa email    : " << std::flush;
        std::getline(std::cin, naasa_email);
        {
            auto s = naasa_email.find_first_not_of(" \t");
            auto e = naasa_email.find_last_not_of(" \t\r\n");
            naasa_email = (s == std::string::npos) ? "" : naasa_email.substr(s, e - s + 1);
        }
        std::string naasa_pw = read_password("  Naasa password : ");

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                naasa_do_login(naasa_email, naasa_pw);
                g_naasa_auto_email    = naasa_email;
                g_naasa_auto_password = naasa_pw;
                {
                    std::lock_guard<std::mutex> lk1(g_naasa_curl_mutex);
                    std::string cv = get_naasa_cookie(".AspNetCore.Session");
                    std::lock_guard<std::mutex> lk2(g_naasa_session_mutex);
                    g_naasa_asp_cookie = cv;
                }
                break;
            } catch (const std::exception& e) {
                cprint("  " + std::string(RD_) + "Naasa attempt " + std::to_string(attempt)
                       + " failed: " + e.what() + R_);
                if (attempt == 3) {
                    cprint(std::string(RD_) + "  Naasa login failed after 3 attempts." + R_);
                    return false;
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
        {
            std::lock_guard<std::mutex> lk(g_naasa_session_mutex);
            cprint(std::string("  ") + BGN_ + "Naasa login OK" + R_
                   + "  UserId=" + B_ + g_naasa_user_id + R_);
        }
    }

    g_logged_in.store(true);
    g_hb_thread = std::thread(heartbeat_loop);
    g_mc_thread = std::thread(market_close_monitor);
    return true;
}

// ── main ──────────────────────────────────────────────────────────────────────
int main() {
    signal(SIGINT,  sig_handler);
    signal(SIGTERM, sig_handler);
#ifndef _WIN32
    signal(SIGPIPE, SIG_IGN);
#endif

    curl_global_init(CURL_GLOBAL_ALL);

    // DNS pin: bypass runtime DNS resolution on the order hot path
    g_dns_pins = curl_slist_append(g_dns_pins, "x.naasasecurities.com.np:443:45.115.219.5");

    g_naasa_curl = curl_easy_init();
    if (!g_naasa_curl) { fprintf(stderr, "curl_easy_init failed (Naasa)\n"); return 1; }
    curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEFILE,  "");
    curl_easy_setopt(g_naasa_curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_RESOLVE,     g_dns_pins);

    // Static order headers — Cookie is NOT here; it lives in each Watch's order_hdrs
    g_naasa_order_hdrs = curl_slist_append(g_naasa_order_hdrs,
        "Accept: application/json, text/javascript, */*; q=0.01");
    g_naasa_order_hdrs = curl_slist_append(g_naasa_order_hdrs,
        "Content-Type: application/json; charset=UTF-8");
    g_naasa_order_hdrs = curl_slist_append(g_naasa_order_hdrs,
        "X-Requested-With: XMLHttpRequest");
    g_naasa_order_hdrs = curl_slist_append(g_naasa_order_hdrs,
        (std::string("Origin: ") + NAASA_BASE).c_str());
    g_naasa_order_hdrs = curl_slist_append(g_naasa_order_hdrs,
        (std::string("Referer: ") + NAASA_BASE + "/MarketOrder/Order").c_str());
    g_naasa_order_hdrs = curl_slist_append(g_naasa_order_hdrs,
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36");

    lws_set_log_level(0, nullptr);
    print_banner();

    while (!g_shutdown.load()) {
        if (do_login_flow()) break;
        std::cout << "  Retry? [y/N]: " << std::flush;
        std::string ans;
        if (!std::getline(std::cin, ans) || g_shutdown.load()) break;
        if (ans != "y" && ans != "Y") break;
    }

    if (!g_logged_in.load()) {
        curl_slist_free_all(g_naasa_order_hdrs);
        curl_slist_free_all(g_dns_pins);
        curl_easy_cleanup(g_naasa_curl);
        curl_global_cleanup();
        return 0;
    }

    cmd_print_help();

    while (!g_shutdown.load()) {
        std::cout << BCY_ << ">" << R_ << " " << std::flush;
        std::string cmd;
        if (!std::getline(std::cin, cmd) || g_shutdown.load()) break;

        cmd.erase(0, cmd.find_first_not_of(" \t"));
        if (cmd.empty()) continue;
        cmd.erase(cmd.find_last_not_of(" \t\r\n") + 1);
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::tolower);

        if      (cmd == "add")     cmd_add_watch();
        else if (cmd == "list")    cmd_list_watches();
        else if (cmd == "time")    cmd_print_time();
        else if (cmd == "stats")   cmd_print_stats();
        else if (cmd == "summary") cmd_print_summary();
        else if (cmd == "help")    cmd_print_help();
        else if (cmd == "stopall") {
            std::vector<std::string> ids;
            {
                std::lock_guard<std::mutex> lk(g_watches_mutex);
                for (auto& [k, _] : g_watches) ids.push_back(k);
            }
            for (auto& wid : ids) stop_watch(wid);
        }
        else if (cmd.size() > 5 && cmd.substr(0, 5) == "stop ") {
            std::string wid = cmd.substr(5);
            wid.erase(0, wid.find_first_not_of(" \t"));
            wid.erase(wid.find_last_not_of(" \t\r\n") + 1);
            bool found;
            {
                std::lock_guard<std::mutex> lk(g_watches_mutex);
                found = g_watches.count(wid) > 0;
            }
            if (found) stop_watch(wid);
            else cprint(std::string(RD_) + "  Watch '" + wid + "' not found." + R_);
        }
        else if (cmd == "naasa") {
            std::cout << "  New Naasa sessionNo (UserId<space>token): " << std::flush;
            std::string new_sno;
            std::getline(std::cin, new_sno);
            auto s = new_sno.find_first_not_of(" \t");
            auto e = new_sno.find_last_not_of(" \t\r\n");
            new_sno = (s == std::string::npos) ? "" : new_sno.substr(s, e - s + 1);
            if (new_sno.empty()) {
                cprint(std::string(RD_) + "  Empty input — session not updated." + R_);
            } else {
                {
                    std::lock_guard<std::mutex> lk(g_naasa_session_mutex);
                    auto sp = new_sno.find(' ');
                    g_naasa_user_id    = (sp != std::string::npos) ? new_sno.substr(0, sp) : new_sno;
                    g_naasa_session_no = new_sno;
                }
                g_naasa_kicked.store(false);
                cprint(std::string("  ") + BGN_ + "Naasa session updated — watches reconnecting." + R_);
            }
        }
        else if (cmd.size() > 5 && cmd.substr(0, 5) == "fire ") {
            std::string sym = cmd.substr(5);
            sym.erase(0, sym.find_first_not_of(" \t"));
            sym.erase(sym.find_last_not_of(" \t\r\n") + 1);
            for (auto& c : sym) c = (char)std::toupper((unsigned char)c);
            cmd_fire(sym);
        }
        else if (cmd == "quit" || cmd == "exit" || cmd == "q") {
            break;
        }
        else cprint(std::string(RD_) + "  Unknown command '" + cmd + "'. Type 'help'." + R_);
    }

    shutdown_all();

    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        g_watches.clear();
    }

    curl_slist_free_all(g_naasa_order_hdrs);
    curl_slist_free_all(g_dns_pins);
    curl_easy_cleanup(g_naasa_curl);
    curl_global_cleanup();
    return 0;
}

// =============================================================================
// NEPSE Auto Trader — Naasa Securities feed + orders
// Feed:   wss://x.naasasecurities.com.np:8006/WebSocket/Connectasdasds
// Orders: https://x.naasasecurities.com.np/MarketOrder/Order
//
// ── Linux build ───────────────────────────────────────────────────────────────
//   g++ -std=c++17 -O2 -o trader main_updated.cpp \
//       $(pkg-config --cflags --libs libcurl libwebsockets) \
//       -lz -lpthread
//
// ── Windows build (MSVC — Developer Command Prompt) ──────────────────────────
//   cl /std:c++17 /O2 /EHsc main_updated.cpp ^
//      /I"C:\vcpkg\installed\x64-windows\include" ^
//      /link /LIBPATH:"C:\vcpkg\installed\x64-windows\lib" ^
//      libcurl.lib websockets.lib zlib.lib
//
// ── Linux dependencies ────────────────────────────────────────────────────────
//   apt-get install build-essential g++ pkg-config \
//     libcurl4-openssl-dev libwebsockets-dev \
//     nlohmann-json3-dev zlib1g-dev
//
// ── Windows dependencies (vcpkg) ─────────────────────────────────────────────
//   vcpkg install curl libwebsockets nlohmann-json zlib
// =============================================================================

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
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
#  include <process.h>
#else
#  include <termios.h>
#  include <unistd.h>
#  include <pthread.h>
#  include <sched.h>
#endif

#include <curl/curl.h>
#include <libwebsockets.h>
#include <nlohmann/json.hpp>
#include <zlib.h>

using json = nlohmann::json;

// ── ANSI palette ──────────────────────────────────────────────────────────────
static const char* R_   = "\033[0m";
static const char* B_   = "\033[1m";
static const char* DM_  = "\033[2m";
static const char* RD_  = "\033[31m";
static const char* YL_  = "\033[33m";
static const char* CY_  = "\033[36m";
static const char* BGN_ = "\033[92m";
static const char* BCY_ = "\033[96m";

// ── Naasa order endpoint ──────────────────────────────────────────────────────
static const std::string NAASA_ORDER_URL      = "https://x.naasasecurities.com.np/MarketOrder/Order";
static const std::string NAASA_SERVER_TIME_URL = "https://x.naasasecurities.com.np/Home/ServerTime";

// Pre-built Naasa order headers — static, no rotating tokens (unlike TMS).
// Built once in main() and copied into each watch's order_hdrs at add time.
static curl_slist* g_naasa_order_hdrs = nullptr;

// ── Config ────────────────────────────────────────────────────────────────────
static const int   NST_OFFSET_SECS = 20700; // UTC+5:45

// ── Naasa Securities feed + orders ───────────────────────────────────────────
static const char* NAASA_BASE         = "https://x.naasasecurities.com.np";
static const char* NAASA_WS_HOST      = "x.naasasecurities.com.np";
static const int   NAASA_WS_PORT      = 8006;
static const char* NAASA_VALIDATE_URL = "https://x.naasasecurities.com.np/Login/ValidateSessionNo";
static const char* NAASA_COOKIE_FILE  = "/tmp/.naasa_ck"; // persists cookies across naasa_request calls

// ── Naasa session state ────────────────────────────────────────────────────────
static CURL*       g_naasa_curl = nullptr;
static std::mutex  g_naasa_curl_mutex;
static std::string       g_naasa_user_id;
static std::string       g_naasa_session_no;  // full WS password ("UserId    token")
static std::mutex        g_naasa_session_mutex;
static std::atomic<bool> g_naasa_kicked{false}; // set when server sends 104 "User logged in from another source"
// Raw .AspNetCore.Session cookie value — stored in memory (not via file) so it is always
// injected directly into order requests, mirroring what main.py's requests.Session does.
static std::string g_naasa_asp_cookie;          // protected by g_naasa_session_mutex
// Optional SOCKS5/HTTP proxy for Naasa auth requests (empty = direct connection).
static std::string g_naasa_proxy;

// ── ATRAD / Sweta Securities (order system) ───────────────────────────────────
static const char* ATRAD_BASE          = "https://tms.swetasecurities.com/atsweb";
static const char* ATRAD_ORDER_URL     = "https://tms.swetasecurities.com/atsweb/order";

static CURL*       g_atrad_curl        = nullptr;
static std::mutex  g_atrad_mutex;
// Full cookie string injected into order requests: "JSESSIONID=xxx; username=yyy; ..."
// Set once at login, read-only afterwards (no lock needed for order fast path).
static std::string g_atrad_cookie_str;
static std::string g_atrad_username;   // login ID (for heartbeat checkUserSession)
static std::string g_atrad_acntid;     // clientacntid from getBOIDUserDetails
static std::string g_atrad_client_acc; // "clientCode ( lastName-nic)"
static std::string g_atrad_broker;     // brokerId (e.g. "SSPL")

// Stored only when auto-login mode is used — enables heartbeat to re-login
// automatically when the Naasa session expires.
static std::string g_naasa_auto_email;
static std::string g_naasa_auto_password; // plain text

// ── Login / DNS ───────────────────────────────────────────────────────────────
static std::atomic<bool> g_logged_in{false};
static curl_slist*       g_dns_pins = nullptr;

// Extract cookie value from g_naasa_curl's jar. Caller must hold g_naasa_curl_mutex.
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

// Build Cookie header string from g_naasa_curl's jar. Caller must hold g_naasa_curl_mutex.
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

struct HttpResponse {
    long        status       = 0;
    std::string body;
    CURLcode    curl_code    = CURLE_OK;
    std::string redirect_url;
};

static size_t write_cb(char* ptr, size_t size, size_t nmemb, void* ud);
static void   apply_conn_opts(CURL* c); // forward decl — defined after curl helpers

// Naasa HTTP request — unified helper.
// method: "GET" | "POST_FORM" | "POST_JSON_EMPTY"
static HttpResponse naasa_request(const std::string& url,
                                   const char* method,
                                   const std::string& post_body = "") {
    // Snapshot the in-memory session cookie before acquiring curl mutex.
    // In manual login mode the cookie file may not be populated yet (FLUSH requires
    // a prior transfer), so we inject it directly via CURLOPT_COOKIE as well.
    std::string asp_cookie_snap;
    {
        std::lock_guard<std::mutex> sl(g_naasa_session_mutex);
        asp_cookie_snap = g_naasa_asp_cookie;
    }

    std::lock_guard<std::mutex> lk(g_naasa_curl_mutex);
    std::string body;
    body.reserve(65536);
    curl_easy_reset(g_naasa_curl);
    curl_easy_setopt(g_naasa_curl, CURLOPT_URL,              url.c_str());
    curl_easy_setopt(g_naasa_curl, CURLOPT_SSL_VERIFYPEER,   0L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_SSL_VERIFYHOST,   0L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEFILE,       NAASA_COOKIE_FILE);
    curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEJAR,        NAASA_COOKIE_FILE);
    // Inject in-memory cookie directly so it is always sent even when the cookie
    // file has not been flushed yet (manual login mode before first transfer).
    std::string naasa_cookie_hdr;
    if (!asp_cookie_snap.empty()) {
        naasa_cookie_hdr = ".AspNetCore.Session=" + asp_cookie_snap;
        curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIE, naasa_cookie_hdr.c_str());
    }
    curl_easy_setopt(g_naasa_curl, CURLOPT_TCP_NODELAY,      1L);
    apply_conn_opts(g_naasa_curl);
    curl_easy_setopt(g_naasa_curl, CURLOPT_CONNECTTIMEOUT,  30L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_TIMEOUT,         60L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_ACCEPT_ENCODING,  "");
    if (!g_naasa_proxy.empty())
        curl_easy_setopt(g_naasa_curl, CURLOPT_PROXY, g_naasa_proxy.c_str());
    curl_easy_setopt(g_naasa_curl, CURLOPT_WRITEFUNCTION,    write_cb);
    curl_easy_setopt(g_naasa_curl, CURLOPT_WRITEDATA,        &body);

    curl_slist* hdrs = nullptr;
    const char* ua = "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                     "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";

    if (std::string_view(method) == "GET") {
        curl_easy_setopt(g_naasa_curl, CURLOPT_FOLLOWLOCATION, 1L);
        hdrs = curl_slist_append(hdrs, ua);
        hdrs = curl_slist_append(hdrs, "Accept-Language: en-US,en;q=0.9");
    } else if (std::string_view(method) == "POST_FORM") {
        curl_easy_setopt(g_naasa_curl, CURLOPT_FOLLOWLOCATION,  1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POST,            1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDS,      post_body.c_str());
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDSIZE,   (long)post_body.size());
        hdrs = curl_slist_append(hdrs, "Content-Type: application/x-www-form-urlencoded");
        hdrs = curl_slist_append(hdrs, ua);
    } else { // POST_JSON_EMPTY
        curl_easy_setopt(g_naasa_curl, CURLOPT_FOLLOWLOCATION, 1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POST,          1L);
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDS,    "");
        curl_easy_setopt(g_naasa_curl, CURLOPT_POSTFIELDSIZE, 0L);
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

// Parse {"sessionNo":"UserId    token"} response and update globals. Returns true on success.
static bool parse_and_store_session_no(const std::string& body) {
    try {
        json resp = json::parse(body);
        std::string sno = resp.at("sessionNo").get<std::string>();
        auto sp  = sno.find(' ');
        std::string uid = (sp != std::string::npos) ? sno.substr(0, sp) : sno;
        std::lock_guard<std::mutex> lk(g_naasa_session_mutex);
        g_naasa_user_id    = uid;
        g_naasa_session_no = sno;
        return true;
    } catch (...) {
        return false;
    }
}

// Extract form action URL containing "authenticate" from Keycloak login HTML.
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

static std::string url_encode(const std::string& s); // defined below
static void sys_log(const std::string& msg, const std::string& level); // defined below

// Naasa Keycloak login. Throws on failure.
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

// Refresh Naasa WS session number. Returns true on success.
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

// ── ATRAD helpers ─────────────────────────────────────────────────────────────
// ATRAD responses use Python-dict single-quote notation in nested data objects.
// Replace ' with " to produce valid JSON before parsing.
static std::string fix_atrad_json(std::string s) {
    for (char& c : s) if (c == '\'') c = '"';
    return s;
}

// 10-character random alphanumeric duplicateOrderId (client-side idempotency key).
static std::string gen_atrad_dup_id() {
    static const char CHARS[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<int> dist(0, 61);
    std::string s(10, ' ');
    for (char& c : s) c = CHARS[dist(gen)];
    return s;
}

// Build the full Cookie header value from g_atrad_curl's in-memory jar.
// Caller must hold g_atrad_mutex.
static std::string build_atrad_cookie_from_jar() {
    struct curl_slist* list = nullptr;
    curl_easy_getinfo(g_atrad_curl, CURLINFO_COOKIELIST, &list);
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

// Raw ATRAD HTTP request. Caller must hold g_atrad_mutex.
// method: "GET" | "POST_FORM"
static HttpResponse atrad_do_request(const std::string& url,
                                      const char* method,
                                      const std::string& post_body = "") {
    std::string body;
    body.reserve(65536);
    curl_easy_reset(g_atrad_curl);
    curl_easy_setopt(g_atrad_curl, CURLOPT_URL,             url.c_str());
    curl_easy_setopt(g_atrad_curl, CURLOPT_SSL_VERIFYPEER,  0L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_SSL_VERIFYHOST,  0L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_COOKIEFILE,      "");  // enable cookie engine
    curl_easy_setopt(g_atrad_curl, CURLOPT_FOLLOWLOCATION,  1L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_TCP_NODELAY,     1L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_CONNECTTIMEOUT,  30L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_TIMEOUT,         60L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_ACCEPT_ENCODING, "");
    curl_easy_setopt(g_atrad_curl, CURLOPT_WRITEFUNCTION,   write_cb);
    curl_easy_setopt(g_atrad_curl, CURLOPT_WRITEDATA,       &body);

    static const char* UA =
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";

    curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs, UA);
    hdrs = curl_slist_append(hdrs, "x-requested-with: XMLHttpRequest");
    hdrs = curl_slist_append(hdrs, "Accept-Language: en-US,en;q=0.9");

    if (std::string_view(method) == "POST_FORM") {
        curl_easy_setopt(g_atrad_curl, CURLOPT_POST,          1L);
        curl_easy_setopt(g_atrad_curl, CURLOPT_POSTFIELDS,    post_body.c_str());
        curl_easy_setopt(g_atrad_curl, CURLOPT_POSTFIELDSIZE, (long)post_body.size());
        hdrs = curl_slist_append(hdrs,
            "Content-Type: application/x-www-form-urlencoded");
        hdrs = curl_slist_append(hdrs,
            (std::string("Origin: ") + ATRAD_BASE).c_str());
    }

    curl_easy_setopt(g_atrad_curl, CURLOPT_HTTPHEADER, hdrs);
    CURLcode res = curl_easy_perform(g_atrad_curl);
    long code = 0;
    curl_easy_getinfo(g_atrad_curl, CURLINFO_RESPONSE_CODE, &code);
    curl_slist_free_all(hdrs);
    return {code, body, res, ""};
}

// ATRAD full login — 5-step flow.
// 1. GET /atsweb  → JSESSIONID cookie set via redirect
// 2. GET getLoginPageParams
// 3. POST login   → verify code == "0"
// 4. GET getBOIDUserDetails → populate acntid, clientAcc, broker
// 5. Build full cookie string for order injection
// Throws std::runtime_error on failure.
static void atrad_login(const std::string& username, const std::string& password) {
    std::lock_guard<std::mutex> lk(g_atrad_mutex);

    // Step 1: GET /atsweb — redirects set JSESSIONID in cookie jar
    sys_log("ATRAD: connecting...", "sys");
    {
        auto r = atrad_do_request(std::string(ATRAD_BASE), "GET");
        if (r.curl_code != CURLE_OK)
            throw std::runtime_error(
                std::string("ATRAD: initial connect failed: ") + curl_easy_strerror(r.curl_code));
    }

    // Step 2: getLoginPageParams
    {
        long ts = (long)(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        atrad_do_request(std::string(ATRAD_BASE) + "/login?action=getLoginPageParams"
                         "&format=json&dojo.preventCache=" + std::to_string(ts), "GET");
    }

    // Step 3: POST login
    sys_log("ATRAD: logging in...", "sys");
    {
        std::string form = "action=login&format=json"
                           "&txtUserName=" + url_encode(username) +
                           "&txtPassword=" + url_encode(password);
        auto r = atrad_do_request(std::string(ATRAD_BASE) + "/login", "POST_FORM", form);
        // Response may have single-quote JSON or plain JSON
        auto body = fix_atrad_json(r.body);
        bool ok = false;
        try {
            json j = json::parse(body);
            ok = (j.value("code", std::string("x")) == "0");
        } catch (...) {
            ok = (body.find("\"code\":\"0\"") != std::string::npos);
        }
        if (!ok)
            throw std::runtime_error("ATRAD: login failed — response: "
                                     + r.body.substr(0, 300));
    }

    // Step 4: getBOIDUserDetails
    sys_log("ATRAD: fetching account details...", "sys");
    {
        long ts = (long)(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
        auto r = atrad_do_request(std::string(ATRAD_BASE)
                                  + "/order?action=getBOIDUserDetails&format=json"
                                    "&dojo.preventCache=" + std::to_string(ts), "GET");
        auto body = fix_atrad_json(r.body);
        try {
            json j    = json::parse(body);
            auto& u   = j.at("data").at("userids").at(0);
            std::string cc = u.value("clientCode", "");
            std::string ln = u.value("lastName",   "");
            std::string nc = u.value("nic",        "");
            g_atrad_acntid     = u.value("clientacntid", "");
            g_atrad_broker     = u.value("brokerId",     "");
            g_atrad_client_acc = cc + " ( " + ln + "-" + nc + ")";
        } catch (const std::exception& ex) {
            throw std::runtime_error(
                std::string("ATRAD: getBOIDUserDetails parse error: ") + ex.what()
                + "  body: " + r.body.substr(0, 300));
        }
        if (g_atrad_acntid.empty())
            throw std::runtime_error("ATRAD: empty acntid from getBOIDUserDetails");
    }

    // Step 5: Build and store full cookie string from jar
    g_atrad_cookie_str = build_atrad_cookie_from_jar();
    g_atrad_username   = username;

    sys_log("ATRAD: login OK — acntid=" + g_atrad_acntid
            + "  broker=" + g_atrad_broker
            + "  clientAcc=" + g_atrad_client_acc, "ok");
}

// Ping ATRAD checkUserSession. Returns true if session is alive.
// Takes g_atrad_mutex twice: once to snapshot cookie, once for curl operation.
static bool atrad_check_session() {
    std::string ck_snap, uname;
    {
        std::lock_guard<std::mutex> lk(g_atrad_mutex);
        ck_snap = g_atrad_cookie_str;
        uname   = g_atrad_username;
    }
    if (ck_snap.empty() || uname.empty()) return false;

    std::lock_guard<std::mutex> lk(g_atrad_mutex);
    long ts = (long)(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    std::string url = std::string(ATRAD_BASE)
                      + "/login?action=checkUserSession&format=json"
                        "&txtUserName=" + url_encode(uname)
                      + "&dojo.preventCache=" + std::to_string(ts);

    // Inject cookie directly for this GET
    std::string body;
    body.reserve(256);
    curl_easy_reset(g_atrad_curl);
    curl_easy_setopt(g_atrad_curl, CURLOPT_URL,             url.c_str());
    curl_easy_setopt(g_atrad_curl, CURLOPT_SSL_VERIFYPEER,  0L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_SSL_VERIFYHOST,  0L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_FOLLOWLOCATION,  1L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_TIMEOUT,         10L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_WRITEFUNCTION,   write_cb);
    curl_easy_setopt(g_atrad_curl, CURLOPT_WRITEDATA,       &body);

    std::string cookie_hdr = "Cookie: " + ck_snap;
    curl_slist* hdrs = nullptr;
    hdrs = curl_slist_append(hdrs, cookie_hdr.c_str());
    hdrs = curl_slist_append(hdrs, "x-requested-with: XMLHttpRequest");
    curl_easy_setopt(g_atrad_curl, CURLOPT_HTTPHEADER, hdrs);
    curl_easy_perform(g_atrad_curl);
    curl_slist_free_all(hdrs);

    // Response: {"code":"0","data":{'validation':[true]}}
    return (body.find("\"code\":\"0\"") != std::string::npos ||
            body.find("'validation'") != std::string::npos);
}

// ── Watch ─────────────────────────────────────────────────────────────────────
struct OrderRecord {
    double price;
    int    qty;
};

struct Watch {
    std::string              id;
    std::string              symbol;       // Naasa subscription symbol and order Scrip (e.g. "UPPER")
    int                      qty       = 0;
    int                      endingQty = 0;
    double                   maxSpend  = 0.0;
    double                   spent     = 0.0;
    std::string              name;
    std::vector<OrderRecord> orders;
    std::atomic<bool>        stop{false};

    // per-watch LTP state
    std::atomic<double> last_ltp{-1.0};
    std::atomic<bool>   first_order_done{false};

    // Open price (prev day's final LTP = field 14 from Naasa type-1 tick).
    std::atomic<double> open_price{0.0};

    // latency tracking
    std::mutex           lat_mutex;
    std::vector<int64_t> tick_to_order_us;
    std::vector<int64_t> api_rtt_us;

    // dedicated curl handle for order POSTs — no contention with other watches
    CURL*       order_curl  = nullptr;
    std::mutex  order_curl_mutex;
    // Reserved — headers are built per-request in place_order() / cmd_fire().
    curl_slist* order_hdrs  = nullptr;

    ~Watch() {
        if (order_curl) {
            curl_easy_cleanup(order_curl);
            order_curl = nullptr;
        }
        curl_slist_free_all(order_hdrs);
    }

    std::thread worker;
};

static std::unordered_map<std::string, std::shared_ptr<Watch>> g_watches;
static std::mutex                                              g_watches_mutex;

// ── Shutdown flag ─────────────────────────────────────────────────────────────
static std::atomic<bool> g_shutdown{false};

// ── Print ─────────────────────────────────────────────────────────────────────
static std::mutex g_print_mutex;

static void cprint(const std::string& text) {
    std::lock_guard<std::mutex> lk(g_print_mutex);
    std::cout << text << "\n" << std::flush;
}

// Cross-platform gmtime into Nepal time (UTC+5:45)
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

static void sys_log(const std::string& msg, const std::string& level = "sys") {
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

// ── Utility: Base64 encode ────────────────────────────────────────────────────
static const std::string B64_CHARS =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static std::string base64_encode(const std::string& in) {
    std::string out;
    int val = 0, valb = -6;
    for (unsigned char c : in) {
        val = (val << 8) + c;
        valb += 8;
        while (valb >= 0) {
            out.push_back(B64_CHARS[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if (valb > -6)
        out.push_back(B64_CHARS[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

// ── Utility: Base64 decode ────────────────────────────────────────────────────
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

// ── Utility: zlib decompress ──────────────────────────────────────────────────
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

// ── Utility: URL encode ───────────────────────────────────────────────────────
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

// ── Utility: zero-alloc Nth field access ──────────────────────────────────────
static std::string_view nth_field(std::string_view s, char delim, int n) {
    for (int i = 0; i < n; i++) {
        auto pos = s.find(delim);
        if (pos == std::string_view::npos) return {};
        s = s.substr(pos + 1);
    }
    return s.substr(0, s.find(delim));
}

static int count_fields(std::string_view s, char delim) {
    int n = 1;
    for (char c : s) if (c == delim) ++n;
    return n;
}

// ── Utility: UUID ─────────────────────────────────────────────────────────────
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

// ── Utility: Nepal time ───────────────────────────────────────────────────────
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

// Permanent connection options — set ONCE at handle creation, NEVER called again.
// curl_easy_reset() is never called on order handles: the TCP+TLS+HTTP2 connection
// cache survives across all requests on the same handle.
static void apply_conn_opts(CURL* c) {
    curl_easy_setopt(c, CURLOPT_NOSIGNAL,        1L);     // mandatory for multi-threaded
    curl_easy_setopt(c, CURLOPT_TIMEOUT_MS,   5000L);     // 5s hard cap per request
    curl_easy_setopt(c, CURLOPT_TCP_KEEPALIVE,   1L);
    curl_easy_setopt(c, CURLOPT_TCP_KEEPIDLE,   10L);     // probe after 10s idle (IIS closes ~15s)
    curl_easy_setopt(c, CURLOPT_TCP_KEEPINTVL,   5L);
    curl_easy_setopt(c, CURLOPT_MAXAGE_CONN,   1200L);
    curl_easy_setopt(c, CURLOPT_FORBID_REUSE,    0L);
    curl_easy_setopt(c, CURLOPT_FRESH_CONNECT,   0L);
    curl_easy_setopt(c, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2TLS);
}

// ── Order placement ───────────────────────────────────────────────────────────
// Build ATRAD (Sweta Securities) order — form-encoded, matches browser POST in
// necessary_endpoints/order. market_price = LTP, price = bid price (LTP * 1.02 cap).
static std::string build_atrad_order_payload(const std::string& symbol, int qty,
                                              double price, double market_price) {
    char pbuf[32], mbuf[32];
    snprintf(pbuf, sizeof(pbuf), "%.1f", price);
    snprintf(mbuf, sizeof(mbuf), "%.1f", market_price);

    std::string s;
    s.reserve(1024);
    s += "action=submitOrder";
    s += "&market=NEPSE";
    s += "&broker="           + url_encode(g_atrad_broker);
    s += "&format=json";
    s += "&clientOrderId=cseOrderId";
    s += "&brokerClient=1";
    s += "&orderStatus=Open";
    s += "&filledQty=";
    s += "&acntid="           + url_encode(g_atrad_acntid);
    s += "&oldPrice=";
    s += "&oldQty=";
    s += "&remainder=";
    s += "&orderplacedate=";
    s += "&marketPrice="      + std::string(mbuf);
    s += "&oldDisclose=";
    s += "&txtContraBroker=110";
    s += "&txtapprovalReason=";
    s += "&txtsenttoapproval=no";
    s += "&txtCompId=";
    s += "&txtOdrStatus=";
    s += "&duplicateOrderId=" + gen_atrad_dup_id();
    s += "&product=web";
    s += "&clientAcc="        + url_encode(g_atrad_client_acc);
    s += "&assetSelect=1";
    s += "&actionSelect=1";
    s += "&txtSecurity="      + url_encode(symbol);
    s += "&cmbTypeOfOrder=1";
    s += "&spnQuantity="      + std::to_string(qty);
    s += "&spnPrice="         + std::string(pbuf);
    s += "&cmbTif=16";
    s += "&cmbTifDays=1";
    s += "&cmbBoard=1";
    s += "&hiddenSpnCseFee=0.02";
    s += "&txtContraBroker_=110";
    s += "&brokerClientVal=1";
    s += "&confirm=1";
    return s;
}

static void print_totals();  // forward decl

// ATRAD order success: HTTP 200 AND "code":"0" (string) in response body.
static bool is_order_success(long http_code, const std::string& body) {
    if (http_code != 200) return false;
    // Fast path: plain string search — avoids JSON parse on hot path.
    return body.find("\"code\":\"0\"") != std::string::npos;
}

// Place one order via ATRAD on watch->order_curl. Measures latency.
// ltp   = raw LTP from WS tick (used as marketPrice in ATRAD payload)
// price = bid price = min(floor(ltp*1.02*10)/10, circuit)
static void place_order(double price, int qty, double ltp,
                        const std::string& watch_id,
                        std::chrono::steady_clock::time_point tick_time) {
    std::shared_ptr<Watch> watch;
    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        auto it = g_watches.find(watch_id);
        if (it == g_watches.end()) return;
        watch = it->second;
    }

    std::string payload = build_atrad_order_payload(watch->symbol, qty, price, ltp);

    // Snapshot ATRAD cookie string — set once at login, no mutex needed on hot path.
    std::string cookie_hdr = "Cookie: " + g_atrad_cookie_str;

    auto do_post = [&]() -> HttpResponse {
        std::lock_guard<std::mutex> wl(watch->order_curl_mutex);
        std::string body;
        body.reserve(4096);

        // Build per-request header list — ATRAD form-encoded POST headers.
        curl_slist* req_hdrs = nullptr;
        req_hdrs = curl_slist_append(req_hdrs, cookie_hdr.c_str());
        req_hdrs = curl_slist_append(req_hdrs,
            "Content-Type: application/x-www-form-urlencoded");
        req_hdrs = curl_slist_append(req_hdrs,
            "x-requested-with: XMLHttpRequest");
        req_hdrs = curl_slist_append(req_hdrs,
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36");
        req_hdrs = curl_slist_append(req_hdrs,
            (std::string("Referer: ") + ATRAD_BASE + "/home?action=showHome&format=html").c_str());

        // No curl_easy_reset() — permanent options survive, HTTP2 connection reused.
        curl_easy_setopt(watch->order_curl, CURLOPT_URL,           ATRAD_ORDER_URL);
        curl_easy_setopt(watch->order_curl, CURLOPT_POST,          1L);
        curl_easy_setopt(watch->order_curl, CURLOPT_POSTFIELDS,    payload.c_str());
        curl_easy_setopt(watch->order_curl, CURLOPT_POSTFIELDSIZE, (long)payload.size());
        curl_easy_setopt(watch->order_curl, CURLOPT_HTTPHEADER,    req_hdrs);
        curl_easy_setopt(watch->order_curl, CURLOPT_WRITEDATA,     &body);
        curl_easy_perform(watch->order_curl);
        curl_slist_free_all(req_hdrs);

        long code = 0;
        curl_easy_getinfo(watch->order_curl, CURLINFO_RESPONSE_CODE, &code);
        return {code, body};
    };

    auto t_send = std::chrono::steady_clock::now();
    HttpResponse r = do_post();
    auto t_recv = std::chrono::steady_clock::now();

    int64_t rtt_us = std::chrono::duration_cast<std::chrono::microseconds>(t_recv - t_send).count();
    int64_t t2o_us = std::chrono::duration_cast<std::chrono::microseconds>(t_send - tick_time).count();
    {
        std::lock_guard<std::mutex> ll(watch->lat_mutex);
        watch->tick_to_order_us.push_back(t2o_us);
        watch->api_rtt_us.push_back(rtt_us);
    }

    auto handle_success = [&]() {
        double spent;
        {
            std::lock_guard<std::mutex> ll(watch->lat_mutex);
            watch->orders.push_back({price, qty});
            watch->spent += price * qty;
            spent = watch->spent;
        }

        std::ostringstream ss;
        ss << std::fixed << std::setprecision(1);
        ss << BGN_ << "  ★ ORDER PLACED" << R_
           << "  watch=" << BCY_ << watch_id << R_
           << "  price=" << B_ << price << R_
           << "  qty=" << B_ << qty << R_
           << "  spent=" << BGN_ << "Rs " << spent << R_
           << " / Rs " << watch->maxSpend;
        cprint(ss.str());
        print_totals();

        if (spent >= watch->maxSpend) {
            cprint(std::string(YL_) + "  [BUDGET HIT]" + R_
                   + "  watch=" + watch_id + " — stopping");
            watch->stop.store(true);
        }
    };

    if (is_order_success(r.status, r.body)) { handle_success(); return; }

    // Order failed — log ATRAD description/error from response
    std::string msg;
    try {
        auto body_fixed = fix_atrad_json(r.body);
        json resp = json::parse(body_fixed);
        msg = resp.value("description", r.body.substr(0, 300));
    } catch (...) { msg = r.body.substr(0, 300); }
    watch_log(watch_id, "Order failed (HTTP " + std::to_string(r.status) + "): " + msg, "error");
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

        thread_local std::vector<uint8_t> tl_b64_buf;
        thread_local std::string          tl_decomp_buf;
        try {
            std::string_view payload(raw.data() + caret + 1,
                                     raw.size()  - caret - 1);
            base64_decode_into(payload, tl_b64_buf);
            zlib_decompress_into(tl_b64_buf, tl_decomp_buf);
        } catch (...) {
            break;
        }
        const std::string& decoded = tl_decomp_buf;

        std::string_view sv(decoded);
        while (!sv.empty()) {
            auto nl = sv.find('\n');
            std::string_view line = sv.substr(0, nl == std::string_view::npos ? sv.size() : nl);
            sv = (nl == std::string_view::npos) ? std::string_view{} : sv.substr(nl + 1);
            if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
            if (line.empty()) continue;

            if (count_fields(line, '$') < 5) continue;

            auto msg_type = nth_field(line, '$', 0);
            auto ver_sym  = nth_field(line, '$', 2);
            auto data_sv  = nth_field(line, '$', 4);

            if (msg_type != "1") continue;
            if (ver_sym.size() < 5 || ver_sym.substr(0, 4) != "25.1") continue;

            auto bang = ver_sym.find('!');
            if (bang == std::string_view::npos) continue;
            auto sym = ver_sym.substr(bang + 1);

            if (sym != ctx->symbol) continue;

            if (count_fields(data_sv, '^') < 15) continue;
            auto ltp_sv = nth_field(data_sv, '^', 0);
            auto pc_sv  = nth_field(data_sv, '^', 14);

            double ltp = 0.0, prev_close = 0.0;
            {
                char* e;
                ltp = std::strtod(ltp_sv.data(), &e);
                if (e == ltp_sv.data()) continue;
                prev_close = std::strtod(pc_sv.data(), &e);
                if (e == pc_sv.data()) continue;
            }

            ctx->watch_ptr->open_price.store(prev_close, std::memory_order_relaxed);

            double circuit    = std::floor(prev_close * 1.10 * 10.0) / 10.0;
            double bid_price  = std::min(std::floor(ltp * 1.02 * 10.0) / 10.0, circuit);
            bool   at_circuit = bid_price >= circuit;

            if (!is_market_open()) {
                char tbuf[96];
                snprintf(tbuf, sizeof(tbuf), "Tick  LTP=%.1f  bid=%.1f  circuit=%.1f%s",
                         ltp, bid_price, circuit, at_circuit ? "  [CIRCUIT]" : "");
                watch_log(ctx->watch_id, tbuf, "info");
                continue;
            }

            int    qty      = 0;
            double prev_ltp = -1.0;
            bool   is_first = false;

            {
                auto& w = *ctx->watch_ptr;
                if (w.stop.load()) break;
                std::lock_guard<std::mutex> ll(w.lat_mutex);
                is_first = !w.first_order_done.load(std::memory_order_relaxed);
                prev_ltp = w.last_ltp.load(std::memory_order_relaxed);
                qty      = at_circuit ? w.endingQty : w.qty;
                if (is_first) {
                    w.first_order_done.store(true,    std::memory_order_relaxed);
                    w.last_ltp.store(ltp,             std::memory_order_relaxed);
                } else if (prev_ltp != ltp) {
                    w.last_ltp.store(ltp,             std::memory_order_relaxed);
                }
            }

            if (is_first || (prev_ltp >= 0 && prev_ltp != ltp)) {
                place_order(bid_price, qty, ltp, ctx->watch_id, t_recv);
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

        // Do NOT call naasa_validate_session() here on every reconnect.
        // Each call is treated by Naasa as a new login, which sends
        // 104^User logged in from another source to any other connected watch.
        // The heartbeat thread keeps the session alive every 10 min.
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
            watch_log(watch_id, "Naasa session not ready — waiting 1s", "warning");
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
        info.ka_time     = 20;   // TCP keepalive after 20s idle
        info.ka_probes   = 3;    // 3 probes before declaring dead
        info.ka_interval = 5;    // 5s between probes

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

            // Keepalive: send empty string every 1s.
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_keepalive).count() >= 1) {
                ctx.send_keepalive = true;
                if (ctx.wsi) lws_callback_on_writable(ctx.wsi);
                last_keepalive = now;
            }

            // Watchdog: if nothing received for 30s, force reconnect.
            if (ctx.subscribed &&
                std::chrono::duration_cast<std::chrono::seconds>(now - ctx.last_rx).count() >= 30) {
                watch_log(watch_id, "Watchdog: no data for 30s — reconnecting", "warning");
                break;
            }

            // Session touch: force a WS reconnect every 5 min to reset the
            // ASP.NET session idle timer via the Cookie in the HTTP upgrade.
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
    // Naasa ValidateSessionNo every 10 min — refreshes the WS sessionNo.
    // Runs in heartbeat thread, NOT the WS thread — calling naasa_request() from
    // the WS thread blocks lws_service() and causes a segfault.
    auto last_naasa_keepalive   = std::chrono::steady_clock::now();
    // ASP.NET session keepalive: GET /MarketOrder/Order every 5 min.
    // WS reconnects (HTTP upgrade) do NOT touch the ASP.NET session idle timer.
    auto last_asp_session_touch = std::chrono::steady_clock::now();
    // ATRAD checkUserSession keepalive every 30s — keeps JSESSIONID alive.
    auto last_atrad_keepalive   = std::chrono::steady_clock::now();
    // Continuous order_curl warmup: GET ATRAD marketStatus every 5s on all active watches.
    // Same host:port as order URL → HTTP/2 connection never idles out.
    auto last_order_warmup      = std::chrono::steady_clock::now();
    // Pre-market warmup at 10:59:55 NST.
    bool warmup_done = false;

    while (!g_hb_stop.load()) {

        // ASP.NET session keepalive: GET /MarketOrder/Order every 5 min.
        if (std::chrono::duration_cast<std::chrono::minutes>(
                std::chrono::steady_clock::now() - last_asp_session_touch).count() >= 5) {
            naasa_request(std::string(NAASA_BASE) + "/MarketOrder/Order", "GET");
            last_asp_session_touch = std::chrono::steady_clock::now();
        }

        // ATRAD session keepalive: checkUserSession every 30s.
        if (std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - last_atrad_keepalive).count() >= 30) {
            bool ok = atrad_check_session();
            if (!ok)
                sys_log("ATRAD: checkUserSession failed — session may have expired", "err");
            last_atrad_keepalive = std::chrono::steady_clock::now();
        }

        // Continuous order_curl keepalive: GET ATRAD marketStatus every 5s.
        // Same host:port as order URL → HTTP/2 connection never idles out.
        if (std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - last_order_warmup).count() >= 5) {
            last_order_warmup = std::chrono::steady_clock::now();
            std::vector<std::shared_ptr<Watch>> snaps;
            {
                std::lock_guard<std::mutex> lk(g_watches_mutex);
                for (auto& [wid, w] : g_watches)
                    if (!w->stop.load() && w->order_curl) snaps.push_back(w);
            }
            std::string ck_hdr = "Cookie: " + g_atrad_cookie_str;
            std::string warmup_url = std::string(ATRAD_BASE)
                                     + "/home?action=marketStatus&dojo.preventCache="
                                     + std::to_string((long)(std::chrono::duration_cast<
                                         std::chrono::milliseconds>(
                                         std::chrono::system_clock::now().time_since_epoch())
                                         .count()));
            for (auto& w : snaps) {
                std::lock_guard<std::mutex> wl(w->order_curl_mutex);
                curl_slist* wh = nullptr;
                wh = curl_slist_append(wh, ck_hdr.c_str());
                wh = curl_slist_append(wh, "x-requested-with: XMLHttpRequest");
                std::string body;
                // No reset — only switch method+URL, permanent options intact.
                curl_easy_setopt(w->order_curl, CURLOPT_URL,        warmup_url.c_str());
                curl_easy_setopt(w->order_curl, CURLOPT_HTTPGET,    1L);
                curl_easy_setopt(w->order_curl, CURLOPT_HTTPHEADER, wh);
                curl_easy_setopt(w->order_curl, CURLOPT_WRITEDATA,  &body);
                curl_easy_perform(w->order_curl);
                curl_slist_free_all(wh);
            }
        }

        // ValidateSessionNo every 10 min — refreshes the WS sessionNo for reconnects.
        if (std::chrono::duration_cast<std::chrono::minutes>(
                std::chrono::steady_clock::now() - last_naasa_keepalive).count() >= 10) {
            sys_log("Naasa: session keep-alive HTTP POST...", "sys");
            bool ok = naasa_validate_session();
            if (ok) {
                sys_log("Naasa: session keep-alive OK", "ok");
            } else {
                sys_log("Naasa: session keep-alive failed — session expired.", "err");
                if (!g_naasa_auto_email.empty()) {
                    sys_log("Naasa: attempting auto re-login...", "sys");
                    bool relogin_ok = false;
                    try {
                        naasa_do_login(g_naasa_auto_email, g_naasa_auto_password);
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

        // Pre-market warmup: at 10:59:55 NST fire a HEAD on each watch's order_curl.
        {
            int nsecs = nepal_secs_since_midnight();
            if (nsecs >= 11 * 3600 + 1) {
                warmup_done = false;
            }
            if (!warmup_done && nsecs >= 10 * 3600 + 59 * 60 + 55 && nsecs < 11 * 3600) {
                warmup_done = true;
                std::vector<std::string> active_wids;
                {
                    std::lock_guard<std::mutex> lk(g_watches_mutex);
                    for (auto& [wid, w] : g_watches)
                        if (!w->stop.load() && w->order_curl)
                            active_wids.push_back(wid);
                }
                if (!active_wids.empty()) {
                    sys_log("Pre-market warmup: touching order connections for "
                            + std::to_string(active_wids.size()) + " watch(es)", "sys");
                }
                for (auto& wid : active_wids) {
                    std::shared_ptr<Watch> w_ptr;
                    {
                        std::lock_guard<std::mutex> lk(g_watches_mutex);
                        auto it = g_watches.find(wid);
                        if (it != g_watches.end()) w_ptr = it->second;
                    }
                    if (!w_ptr || !w_ptr->order_curl) continue;
                    {
                        std::lock_guard<std::mutex> wl(w_ptr->order_curl_mutex);
                        std::string body;
                        std::string w_ck2 = "Cookie: " + g_atrad_cookie_str;
                        std::string wu2 = std::string(ATRAD_BASE)
                                          + "/home?action=marketStatus&dojo.preventCache="
                                          + std::to_string((long)(std::chrono::duration_cast<
                                              std::chrono::milliseconds>(
                                              std::chrono::system_clock::now().time_since_epoch())
                                              .count()));
                        curl_slist* wh2 = nullptr;
                        wh2 = curl_slist_append(wh2, w_ck2.c_str());
                        wh2 = curl_slist_append(wh2, "x-requested-with: XMLHttpRequest");
                        // No reset — only switch method+URL, permanent options intact.
                        curl_easy_setopt(w_ptr->order_curl, CURLOPT_URL,        wu2.c_str());
                        curl_easy_setopt(w_ptr->order_curl, CURLOPT_HTTPGET,    1L);
                        curl_easy_setopt(w_ptr->order_curl, CURLOPT_HTTPHEADER, wh2);
                        curl_easy_setopt(w_ptr->order_curl, CURLOPT_WRITEDATA,  &body);
                        curl_easy_perform(w_ptr->order_curl);
                        curl_slist_free_all(wh2);
                    }
                    sys_log("Pre-market warmup done for watch " + wid, "ok");
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
    std::shared_ptr<Watch> w_snap;
    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        auto it = g_watches.find(wid);
        if (it != g_watches.end()) {
            it->second->stop.store(true);
            w_snap = it->second;
        }
    }
    if (w_snap) {
        cprint(std::string(YL_) + "  [WATCH STOPPED]" + R_ + "  " + wid);
        print_totals();
    }
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

    auto watch         = std::make_shared<Watch>();
    watch->id          = wid;
    watch->symbol      = symbol_str;
    watch->qty         = qty;
    watch->endingQty   = ending_qty;
    watch->maxSpend    = max_spend;

    // Per-watch order handle: independent curl handle for ATRAD order POSTs.
    // No curl_easy_reset() is ever called after initial setup — connection cache survives.
    watch->order_curl = curl_easy_init();
    if (!watch->order_curl) {
        cprint(std::string(RD_) + "  curl_easy_init failed for watch order handle." + R_);
        return;
    }
    // Permanent options — set once, survive for the handle's entire lifetime.
    curl_easy_setopt(watch->order_curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(watch->order_curl, CURLOPT_SSL_VERIFYHOST, 0L);
    curl_easy_setopt(watch->order_curl, CURLOPT_TCP_NODELAY,    1L);
    curl_easy_setopt(watch->order_curl, CURLOPT_RESOLVE,        g_dns_pins);
    curl_easy_setopt(watch->order_curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(watch->order_curl, CURLOPT_WRITEFUNCTION,  write_cb);
    apply_conn_opts(watch->order_curl);

    // Initial warmup: establish TCP+TLS+HTTP2 to ATRAD host immediately.
    {
        std::string w_ck = "Cookie: " + g_atrad_cookie_str;
        std::string warmup_url = std::string(ATRAD_BASE)
                                 + "/home?action=marketStatus&dojo.preventCache="
                                 + std::to_string((long)(std::chrono::duration_cast<
                                     std::chrono::milliseconds>(
                                     std::chrono::system_clock::now().time_since_epoch())
                                     .count()));
        curl_slist* wh = nullptr;
        wh = curl_slist_append(wh, w_ck.c_str());
        wh = curl_slist_append(wh, "x-requested-with: XMLHttpRequest");
        std::string dummy;
        curl_easy_setopt(watch->order_curl, CURLOPT_URL,        warmup_url.c_str());
        curl_easy_setopt(watch->order_curl, CURLOPT_HTTPGET,    1L);
        curl_easy_setopt(watch->order_curl, CURLOPT_HTTPHEADER, wh);
        curl_easy_setopt(watch->order_curl, CURLOPT_WRITEDATA,  &dummy);
        curl_easy_perform(watch->order_curl);
        curl_slist_free_all(wh);
    }

    {
        std::lock_guard<std::mutex> lk(g_watches_mutex);
        g_watches[wid] = watch;
    }

    watch->worker = std::thread(run_watch_socket, wid);
#ifndef _WIN32
    // Elevate WS thread to real-time priority — prevents OS from preempting it
    // between tick receive and order dispatch. Silently skipped if CAP_SYS_NICE
    // is not granted (no crash, just runs at normal priority).
    {
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
        << std::setw(18) << "Name"
        << std::right << std::setw(5) << "Qty"
        << std::setw(6)  << "EQty"
        << std::setw(15) << "Spent"
        << std::setw(15) << "Budget"
        << "  St" << R_;
    cprint(hdr.str());
    cprint("  " + std::string(79, '-'));

    for (auto& [wid, w] : g_watches) {
        std::string active = !w->stop.load()
            ? (std::string(BGN_) + "ON" + R_)
            : (std::string(DM_)  + "off" + R_);
        std::string name = w->name.empty() ? "?" : w->name;
        if (name.size() > 18) name = name.substr(0, 18);

        double spent;
        { std::lock_guard<std::mutex> ll(w->lat_mutex); spent = w->spent; }
        std::ostringstream row;
        row << std::fixed << std::setprecision(1);
        row << "  " << BCY_ << std::left << std::setw(10) << wid << R_
            << std::setw(10) << w->symbol
            << std::setw(18) << name
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
        std::string name = w->name.empty() ? w->symbol : w->name;
        cprint("  Watch " + std::string(BCY_) + wid + R_
               + "  (" + B_ + name + R_ + ")");
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
    if (g_watches.empty()) {
        cprint(std::string(DM_) + "  No watches." + R_);
        return;
    }

    for (auto& [wid, w] : g_watches) {
        std::vector<int64_t> t2o, rtt;
        {
            std::lock_guard<std::mutex> ll(w->lat_mutex);
            t2o = w->tick_to_order_us;
            rtt = w->api_rtt_us;
        }
        size_t count = t2o.size();
        std::string nm = w->name.empty() ? w->symbol : w->name;

        cprint("  " + std::string(BCY_) + wid + R_
               + "  (" + nm + ")  orders=" + std::to_string(count));

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
// Perform one order POST on a pre-configured fire_curl handle.
static long fire_shot(CURL* fire_curl, const std::string& payload,
                      curl_slist* hdrs_snap, std::string& out_body, double& rtt_ms) {
    out_body.clear();
    out_body.reserve(256);
    curl_easy_setopt(fire_curl, CURLOPT_POSTFIELDS,    payload.c_str());
    curl_easy_setopt(fire_curl, CURLOPT_POSTFIELDSIZE, (long)payload.size());
    curl_easy_setopt(fire_curl, CURLOPT_WRITEDATA,     &out_body);
    curl_easy_setopt(fire_curl, CURLOPT_HTTPHEADER,    hdrs_snap);
    auto t0 = std::chrono::steady_clock::now();
    curl_easy_perform(fire_curl);
    rtt_ms = std::chrono::duration<double, std::milli>(
                 std::chrono::steady_clock::now() - t0).count();
    long code = 0;
    curl_easy_getinfo(fire_curl, CURLINFO_RESPONSE_CODE, &code);
    return code;
}

// Blind-fire orders at the calculated bid ceiling (no WS tick required).
// Usage: fire <SYMBOL> — symbol must match an active watch.
static void cmd_fire(const std::string& sym) {
    // 1. Resolve watch
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
        cprint(std::string(YL_) + "  No WS tick yet — entering manual price mode (local only, does not affect watch)." + R_);
        std::cout << "  Enter open / prev-close price: " << std::flush;
        std::string open_str;
        std::getline(std::cin, open_str);
        open_str.erase(0, open_str.find_first_not_of(" \t"));
        open_str.erase(open_str.find_last_not_of(" \t\r\n") + 1);
        try { open = std::stod(open_str); } catch (...) {}
        if (open <= 0.0) {
            cprint(std::string(RD_) + "  Invalid open price — aborted." + R_);
            return;
        }
    }

    double circuit = std::floor(open * 1.10 * 10.0) / 10.0;
    {
        std::ostringstream inf;
        inf << std::fixed << std::setprecision(1);
        inf << "  Open: " << open << "  →  Circuit ceiling: " << BCY_ << circuit << R_;
        cprint(inf.str());
    }

    // 2. Prompt for future LTP
    std::cout << "  Enter future LTP (price you will sell at): " << std::flush;
    std::string ltp_str;
    std::getline(std::cin, ltp_str);
    ltp_str.erase(0, ltp_str.find_first_not_of(" \t"));
    ltp_str.erase(ltp_str.find_last_not_of(" \t\r\n") + 1);
    double future_ltp = 0.0;
    try { future_ltp = std::stod(ltp_str); } catch (...) {}
    if (future_ltp <= 0.0) {
        cprint(std::string(RD_) + "  Invalid LTP — aborted." + R_);
        return;
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
    std::string dummy;
    std::getline(std::cin, dummy);

    // 3. Build ATRAD payload + header list — reused for every shot.
    //    Uses watch->order_curl (already warm from 5s heartbeat GET marketStatus).
    //    market_price = future_ltp (user-supplied), bid = calculated ceiling bid.
    std::string payload = build_atrad_order_payload(watch->symbol, qty, bid, future_ltp);
    std::string fire_cookie_hdr = "Cookie: " + g_atrad_cookie_str;
    curl_slist* hdrs_snap = nullptr;
    hdrs_snap = curl_slist_append(hdrs_snap, fire_cookie_hdr.c_str());
    hdrs_snap = curl_slist_append(hdrs_snap,
        "Content-Type: application/x-www-form-urlencoded");
    hdrs_snap = curl_slist_append(hdrs_snap,
        "x-requested-with: XMLHttpRequest");
    hdrs_snap = curl_slist_append(hdrs_snap,
        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36");
    hdrs_snap = curl_slist_append(hdrs_snap,
        (std::string("Referer: ") + ATRAD_BASE + "/home?action=showHome&format=html").c_str());

    // 4b. 3-second countdown — switch to Naasa web tab, prepare to click SELL.
    for (int i = 3; i >= 1; --i) {
        cprint(std::string("  ") + YL_ + "Firing in " + std::to_string(i) + "s...  ← switch to Naasa tab now" + R_);
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    cprint(std::string("  ") + RD_ + B_ + "★ FIRE!  ← CLICK SELL NOW" + R_);

    // 5. Up to 7 shots; stop on first success (ErrorCode == 0).
    //    Uses watch->order_curl (warm from 5s heartbeat) — no new TCP/TLS/HTTP2 setup.
    //    curl_easy_reset preserves the connection cache so the warm HTTP/2 stream survives.
    const int MAX_SHOTS = 7;
    std::string shot_body;
    shot_body.reserve(256);
    for (int shot = 1; shot <= MAX_SHOTS; ++shot) {
        double rtt_ms = 0.0;
        long status;
        {
            std::lock_guard<std::mutex> wl(watch->order_curl_mutex);
            // No reset — permanent options intact, HTTP2 connection reused from 5s warmup.
            curl_easy_setopt(watch->order_curl, CURLOPT_URL,  ATRAD_ORDER_URL);
            curl_easy_setopt(watch->order_curl, CURLOPT_POST, 1L);
            status = fire_shot(watch->order_curl, payload, hdrs_snap, shot_body, rtt_ms);
        }
        bool ok = is_order_success(status, shot_body);
        {
            std::ostringstream ls;
            ls << std::fixed << std::setprecision(1);
            ls << "  Shot " << shot << "/" << MAX_SHOTS << "  HTTP " << status
               << "  RTT=" << rtt_ms << "ms";
            if (ok) ls << "  " << BGN_ << "SUCCESS" << R_;
            cprint(ls.str());
        }
        if (!shot_body.empty())
            cprint(std::string("  JSON: ") + shot_body);
        if (ok) {
            double spent;
            {
                std::lock_guard<std::mutex> ll(watch->lat_mutex);
                watch->orders.push_back({bid, qty});
                watch->spent += bid * (double)qty;
                spent = watch->spent;
                watch->first_order_done.store(true,        std::memory_order_relaxed);
                watch->last_ltp.store(future_ltp,          std::memory_order_relaxed);
            }
            cprint(std::string("  ") + BGN_ + "Order accepted — fire sequence stopped." + R_);
            print_totals();
            if (spent >= watch->maxSpend) {
                cprint(std::string(YL_) + "  [BUDGET HIT]" + R_
                       + "  watch=" + watch->id + " — stopping");
                watch->stop.store(true);
            }
            curl_slist_free_all(hdrs_snap);
            return;
        }
    }
    cprint(std::string("  ") + YL_ + "Fire sequence done — no success received." + R_);
    curl_slist_free_all(hdrs_snap);
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

    for (auto& w : snaps) {
        if (w->worker.joinable()) w->worker.join();
    }

    if (g_hb_thread.joinable()) g_hb_thread.join();
    if (g_mc_thread.joinable()) g_mc_thread.join();

    cmd_print_summary();
}

// Signal handler: set atomic flag and unblock getline by closing stdin.
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
          "  ║      Feed : Naasa Securities (WebSocket)         ║\n"
          "  ║      Orders: ATRAD / Sweta Securities            ║\n"
          "  ╚══════════════════════════════════════════════════╝" + R_);
}

// ── Login flow ────────────────────────────────────────────────────────────────
static bool do_login_flow() {
    cprint(std::string("\n") + B_ + "  Naasa Securities login (WebSocket feed)" + R_);
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
        // Manual path: user obtained session values from naasa_session.py on Windows.
        // naasa_session.py prints:
        //   AspNetCore.Session : <value>
        //   sessionNo          : <value>
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

        // Inject the cookie into the naasa curl handle and persist to the cookie file.
        // Must initialise the cookie engine (CURLOPT_COOKIEFILE/JAR) BEFORE calling
        // CURLOPT_COOKIELIST — otherwise the engine is inactive and the injection is silently ignored.
        {
            std::lock_guard<std::mutex> lk(g_naasa_curl_mutex);
            std::remove(NAASA_COOKIE_FILE);
            curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEFILE, NAASA_COOKIE_FILE);
            curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEJAR,  NAASA_COOKIE_FILE);
            // Netscape cookie format: domain \t flag \t path \t secure \t expiry \t name \t value
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
            // Store cookie value in memory — same as main.py's session.cookies.set()
            g_naasa_asp_cookie = asp_cookie;
        }

        // Call ValidateSessionNo so the server-side session is fully activated.
        // The order endpoint checks a session key that is only set by ValidateSessionNo.
        // Without this call, the order POST redirects to "/" (home) even though the
        // .AspNetCore.Session cookie itself is valid.
        sys_log("Naasa: activating session via ValidateSessionNo...", "sys");
        if (!naasa_validate_session()) {
            // 302 here almost always means the server has bound this session to the
            // IP that created it (the Windows machine running naasa_session.py).
            // Orders will fail from a VPS with a different IP.
            // Fix: use [a] Auto login mode so the VPS creates its own session.
            cprint(std::string(RD_) + B_
                   + "  WARNING: ValidateSessionNo failed — session not valid from this IP."
                   + R_);
            cprint(std::string(YL_)
                   + "  The session cookie is likely IP-bound to the machine that ran naasa_session.py.\n"
                   + "  Orders will fail from this VPS (different IP).\n"
                   + "  Use [a] Auto login so this machine creates its own session."
                   + R_);
            // Do not abort — WS feed still works; user is warned about orders.
        } else {
            // Re-read cookie after ValidateSessionNo in case the server issued a refreshed one.
            std::lock_guard<std::mutex> lk1(g_naasa_curl_mutex);
            std::string cv = get_naasa_cookie(".AspNetCore.Session");
            if (!cv.empty()) {
                std::lock_guard<std::mutex> lk2(g_naasa_session_mutex);
                g_naasa_asp_cookie = cv;
            }
        }

        cprint(std::string("  ") + BGN_ + "Naasa manual session OK" + R_);

    } else {
        // Auto path: this machine does the full Keycloak login.
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
                // Extract cookie into memory so order handles can inject it directly
                // without going through the cookie file.
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

    // ── ATRAD / Sweta Securities login (order system) ─────────────────────────
    cprint(std::string("\n") + B_ + "  ATRAD / Sweta Securities login (order system)" + R_);
    cprint(std::string(DM_)
           + "  tms.swetasecurities.com — Login ID and Password" + R_);

    std::string atrad_user;
    std::cout << "  ATRAD Login ID : " << std::flush;
    std::getline(std::cin, atrad_user);
    {
        auto s = atrad_user.find_first_not_of(" \t");
        auto e = atrad_user.find_last_not_of(" \t\r\n");
        atrad_user = (s == std::string::npos) ? "" : atrad_user.substr(s, e - s + 1);
    }
    std::string atrad_pw = read_password("  ATRAD Password : ");

    for (int attempt = 1; attempt <= 3; attempt++) {
        try {
            atrad_login(atrad_user, atrad_pw);
            break;
        } catch (const std::exception& ex) {
            cprint("  " + std::string(RD_) + "ATRAD attempt " + std::to_string(attempt)
                   + " failed: " + ex.what() + R_);
            if (attempt == 3) {
                cprint(std::string(RD_) + "  ATRAD login failed after 3 attempts." + R_);
                return false;
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    cprint(std::string("  ") + BGN_ + "ATRAD login OK" + R_
           + "  acntid=" + B_ + g_atrad_acntid + R_
           + "  broker=" + B_ + g_atrad_broker + R_);

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
    signal(SIGPIPE, SIG_IGN);   // prevent broken socket from killing the process
#endif

    curl_global_init(CURL_GLOBAL_ALL);

    // DNS pins — bypass runtime DNS on the hot order path.
    g_dns_pins = curl_slist_append(g_dns_pins, "x.naasasecurities.com.np:443:45.115.219.5");
    g_dns_pins = curl_slist_append(g_dns_pins, "tms.swetasecurities.com:443:203.134.251.22");

    g_naasa_curl = curl_easy_init();
    if (!g_naasa_curl) { fprintf(stderr, "curl_easy_init failed (Naasa)\n"); return 1; }
    curl_easy_setopt(g_naasa_curl, CURLOPT_COOKIEFILE,  "");
    curl_easy_setopt(g_naasa_curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(g_naasa_curl, CURLOPT_RESOLVE,     g_dns_pins);

    g_atrad_curl = curl_easy_init();
    if (!g_atrad_curl) { fprintf(stderr, "curl_easy_init failed (ATRAD)\n"); return 1; }
    curl_easy_setopt(g_atrad_curl, CURLOPT_COOKIEFILE,  "");  // enable in-memory cookie engine
    curl_easy_setopt(g_atrad_curl, CURLOPT_TCP_NODELAY, 1L);
    curl_easy_setopt(g_atrad_curl, CURLOPT_RESOLVE,     g_dns_pins);

    // Build static order headers — mirrors Rust build_order_headers() exactly.
    // Cookie is injected per-request as an explicit Cookie: header in req_hdrs.
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

    // Login loop
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
        if (g_atrad_curl) curl_easy_cleanup(g_atrad_curl);
        curl_global_cleanup();
        return 0;
    }

    cmd_print_help();

    // Command loop — exits when stdin closes (^C) or quit/exit typed
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
            bool found = false;
            {
                std::lock_guard<std::mutex> lk(g_watches_mutex);
                found = g_watches.count(wid) > 0;
            }
            if (found) stop_watch(wid);
            else cprint(std::string(RD_) + "  Watch '" + wid + "' not found." + R_);
        }
        else if (cmd == "naasa") {
            // Restore Naasa session after it was stolen by a web login.
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
    if (g_atrad_curl) curl_easy_cleanup(g_atrad_curl);
    curl_global_cleanup();
    return 0;
}

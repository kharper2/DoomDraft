#include "http_server.hh"

#include "cotamer/io.hh"
#include "doc_ops.hh"
#include "doc_state.hh"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <format>
#include <fstream>
#include <iterator>
#include <print>
#include <string>
#include <string_view>
#include <utility>

namespace cot = cotamer;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// http_client_model
// ---------------------------------------------------------------------------

http_client_model::http_client_model(size_t nreplicas, random_source& randomness)
    : client_model(nreplicas, randomness) {}

void http_client_model::start() {
    // Reserve exactly one simulated client (cid 0). The HTTP layer multiplexes
    // many real connections onto this one cid by using distinct serials.
    set_nclients(1);
}

cot::task<pancy::version_type> http_client_model::submit_put(std::string key,
                                                              std::string value) {
    constexpr int kMaxAttempts = 12;
    // Serial layout: low bits = cid, higher bits incremented per request via
    // serial_step(). Atomic offset prevents two concurrent submits from
    // colliding on the same serial.
    uint64_t off = next_serial_offset_.fetch_add(1) + 1;
    uint64_t serial = kMyCid + off * serial_step();

    size_t replica = leader_;
    for (int tries = 0; tries < kMaxAttempts; ++tries) {
        co_await send_request<pancy::put_request>(replica, serial, key, value);
        auto resp = co_await cot::attempt(
            receive_response<pancy::put_response>(replica, serial),
            cot::after(2s)
        );
        if (resp && resp->errcode == pancy::errc::ok) {
            leader_ = replica; // sticky leader
            co_return resp->version;
        }
        // No response, or non-ok: try a different replica next round.
        // receive_response updates `replica` in-place on redirect.
        if (!resp) {
            replica = (replica + 1) % nreplicas();
        }
    }
    co_return 0;
}

// ---------------------------------------------------------------------------
// HTTP wire helpers
// ---------------------------------------------------------------------------

namespace {

struct doc_cache_state {
    std::vector<collab::committed_op> ops_;
    std::string text_;
    pancy::version_type version_ = 0;
    uint64_t epoch_ = 0;

    void refresh_from_db(const pancy::pancydb& db, std::string_view doc_id) {
        std::vector<collab::committed_op> fresh = collab::read_ops(db, doc_id);
        const pancy::version_type fresh_version = fresh.empty() ? 0 : fresh.back().version;
        if (fresh.empty()) {
            if (!ops_.empty()) {
                ops_.clear();
                text_.clear();
                version_ = 0;
                ++epoch_;
            }
            return;
        }

        // Fast path: prefix unchanged, only new suffix ops were committed.
        bool append_only = fresh.size() >= ops_.size();
        if (append_only) {
            for (size_t i = 0; i != ops_.size(); ++i) {
                if (fresh[i].version != ops_[i].version
                    || fresh[i].client_id != ops_[i].client_id
                    || fresh[i].client_seq != ops_[i].client_seq) {
                    append_only = false;
                    break;
                }
            }
        }
        if (append_only) {
            for (size_t i = ops_.size(); i != fresh.size(); ++i) {
                collab::apply_op(text_, fresh[i].op);
                ops_.push_back(fresh[i]);
            }
            if (fresh_version != version_) {
                version_ = fresh_version;
                ++epoch_;
            }
            return;
        }

        // Slow path: recompute full state when prefix changed unexpectedly.
        ops_ = std::move(fresh);
        text_ = collab::reconstruct(ops_);
        version_ = fresh_version;
        ++epoch_;
    }

    void note_committed(const collab::committed_op& c) {
        // Expected hot path: monotonically increasing commit versions.
        if (c.version > version_) {
            collab::apply_op(text_, c.op);
            ops_.push_back(c);
            version_ = c.version;
            ++epoch_;
        }
    }
};

doc_cache_state g_doc_cache;

// Lowercase, ASCII-only.
std::string ascii_lower(std::string s) {
    for (auto& c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

// Find the value of a header (case-insensitive). Returns "" if not found.
std::string find_header(std::string_view head, std::string_view name) {
    std::string lower(head);
    std::string lname = ascii_lower(std::string(name));
    std::string lhead = ascii_lower(lower);
    size_t pos = 0;
    while (pos < lhead.size()) {
        size_t eol = lhead.find("\r\n", pos);
        if (eol == std::string::npos) eol = lhead.size();
        size_t colon = lhead.find(':', pos);
        if (colon != std::string::npos && colon < eol) {
            std::string hname = lhead.substr(pos, colon - pos);
            // trim
            while (!hname.empty() && hname.back() == ' ') hname.pop_back();
            if (hname == lname) {
                size_t v_begin = colon + 1;
                while (v_begin < eol && head[v_begin] == ' ') ++v_begin;
                size_t v_end = eol;
                while (v_end > v_begin && head[v_end - 1] == ' ') --v_end;
                return std::string(head.substr(v_begin, v_end - v_begin));
            }
        }
        pos = eol + 2;
    }
    return {};
}

// Write all bytes of `s`. Returns true on success.
cot::task<bool> write_all(const cot::fd& f, std::string_view s) {
    size_t off = 0;
    while (off < s.size()) {
        try {
            size_t n = co_await cot::write_once(f, s.data() + off, s.size() - off);
            if (n == 0) co_return false;
            off += n;
        } catch (...) {
            co_return false;
        }
    }
    co_return true;
}

// Read until "\r\n\r\n" or EOF or buffer too big. Returns the raw bytes read.
cot::task<std::string> read_request_head(const cot::fd& f) {
    std::string buf;
    buf.reserve(512);
    char tmp[2048];
    while (true) {
        if (buf.find("\r\n\r\n") != std::string::npos) break;
        if (buf.size() > 64 * 1024) break;
        size_t n = 0;
        try {
            n = co_await cot::read_once(f, tmp, sizeof(tmp));
        } catch (...) {
            break;
        }
        if (n == 0) break;
        buf.append(tmp, n);
    }
    co_return buf;
}

// Parse first line + body offset. Returns (method, path, body_start_offset).
// On parse failure returns ("", "", 0).
struct request_line {
    std::string method;
    std::string path;
    size_t body_offset = 0;
};

request_line parse_request_line(std::string_view buf) {
    request_line out;
    size_t header_end = buf.find("\r\n\r\n");
    if (header_end == std::string::npos) return out;
    size_t first_eol = buf.find("\r\n");
    if (first_eol == std::string::npos) return out;
    std::string_view rl = buf.substr(0, first_eol);
    size_t sp1 = rl.find(' ');
    if (sp1 == std::string_view::npos) return out;
    size_t sp2 = rl.find(' ', sp1 + 1);
    if (sp2 == std::string_view::npos) return out;
    out.method = std::string(rl.substr(0, sp1));
    out.path = std::string(rl.substr(sp1 + 1, sp2 - sp1 - 1));
    out.body_offset = header_end + 4;
    return out;
}

// Drain Content-Length more bytes into `body` if not already present.
cot::task<bool> read_body(const cot::fd& f, std::string& body, size_t want) {
    char tmp[2048];
    while (body.size() < want) {
        size_t n = 0;
        try {
            n = co_await cot::read_once(f, tmp, sizeof(tmp));
        } catch (...) {
            co_return false;
        }
        if (n == 0) co_return false;
        body.append(tmp, n);
    }
    co_return true;
}

// JSON helpers (flat objects only, like Chunk 6 inputs).
//
// json_get_string / json_get_int look for `"key"\s*:\s*"..."` or `"key"\s*:\s*N`
// in `body`. Good enough for the trivial bodies we ship from curl/JS.

std::optional<std::string> json_get_string(std::string_view body, std::string_view key) {
    std::string needle = std::format("\"{}\"", key);
    size_t k = body.find(needle);
    if (k == std::string_view::npos) return std::nullopt;
    size_t colon = body.find(':', k + needle.size());
    if (colon == std::string_view::npos) return std::nullopt;
    size_t q1 = body.find('"', colon + 1);
    if (q1 == std::string_view::npos) return std::nullopt;
    std::string out;
    size_t i = q1 + 1;
    while (i < body.size()) {
        char c = body[i];
        if (c == '\\' && i + 1 < body.size()) {
            char n = body[i + 1];
            if (n == 'n') out.push_back('\n');
            else if (n == 't') out.push_back('\t');
            else if (n == '"') out.push_back('"');
            else if (n == '\\') out.push_back('\\');
            else out.push_back(n);
            i += 2;
        } else if (c == '"') {
            return out;
        } else {
            out.push_back(c);
            ++i;
        }
    }
    return std::nullopt;
}

std::optional<int64_t> json_get_int(std::string_view body, std::string_view key) {
    std::string needle = std::format("\"{}\"", key);
    size_t k = body.find(needle);
    if (k == std::string_view::npos) return std::nullopt;
    size_t colon = body.find(':', k + needle.size());
    if (colon == std::string_view::npos) return std::nullopt;
    size_t i = colon + 1;
    while (i < body.size() && (body[i] == ' ' || body[i] == '\t')) ++i;
    bool neg = false;
    if (i < body.size() && (body[i] == '-' || body[i] == '+')) {
        neg = (body[i] == '-');
        ++i;
    }
    if (i >= body.size() || body[i] < '0' || body[i] > '9') return std::nullopt;
    int64_t v = 0;
    while (i < body.size() && body[i] >= '0' && body[i] <= '9') {
        v = v * 10 + (body[i] - '0');
        ++i;
    }
    return neg ? -v : v;
}

// Escape a string for inclusion in a JSON string literal.
std::string json_escape(std::string_view s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    out += std::format("\\u{:04x}", static_cast<unsigned char>(c));
                } else {
                    out.push_back(c);
                }
        }
    }
    return out;
}

// Stable hash of a string client_id into a uint64_t for op_key.
uint64_t hash_client_id(std::string_view s) {
    // FNV-1a 64
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : s) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h;
}

// Compose "<status> <reason>\r\n<headers>\r\n\r\n<body>".
std::string http_response(int status, std::string_view reason,
                          std::string_view content_type,
                          std::string_view body,
                          std::string_view extra_headers = {}) {
    return std::format(
        "HTTP/1.1 {} {}\r\n"
        "Content-Type: {}\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "{}\r\n{}",
        status, reason, content_type, body.size(), extra_headers, body);
}

// ---------------------------------------------------------------------------
// Static file helper
// ---------------------------------------------------------------------------

// Read a file from disk and send it as an HTTP response.
// path is relative to the process working directory (run server from pset4/).
cot::task<> serve_static_file(const cot::fd& f, const char* path,
                               std::string_view ct) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        co_await write_all(f, http_response(404, "Not Found", "text/plain",
                                            "File not found\n"));
        co_return;
    }
    std::string body(std::istreambuf_iterator<char>(file), {});
    co_await write_all(f, http_response(200, "OK", ct, body));
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

// GET /doc/<id> -> {"text":"...","version":N}
cot::task<> handle_get_doc(const cot::fd& f, const http_paxos_bridge&) {
    std::string body = std::format("{{\"text\":\"{}\",\"version\":{}}}",
                                    json_escape(g_doc_cache.text_), g_doc_cache.version_);
    co_await write_all(f, http_response(200, "OK", "application/json", body));
}

// POST /doc/<id>/op  body: {"type":"I","pos":N,"text":"..","client_id":"..","seq":N}
//                          {"type":"D","pos":N,"len":N,"client_id":"..","seq":N}
cot::task<> handle_post_op(const cot::fd& f, const http_paxos_bridge& bridge,
                           std::string body) {
    auto started = cot::steady_now();
    auto type_opt = json_get_string(body, "type");
    auto pos_opt = json_get_int(body, "pos");
    auto cid_opt = json_get_string(body, "client_id");
    auto seq_opt = json_get_int(body, "seq");
    if (!type_opt || !pos_opt || !cid_opt || !seq_opt) {
        co_await write_all(f, http_response(
            400, "Bad Request", "application/json",
            "{\"error\":\"missing field (type/pos/client_id/seq)\"}"));
        co_return;
    }
    if (*pos_opt < 0 || *seq_opt < 0) {
        co_await write_all(f, http_response(
            400, "Bad Request", "application/json",
            "{\"error\":\"pos and seq must be >= 0\"}"));
        co_return;
    }

    collab::doc_op op;
    if (*type_opt == "I") {
        auto text_opt = json_get_string(body, "text");
        if (!text_opt) {
            co_await write_all(f, http_response(
                400, "Bad Request", "application/json",
                "{\"error\":\"missing text for insert\"}"));
            co_return;
        }
        op = collab::insert_op{static_cast<size_t>(*pos_opt), std::move(*text_opt)};
    } else if (*type_opt == "D") {
        auto len_opt = json_get_int(body, "len");
        if (!len_opt || *len_opt < 0) {
            co_await write_all(f, http_response(
                400, "Bad Request", "application/json",
                "{\"error\":\"missing/invalid len for delete\"}"));
            co_return;
        }
        op = collab::delete_op{static_cast<size_t>(*pos_opt),
                               static_cast<size_t>(*len_opt)};
    } else {
        co_await write_all(f, http_response(
            400, "Bad Request", "application/json",
            "{\"error\":\"type must be 'I' or 'D'\"}"));
        co_return;
    }

    uint64_t hashed_cid = hash_client_id(*cid_opt);
    std::string key = collab::op_key(bridge.doc_id, hashed_cid,
                                     static_cast<uint64_t>(*seq_opt));
    std::string value = collab::serialize(op);

    pancy::version_type version = co_await bridge.client->submit_put(key, value);
    if (version == 0) {
        co_await write_all(f, http_response(
            504, "Gateway Timeout", "application/json",
            "{\"error\":\"paxos commit timed out\"}"));
        co_return;
    }
    g_doc_cache.note_committed(collab::committed_op{
        version, hashed_cid, static_cast<uint64_t>(*seq_opt), op
    });
    std::string resp_body = std::format("{{\"version\":{}}}", version);
    co_await write_all(f, http_response(200, "OK", "application/json", resp_body));
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        cot::steady_now() - started);
    std::print("POST /doc/{}/op commit version={} latency_ms={}\n",
               bridge.doc_id, version, elapsed_ms.count());
}

// GET /doc/<id>/stream  ->  text/event-stream forever
//
// Polls the leader's pancydb every 250 ms; emits any committed ops with
// version > last_seen as `id: N\nevent: op\ndata: {...}\n\n`. Heartbeats
// every 5 s (keep browsers and proxies from treating the idle connection
// as dead). The `retry: 1000` directive tells the browser to reconnect
// after 1 s on error. `id:` fields let the browser send Last-Event-ID
// when reconnecting so we can resume from the right version.
cot::task<> handle_stream(const cot::fd& f, const http_paxos_bridge&,
                           pancy::version_type resume_from) {
    std::string head = std::format(
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/event-stream\r\n"
        "Cache-Control: no-cache\r\n"
        "Connection: keep-alive\r\n"
        "Access-Control-Allow-Origin: *\r\n"
        "\r\n"
        "retry: 1000\n\n");   // tell browser: 1 s reconnect delay
    if (!co_await write_all(f, head)) co_return;

    pancy::version_type last_seen = resume_from;
    auto last_heartbeat = cot::steady_now();
    while (true) {
        for (const auto& c : g_doc_cache.ops_) {
            if (c.version <= last_seen) continue;
            std::string op_kind;
            std::string op_payload;
            if (auto* in = std::get_if<collab::insert_op>(&c.op)) {
                op_kind = "I";
                op_payload = std::format(
                    "\"pos\":{},\"text\":\"{}\"", in->pos, json_escape(in->text));
            } else {
                auto& dl = std::get<collab::delete_op>(c.op);
                op_kind = "D";
                op_payload = std::format("\"pos\":{},\"len\":{}", dl.pos, dl.len);
            }
            std::string ev = std::format(
                "id: {}\n"
                "event: op\n"
                "data: {{\"version\":{},\"client_id\":{},\"seq\":{},"
                "\"op\":{{\"type\":\"{}\",{}}}}}\n\n",
                c.version,
                c.version, c.client_id, c.client_seq, op_kind, op_payload);
            if (!co_await write_all(f, ev)) co_return;
            last_seen = c.version;
        }

        auto now = cot::steady_now();
        if (now - last_heartbeat >= 5s) {   // 5 s — well inside browser idle limits
            if (!co_await write_all(f, std::string("event: ping\ndata: {}\n\n"))) {
                co_return;
            }
            last_heartbeat = now;
        }
        co_await cot::after(250ms);
    }
}

cot::task<> poll_doc_cache(http_paxos_bridge bridge) {
    while (true) {
        g_doc_cache.refresh_from_db(bridge.current_db(), bridge.doc_id);
        co_await cot::after(100ms);
    }
}

// One TCP connection -> dispatch one request, then close.
cot::task<> handle_connection(cot::fd conn, http_paxos_bridge bridge) {
    std::string head_buf = co_await read_request_head(conn);
    if (head_buf.empty()) co_return;

    request_line rl = parse_request_line(head_buf);
    if (rl.method.empty()) co_return;

    std::string body = head_buf.substr(rl.body_offset);
    std::string head_only = head_buf.substr(0, rl.body_offset);

    size_t want = 0;
    std::string cl = find_header(head_only, "content-length");
    if (!cl.empty()) {
        try { want = static_cast<size_t>(std::stoul(cl)); }
        catch (...) { want = 0; }
    }
    if (body.size() < want) {
        if (!co_await read_body(conn, body, want)) co_return;
    }

    const std::string doc_prefix = "/doc/" + bridge.doc_id;
    if (rl.method == "GET" && rl.path == doc_prefix) {
        co_await handle_get_doc(conn, bridge);
    } else if (rl.method == "POST" && rl.path == doc_prefix + "/op") {
        co_await handle_post_op(conn, bridge, std::move(body));
    } else if (rl.method == "GET" && rl.path == doc_prefix + "/stream") {
        // Resume from Last-Event-ID if the browser is reconnecting.
        pancy::version_type resume_from = 0;
        {
            std::string lei = find_header(head_only, "last-event-id");
            if (!lei.empty()) {
                pancy::version_type v = 0;
                auto [ptr, ec] = std::from_chars(lei.data(), lei.data() + lei.size(), v);
                if (ec == std::errc()) resume_from = v;
            }
        }
        co_await handle_stream(conn, bridge, resume_from);
    } else if (rl.method == "GET" && rl.path == "/docs") {
        std::string body_out = std::format("[\"{}\"]", json_escape(bridge.doc_id));
        co_await write_all(conn, http_response(200, "OK",
                                               "application/json", body_out));
    } else if (rl.method == "GET" &&
               (rl.path == "/" || rl.path == "/editor.html")) {
        co_await serve_static_file(conn, "static/editor.html",
                                   "text/html; charset=utf-8");
    } else if (rl.method == "GET" && rl.path == "/editor.js") {
        co_await serve_static_file(conn, "static/editor.js",
                                   "application/javascript; charset=utf-8");
    } else if (rl.method == "OPTIONS") {
        // CORS preflight — browsers send this before cross-origin POSTs.
        co_await write_all(conn, std::string(
            "HTTP/1.1 204 No Content\r\n"
            "Access-Control-Allow-Origin: *\r\n"
            "Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n"
            "Access-Control-Allow-Headers: Content-Type\r\n"
            "Access-Control-Max-Age: 86400\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n"
            "\r\n"));
    } else if (rl.method == "POST" && rl.path.rfind("/admin/fail/", 0) == 0) {
        if (!bridge.fail_replica) {
            co_await write_all(conn, http_response(
                501, "Not Implemented", "application/json",
                "{\"error\":\"fail endpoint not wired\"}"));
            co_return;
        }
        size_t rid = 0;
        std::string_view suffix(rl.path);
        suffix.remove_prefix(std::string_view("/admin/fail/").size());
        auto [ptr, ec] = std::from_chars(suffix.data(), suffix.data() + suffix.size(), rid);
        if (ec != std::errc() || ptr != suffix.data() + suffix.size()) {
            co_await write_all(conn, http_response(
                400, "Bad Request", "application/json",
                "{\"error\":\"replica id must be numeric\"}"));
            co_return;
        }
        bool ok = bridge.fail_replica(rid);
        if (!ok) {
            co_await write_all(conn, http_response(
                400, "Bad Request", "application/json",
                "{\"error\":\"invalid replica id\"}"));
            co_return;
        }
        std::print("Admin fail: replica {}\n", rid);
        co_await write_all(conn, http_response(
            200, "OK", "application/json",
            std::format("{{\"failed_replica\":{}}}", rid)));
    } else {
        co_await write_all(conn, http_response(404, "Not Found",
                                               "application/json",
                                               "{\"error\":\"no route\"}"));
    }
}

} // namespace

// ---------------------------------------------------------------------------
// run_http_server
// ---------------------------------------------------------------------------

cot::task<> run_http_server(uint16_t port, http_paxos_bridge bridge) {
    cot::fd listen_fd;
    try {
        listen_fd = co_await cot::tcp_listen(std::format("0.0.0.0:{}", port));
    } catch (const std::exception& e) {
        std::print(std::cerr, "HTTP listen on port {} failed: {}\n", port, e.what());
        co_return;
    }
    g_doc_cache.refresh_from_db(bridge.current_db(), bridge.doc_id);
    poll_doc_cache(bridge).detach();
    while (true) {
        cot::fd conn;
        try {
            conn = co_await cot::tcp_accept(listen_fd);
        } catch (const std::exception& e) {
            std::print(std::cerr, "HTTP accept failed: {}\n", e.what());
            continue;
        }
        // Detach a per-connection task so the accept loop keeps running.
        handle_connection(std::move(conn), bridge).detach();
    }
}

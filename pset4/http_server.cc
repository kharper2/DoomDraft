#include "http_server.hh"

#include "cotamer/io.hh"
#include "doc_ops.hh"
#include "doc_state.hh"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <format>
#include <fstream>
#include <iterator>
#include <map>
#include <print>
#include <set>
#include <string>
#include <string_view>
#include <utility>

namespace cot = cotamer;
using namespace std::chrono_literals;

namespace {
// Global counters for log correlation. Each accepted TCP connection gets a
// monotonic conn_id; each poll_doc_cache tick gets a monotonic tick_id.
std::atomic<uint64_t> g_next_conn_id{1};
std::atomic<uint64_t> g_next_tick_id{1};
} // namespace

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
    std::print(stderr,
               "[DoomDraft server] paxos submit begin key={} value=\"{}\" serial={} start_replica={} leader={}\n",
               key, value, serial, replica, leader_);
    for (int tries = 0; tries < kMaxAttempts; ++tries) {
        std::print(stderr,
                   "[DoomDraft server] paxos submit try={} key={} serial={} replica={}\n",
                   tries, key, serial, replica);
        co_await send_request<pancy::put_request>(replica, serial, key, value);
        auto resp = co_await cot::attempt(
            receive_response<pancy::put_response>(replica, serial),
            cot::after(2s)
        );
        if (resp && resp->errcode == pancy::errc::ok) {
            leader_ = replica; // sticky leader
            std::print(stderr,
                       "[DoomDraft server] paxos submit ok key={} serial={} replica={} version={} previous_version={}\n",
                       key, serial, replica, resp->version, resp->previous_version);
            co_return resp->version;
        }
        if (resp) {
            std::print(stderr,
                       "[DoomDraft server] paxos submit non-ok key={} serial={} replica={} errcode={} version={}\n",
                       key, serial, replica, static_cast<int>(resp->errcode),
                       resp->version);
        } else {
            std::print(stderr,
                       "[DoomDraft server] paxos submit timeout key={} serial={} replica={}\n",
                       key, serial, replica);
        }
        // No response, or non-ok: try a different replica next round.
        // receive_response updates `replica` in-place on redirect.
        if (!resp) {
            replica = (replica + 1) % nreplicas();
        }
    }
    std::print(stderr,
               "[DoomDraft server] paxos submit failed key={} serial={} attempts={}\n",
               key, serial, kMaxAttempts);
    co_return 0;
}

// ---------------------------------------------------------------------------
// HTTP wire helpers
// ---------------------------------------------------------------------------

namespace {

constexpr std::string_view kDocsRegistryKey = "docs/registry";
std::string json_escape(std::string_view s);
std::optional<std::string> json_get_string(std::string_view body, std::string_view key);
std::optional<int64_t> json_get_int(std::string_view body, std::string_view key);
std::string preview_text(std::string_view s);
std::string op_debug_string(const collab::doc_op& op);
std::string text_debug_string(std::string_view text);
std::string safe_log_component(std::string_view s);

std::string docs_registry_json(const std::set<std::string>& docs) {
    std::string out = "[";
    bool first = true;
    for (const auto& d : docs) {
        if (!first) out += ",";
        first = false;
        out += std::format("\"{}\"", json_escape(d));
    }
    out += "]";
    return out;
}

bool valid_doc_id(std::string_view doc_id) {
    if (doc_id.empty()) return false;
    for (char c : doc_id) {
        const bool ok = std::isalnum(static_cast<unsigned char>(c))
                        || c == '_' || c == '-' || c == '.';
        if (!ok) return false;
    }
    return true;
}

std::set<std::string> parse_docs_registry(std::string_view body) {
    std::set<std::string> out;
    size_t i = 0;
    while (i < body.size()) {
        if (body[i] == '"') {
            size_t j = i + 1;
            while (j < body.size() && body[j] != '"') ++j;
            if (j < body.size() && j > i + 1) {
                std::string d(body.substr(i + 1, j - i - 1));
                if (valid_doc_id(d)) out.insert(std::move(d));
            }
            i = j + 1;
        } else {
            ++i;
        }
    }
    return out;
}

struct cursor_entry {
    pancy::version_type version = 0;
    std::string client_id;
    int64_t pos = 0;
};

struct per_doc_cache {
    std::vector<collab::committed_op> ops_;
    std::map<uint64_t, cursor_entry> cursors_;
    std::map<uint64_t, std::string> client_labels_;
    std::string text_;
    pancy::version_type version_ = 0;
    uint64_t epoch_ = 0;
    uint64_t cursor_epoch_ = 0;

    void refresh_ops_from_db(const pancy::pancydb& db, std::string_view doc_id) {
        std::vector<collab::committed_op> fresh = collab::read_ops(db, doc_id);
        const pancy::version_type fresh_version = fresh.empty() ? 0 : fresh.back().version;
        if (fresh.size() != ops_.size()
            || (!fresh.empty() && fresh_version != version_)) {
            std::print(stderr,
                       "[DoomDraft server] cache refresh doc={} fresh_ops={} cached_ops={} fresh_v={} cached_v={} cached_text={}\n",
                       doc_id, fresh.size(), ops_.size(), fresh_version, version_,
                       text_debug_string(text_));
        }
        if (fresh.empty()) {
            if (!ops_.empty()) {
                std::print(stderr,
                           "[DoomDraft server] cache reset empty doc={} old_ops={} old_text={}\n",
                           doc_id, ops_.size(), text_debug_string(text_));
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
                auto label_it = client_labels_.find(fresh[i].client_id);
                if (label_it != client_labels_.end()) {
                    fresh[i].client_id_label = label_it->second;
                }
                const std::string before = text_;
                collab::apply_op(text_, fresh[i].op);
                std::print(stderr,
                           "[DoomDraft server] cache append doc={} idx={} version={} cid_hash={} seq={} op={} before={} after={}\n",
                           doc_id, i, fresh[i].version, fresh[i].client_id,
                           fresh[i].client_seq, op_debug_string(fresh[i].op),
                           text_debug_string(before), text_debug_string(text_));
                ops_.push_back(fresh[i]);
            }
            if (fresh_version != version_) {
                version_ = fresh_version;
                ++epoch_;
            }
            return;
        }

        // Slow path: recompute full state when prefix changed unexpectedly.
        // Preserve client_id_label strings learned via note_committed — the DB
        // only stores the numeric hash, so without this rebuild would flip the
        // SSE-emitted client_id from "c_..." to a uint64.
        std::map<std::pair<uint64_t, uint64_t>, std::string> saved_labels;
        for (const auto& o : ops_) {
            if (!o.client_id_label.empty()) {
                saved_labels.emplace(std::make_pair(o.client_id, o.client_seq),
                                     o.client_id_label);
            }
        }
        ops_ = std::move(fresh);
        for (auto& o : ops_) {
            auto it = saved_labels.find({o.client_id, o.client_seq});
            if (it != saved_labels.end()) o.client_id_label = std::move(it->second);
        }
        text_ = collab::reconstruct(ops_);
        std::print(stderr,
                   "[DoomDraft server] cache full rebuild doc={} ops={} version={} text={}\n",
                   doc_id, ops_.size(), fresh_version, text_debug_string(text_));
        version_ = fresh_version;
        ++epoch_;
    }

    void refresh_cursors_from_db(const pancy::pancydb& db, std::string_view doc_id) {
        std::string prefix = std::format("doc/{}/cursor/", doc_id);
        std::map<uint64_t, cursor_entry> fresh;
        for (auto it = db.begin(); it != db.end(); ++it) {
            const auto& key = it->first;
            if (key.size() <= prefix.size() || key.compare(0, prefix.size(), prefix) != 0) {
                continue;
            }
            std::string_view suffix(key);
            suffix.remove_prefix(prefix.size());
            uint64_t cid_hash = 0;
            auto [ptr, ec] = std::from_chars(suffix.data(), suffix.data() + suffix.size(), cid_hash);
            if (ec != std::errc() || ptr != suffix.data() + suffix.size()) {
                continue;
            }
            const auto& cell = it->second;
            auto client_id = json_get_string(cell.value, "client_id");
            auto pos = json_get_int(cell.value, "pos");
            if (!client_id || !pos) continue;
            fresh[cid_hash] = cursor_entry{cell.version, std::move(*client_id), *pos};
        }
        bool changed = fresh.size() != cursors_.size();
        if (!changed) {
            auto it_a = fresh.begin();
            auto it_b = cursors_.begin();
            while (it_a != fresh.end()) {
                if (it_a->first != it_b->first
                    || it_a->second.version != it_b->second.version
                    || it_a->second.client_id != it_b->second.client_id
                    || it_a->second.pos != it_b->second.pos) {
                    changed = true;
                    break;
                }
                ++it_a;
                ++it_b;
            }
        }
        if (changed) {
            std::print(stderr,
                       "[DoomDraft server] cursor cache changed doc={} old={} fresh={}\n",
                       doc_id, cursors_.size(), fresh.size());
            cursors_ = std::move(fresh);
            ++cursor_epoch_;
        }
    }

    void note_committed(const collab::committed_op& c) {
        if (!c.client_id_label.empty()) {
            client_labels_[c.client_id] = c.client_id_label;
        }
        std::print(stderr,
                   "[DoomDraft server] note_committed incoming version={} cid_hash={} cid_label={} seq={} op={} cache_v={} cache_ops={} cache_text={}\n",
                   c.version, c.client_id,
                   c.client_id_label.empty() ? "(none)" : c.client_id_label,
                   c.client_seq, op_debug_string(c.op), version_, ops_.size(),
                   text_debug_string(text_));
        // poll_doc_cache may have already inserted this op from the DB without
        // a client_id_label. Upgrade the existing entry instead of appending a
        // duplicate row (which would cause SSE to emit the same op twice — once
        // with the numeric hash, once with the string label).
        for (auto& existing : ops_) {
            if (existing.client_id == c.client_id
                && existing.client_seq == c.client_seq) {
                if (existing.client_id_label.empty() && !c.client_id_label.empty()) {
                    existing.client_id_label = c.client_id_label;
                    std::print(stderr,
                               "[DoomDraft server] note_committed upgraded label version={} cid_hash={} seq={} label={}\n",
                               existing.version, existing.client_id,
                               existing.client_seq, existing.client_id_label);
                } else {
                    std::print(stderr,
                               "[DoomDraft server] note_committed duplicate ignored existing_version={} incoming_version={} cid_hash={} seq={}\n",
                               existing.version, c.version, c.client_id, c.client_seq);
                }
                return;
            }
        }
        if (c.version > version_) {
            const std::string before = text_;
            collab::apply_op(text_, c.op);
            ops_.push_back(c);
            version_ = c.version;
            ++epoch_;
            std::print(stderr,
                       "[DoomDraft server] note_committed applied version={} cid_hash={} seq={} before={} after={} epoch={}\n",
                       c.version, c.client_id, c.client_seq,
                       text_debug_string(before), text_debug_string(text_), epoch_);
        } else {
            std::print(stderr,
                       "[DoomDraft server] note_committed stale ignored incoming_version={} cache_v={} cid_hash={} seq={}\n",
                       c.version, version_, c.client_id, c.client_seq);
        }
    }

    void note_client_label(uint64_t cid_hash, std::string_view label) {
        if (label.empty()) return;
        client_labels_[cid_hash] = std::string(label);
        for (auto& op : ops_) {
            if (op.client_id == cid_hash && op.client_id_label.empty()) {
                op.client_id_label = std::string(label);
            }
        }
        std::print(stderr,
                   "[DoomDraft server] remembered client label cid_hash={} label={} known_labels={}\n",
                   cid_hash, label, client_labels_.size());
    }
};

struct server_cache_state {
    std::set<std::string> known_docs_;
    std::map<std::string, per_doc_cache> by_doc_;
    uint64_t docs_epoch_ = 0;

    per_doc_cache& doc(std::string_view doc_id) {
        return by_doc_[std::string(doc_id)];
    }

    void ensure_doc(std::string_view doc_id) {
        if (known_docs_.insert(std::string(doc_id)).second) {
            ++docs_epoch_;
        }
        by_doc_.try_emplace(std::string(doc_id));
    }
};

server_cache_state g_state;

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
        auto r = co_await cot::write_once(f, s.data() + off, s.size() - off);
        if (!r) co_return false;
        if (*r == 0) co_return false;
        off += *r;
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
        auto r = co_await cot::read_once(f, tmp, sizeof(tmp));
        if (!r) break;
        if (*r == 0) break;
        buf.append(tmp, *r);
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
        auto r = co_await cot::read_once(f, tmp, sizeof(tmp));
        if (!r) co_return false;
        if (*r == 0) co_return false;
        body.append(tmp, *r);
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
            case '{':  out += "\\u007b"; break;
            case '}':  out += "\\u007d"; break;
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

// One SSE op event: single-line `data:` JSON (no std::format on user text).
std::string sse_op_frame(const collab::committed_op& c) {
    std::string op_inner;
    if (auto* in = std::get_if<collab::insert_op>(&c.op)) {
        op_inner.reserve(24 + in->text.size() * 2);
        op_inner += "\"type\":\"I\",\"pos\":";
        op_inner += std::to_string(in->pos);
        op_inner += ",\"text\":\"";
        op_inner += json_escape(in->text);
        op_inner += '\"';
    } else {
        const auto& dl = std::get<collab::delete_op>(c.op);
        op_inner = "\"type\":\"D\",\"pos\":";
        op_inner += std::to_string(dl.pos);
        op_inner += ",\"len\":";
        op_inner += std::to_string(dl.len);
    }
    std::string cid_json =
        c.client_id_label.empty()
            ? std::to_string(c.client_id)
            : (std::string("\"") + json_escape(c.client_id_label) + '\"');

    std::string data;
    data.reserve(64 + op_inner.size());
    data += "{\"version\":";
    data += std::to_string(c.version);
    data += ",\"client_id\":";
    data += cid_json;
    data += ",\"seq\":";
    data += std::to_string(c.client_seq);
    data += ",\"op\":{";
    data += op_inner;
    data += "}}";

    std::string out;
    out.reserve(32 + data.size());
    out += "id: ";
    out += std::to_string(c.version);
    out += "\nevent: op\ndata: ";
    out += data;
    out += "\n\n";
    return out;
}

std::string sse_cursor_frame(const cursor_entry& cur) {
    std::string body = "{\"client_id\":\"";
    body += json_escape(cur.client_id);
    body += "\",\"pos\":";
    body += std::to_string(cur.pos);
    body += ",\"version\":";
    body += std::to_string(cur.version);
    body += '}';
    std::string out = "event: cursor\ndata: ";
    out += body;
    out += "\n\n";
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

uint64_t hash_text(std::string_view s) {
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : s) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h;
}

std::string preview_text(std::string_view s) {
    std::string out;
    size_t n = std::min<size_t>(s.size(), 80);
    out.reserve(n + 8);
    for (size_t i = 0; i != n; ++i) {
        char c = s[i];
        if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else if (c == '\t') out += "\\t";
        else out.push_back(c);
    }
    if (s.size() > n) out += "...";
    return out;
}

std::string text_debug_string(std::string_view text) {
    return std::format("len={} hash={:016x} preview=\"{}\"",
                       text.size(), hash_text(text), preview_text(text));
}

std::string op_debug_string(const collab::doc_op& op) {
    if (const auto* in = std::get_if<collab::insert_op>(&op)) {
        return std::format("I@{}+{} \"{}\"", in->pos, in->text.size(),
                           preview_text(in->text));
    }
    const auto& dl = std::get<collab::delete_op>(op);
    return std::format("D@{}x{}", dl.pos, dl.len);
}

std::string safe_log_component(std::string_view s) {
    std::string out;
    out.reserve(std::min<size_t>(s.size(), 80));
    for (char c : s) {
        const bool ok = std::isalnum(static_cast<unsigned char>(c))
                        || c == '_' || c == '-' || c == '.';
        out.push_back(ok ? c : '_');
        if (out.size() >= 80) break;
    }
    return out.empty() ? "unknown" : out;
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

cot::task<> handle_get_doc_id(const cot::fd& f, std::string_view doc_id) {
    auto& cache = g_state.doc(doc_id);
    std::print(stderr,
               "[DoomDraft server] GET doc={} cache_v={} ops={} epoch={} text={}\n",
               doc_id, cache.version_, cache.ops_.size(), cache.epoch_,
               text_debug_string(cache.text_));
    std::string body = std::format("{{\"text\":\"{}\",\"version\":{}}}",
                                    json_escape(cache.text_), cache.version_);
    co_await write_all(f, http_response(200, "OK", "application/json", body));
}

// POST /doc/<id>/op  body: {"type":"I","pos":N,"text":"..","client_id":"..","seq":N}
//                          {"type":"D","pos":N,"len":N,"client_id":"..","seq":N}
cot::task<> handle_post_op_doc(const cot::fd& f, const http_paxos_bridge& bridge,
                               std::string_view doc_id, std::string body) {
    auto started = cot::steady_now();
    std::print(stderr,
               "[DoomDraft server] POST op begin doc={} raw_body={}\n",
               doc_id, body);
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
    g_state.doc(doc_id).note_client_label(hashed_cid, *cid_opt);
    std::string key = collab::op_key(doc_id, hashed_cid,
                                     static_cast<uint64_t>(*seq_opt));
    std::string value = collab::serialize(op);
    {
        auto& cache = g_state.doc(doc_id);
        std::print(stderr,
                   "[DoomDraft server] POST op parsed doc={} cid_label={} cid_hash={} seq={} key={} op={} serialized=\"{}\" cache_v={} cache_ops={} cache_text={}\n",
                   doc_id, *cid_opt, hashed_cid, *seq_opt, key,
                   op_debug_string(op), value, cache.version_, cache.ops_.size(),
                   text_debug_string(cache.text_));
    }

    pancy::version_type version = co_await bridge.client->submit_put(key, value);
    if (version == 0) {
        std::print(stderr,
                   "[DoomDraft server] POST op timeout doc={} cid_label={} cid_hash={} seq={} key={} op={}\n",
                   doc_id, *cid_opt, hashed_cid, *seq_opt, key,
                   op_debug_string(op));
        co_await write_all(f, http_response(
            504, "Gateway Timeout", "application/json",
            "{\"error\":\"paxos commit timed out\"}"));
        co_return;
    }
    auto& cache = g_state.doc(doc_id);
    collab::committed_op co{version, hashed_cid,
                            static_cast<uint64_t>(*seq_opt), std::move(op), {}};
    co.client_id_label = *cid_opt;
    cache.note_committed(co);
    std::string resp_body = std::format("{{\"version\":{}}}", version);
    co_await write_all(f, http_response(200, "OK", "application/json", resp_body));
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        cot::steady_now() - started);
    std::print(stderr,
               "[DoomDraft server] POST op done doc={} version={} cid_label={} cid_hash={} seq={} latency_ms={} cache_text={}\n",
               doc_id, version, *cid_opt, hashed_cid, *seq_opt,
               elapsed_ms.count(), text_debug_string(cache.text_));
}

// GET /doc/<id>/stream  ->  text/event-stream forever
//
// Polls the leader's pancydb every 250 ms; emits any committed ops with
// version > last_seen as `id: N\nevent: op\ndata: {...}\n\n`. Heartbeats
// every 5 s (keep browsers and proxies from treating the idle connection
// as dead). The `retry: 1000` directive tells the browser to reconnect
// after 1 s on error. `id:` fields let the browser send Last-Event-ID
// when reconnecting so we can resume from the right version.
cot::task<> handle_stream_doc(const cot::fd& f, std::string_view doc_id,
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
    uint64_t last_cursor_epoch = 0;
    auto last_heartbeat = cot::steady_now();
    std::print(stderr,
               "[DoomDraft server] SSE stream open doc={} resume_from={}\n",
               doc_id, resume_from);
    while (true) {
        auto& cache = g_state.doc(doc_id);
        // Copy before iterating: POST /op (note_committed), POST /cursor, and
        // poll_doc_cache can mutate ops_/cursors_ while this coroutine is
        // suspended in write_all/after. Range-for on a live vector/map is UB
        // if another task reallocates — manifests as dropped SSE / "stream
        // error" in the browser as soon as commits race the stream loop.
        const std::vector<collab::committed_op> ops_snap = cache.ops_;
        if (!ops_snap.empty() && ops_snap.back().version > last_seen) {
            std::print(stderr,
                       "[DoomDraft server] SSE scan doc={} last_seen={} cache_v={} snap_ops={} cache_text={}\n",
                       doc_id, last_seen, cache.version_, ops_snap.size(),
                       text_debug_string(cache.text_));
        }
        for (const auto& c : ops_snap) {
            if (c.version <= last_seen) continue;
            const std::string ev = sse_op_frame(c);
            std::print(stderr,
                       "[DoomDraft server] SSE emit op doc={} version={} cid_hash={} cid_label={} seq={} op={} last_seen_before={}\n",
                       doc_id, c.version, c.client_id,
                       c.client_id_label.empty() ? "(none)" : c.client_id_label,
                       c.client_seq, op_debug_string(c.op), last_seen);
            if (!co_await write_all(f, ev)) {
                std::print(std::cerr, "SSE op write failed (doc {})\n", doc_id);
                co_return;
            }
            last_seen = c.version;
        }
        const uint64_t epoch_snap = cache.cursor_epoch_;
        const std::map<uint64_t, cursor_entry> cursors_snap = cache.cursors_;
        if (epoch_snap != last_cursor_epoch) {
            std::print(stderr,
                       "[DoomDraft server] SSE cursor epoch doc={} old_epoch={} new_epoch={} cursors={}\n",
                       doc_id, last_cursor_epoch, epoch_snap, cursors_snap.size());
            for (const auto& p : cursors_snap) {
                const cursor_entry& cur = p.second;
                const std::string cev = sse_cursor_frame(cur);
                std::print(stderr,
                           "[DoomDraft server] SSE emit cursor doc={} cid_hash={} cid_label={} pos={} version={}\n",
                           doc_id, p.first, cur.client_id, cur.pos, cur.version);
                if (!co_await write_all(f, cev)) {
                    std::print(std::cerr, "SSE cursor write failed (doc {})\n", doc_id);
                    co_return;
                }
            }
            last_cursor_epoch = epoch_snap;
        }

        auto now = cot::steady_now();
        if (now - last_heartbeat >= 5s) {   // 5 s — well inside browser idle limits
            std::print(stderr,
                       "[DoomDraft server] SSE heartbeat doc={} last_seen={} cache_v={}\n",
                       doc_id, last_seen, cache.version_);
            if (!co_await write_all(f, std::string("event: ping\ndata: {}\n\n"))) {
                co_return;
            }
            last_heartbeat = now;
        }
        co_await cot::after(25ms);
    }
}

cot::task<> handle_post_cursor_doc(const cot::fd& f, const http_paxos_bridge& bridge,
                                   std::string_view doc_id, std::string body) {
    std::print(stderr,
               "[DoomDraft server] POST cursor begin doc={} raw_body={}\n",
               doc_id, body);
    auto cid = json_get_string(body, "client_id");
    auto pos = json_get_int(body, "pos");
    if (!cid || !pos) {
        co_await write_all(f, http_response(
            400, "Bad Request", "application/json",
            "{\"error\":\"missing field (client_id/pos)\"}"));
        co_return;
    }
    if (*pos < 0) {
        co_await write_all(f, http_response(
            400, "Bad Request", "application/json",
            "{\"error\":\"pos must be >= 0\"}"));
        co_return;
    }
    uint64_t cid_hash = hash_client_id(*cid);
    g_state.doc(doc_id).note_client_label(cid_hash, *cid);
    std::string key = std::format("doc/{}/cursor/{}", doc_id, cid_hash);
    std::string val = std::format("{{\"client_id\":\"{}\",\"pos\":{}}}",
                                  json_escape(*cid), *pos);
    std::print(stderr,
               "[DoomDraft server] POST cursor parsed doc={} cid_label={} cid_hash={} pos={} key={} value={}\n",
               doc_id, *cid, cid_hash, *pos, key, val);
    pancy::version_type version = co_await bridge.client->submit_put(key, val);
    if (version == 0) {
        std::print(stderr,
                   "[DoomDraft server] POST cursor timeout doc={} cid_label={} cid_hash={} pos={}\n",
                   doc_id, *cid, cid_hash, *pos);
        co_await write_all(f, http_response(
            504, "Gateway Timeout", "application/json",
            "{\"error\":\"paxos commit timed out\"}"));
        co_return;
    }
    g_state.doc(doc_id).cursors_[cid_hash] = cursor_entry{version, *cid, *pos};
    g_state.doc(doc_id).cursor_epoch_++;
    std::print(stderr,
               "[DoomDraft server] POST cursor done doc={} version={} cid_label={} cid_hash={} pos={} cursor_epoch={}\n",
               doc_id, version, *cid, cid_hash, *pos,
               g_state.doc(doc_id).cursor_epoch_);
    co_await write_all(f, http_response(
        200, "OK", "application/json",
        std::format("{{\"version\":{}}}", version)));
}

cot::task<> handle_post_debug_log(const cot::fd& f, std::string body) {
    auto cid = json_get_string(body, "clientId");
    if (!cid) cid = json_get_string(body, "client_id");
    auto doc = json_get_string(body, "doc");
    const std::string cid_part = safe_log_component(cid.value_or("unknown"));
    const std::string doc_part = safe_log_component(doc.value_or("unknown"));
    const std::string path = std::format("logs/browser-{}-{}.jsonl", doc_part, cid_part);
    std::ofstream out(path, std::ios::app);
    if (!out) {
        std::print(stderr,
                   "[DoomDraft server] debug log write failed path={} body={}\n",
                   path, body);
        co_await write_all(f, http_response(
            500, "Internal Server Error", "application/json",
            "{\"error\":\"could not open debug log\"}"));
        co_return;
    }
    out << body << '\n';
    std::print(stderr,
               "[DoomDraft server] browser debug logged path={} bytes={}\n",
               path, body.size());
    co_await write_all(f, http_response(
        204, "No Content", "application/json", ""));
}

cot::task<> poll_doc_cache(http_paxos_bridge bridge) {
    auto last_tick_end = cot::steady_now();
    while (true) {
        const uint64_t tick = g_next_tick_id.fetch_add(1);
        const auto tick_start = cot::steady_now();
        const auto gap_us = std::chrono::duration_cast<std::chrono::microseconds>(
            tick_start - last_tick_end).count();
        std::print(stderr,
                   "[DoomDraft server] poll tick={} begin gap_us={} known_docs={}\n",
                   tick, gap_us, g_state.known_docs_.size());

        const auto& db = bridge.current_db();
        if (auto vv = db.get(std::string(kDocsRegistryKey)); vv) {
            auto parsed = parse_docs_registry(vv->value);
            if (parsed != g_state.known_docs_) {
                std::print(stderr,
                           "[DoomDraft server] poll tick={} docs registry version={} value={} parsed_count={} known_count={}\n",
                           tick, vv->version, vv->value, parsed.size(),
                           g_state.known_docs_.size());
            }
            for (const auto& d : parsed) g_state.ensure_doc(d);
        }
        for (const auto& d : g_state.known_docs_) {
            auto& cache = g_state.doc(d);
            cache.refresh_ops_from_db(db, d);
            cache.refresh_cursors_from_db(db, d);
        }
        const auto tick_end = cot::steady_now();
        const auto work_us = std::chrono::duration_cast<std::chrono::microseconds>(
            tick_end - tick_start).count();
        std::print(stderr,
                   "[DoomDraft server] poll tick={} end work_us={}\n",
                   tick, work_us);
        last_tick_end = tick_end;
        co_await cot::after(100ms);
    }
}

std::optional<std::pair<std::string, std::string>> parse_doc_route(std::string_view path) {
    if (!path.starts_with("/doc/")) return std::nullopt;
    std::string_view rest = path.substr(std::string_view("/doc/").size());
    size_t slash = rest.find('/');
    if (slash == std::string_view::npos) {
        std::string doc(rest);
        if (!valid_doc_id(doc)) return std::nullopt;
        return {{doc, ""}};
    }
    std::string doc(rest.substr(0, slash));
    std::string sub(rest.substr(slash + 1));
    if (!valid_doc_id(doc)) return std::nullopt;
    return {{doc, sub}};
}

// RAII: fires when the coroutine frame is destroyed, regardless of which
// co_return path was taken. Gives every connection a paired open/close pair
// in the log.
struct conn_close_logger {
    uint64_t conn_id;
    std::chrono::steady_clock::time_point start;
    ~conn_close_logger() {
        auto dur_us = std::chrono::duration_cast<std::chrono::microseconds>(
            cot::steady_now() - start).count();
        std::print(stderr,
                   "[DoomDraft server] conn={} closed total_us={}\n",
                   conn_id, dur_us);
    }
};

// One TCP connection -> dispatch one request, then close.
cot::task<> handle_connection(cot::fd conn, http_paxos_bridge bridge, uint64_t conn_id) {
    const auto conn_start = cot::steady_now();
    conn_close_logger close_log{conn_id, conn_start};
    std::print(stderr,
               "[DoomDraft server] conn={} accepted\n", conn_id);
    std::string head_buf = co_await read_request_head(conn);
    if (head_buf.empty()) {
        std::print(stderr,
                   "[DoomDraft server] conn={} closed empty_head\n", conn_id);
        co_return;
    }

    request_line rl = parse_request_line(head_buf);
    if (rl.method.empty()) {
        std::print(stderr,
                   "[DoomDraft server] conn={} closed unparsable head_len={}\n",
                   conn_id, head_buf.size());
        co_return;
    }

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

    auto doc_route = parse_doc_route(rl.path);
    std::print(stderr,
               "[DoomDraft server] conn={} dispatch method={} path={} content_length={} body_read={}\n",
               conn_id, rl.method, rl.path, want, body.size());
    if (doc_route && rl.method == "GET" && doc_route->second.empty()) {
        g_state.ensure_doc(doc_route->first);
        co_await handle_get_doc_id(conn, doc_route->first);
    } else if (rl.method == "POST" && rl.path == "/debug/log") {
        co_await handle_post_debug_log(conn, std::move(body));
    } else if (doc_route && rl.method == "POST" && doc_route->second == "op") {
        g_state.ensure_doc(doc_route->first);
        co_await handle_post_op_doc(conn, bridge, doc_route->first, std::move(body));
    } else if (doc_route && rl.method == "POST" && doc_route->second == "cursor") {
        g_state.ensure_doc(doc_route->first);
        co_await handle_post_cursor_doc(conn, bridge, doc_route->first, std::move(body));
    } else if (doc_route && rl.method == "GET" && doc_route->second == "stream") {
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
        g_state.ensure_doc(doc_route->first);
        co_await handle_stream_doc(conn, doc_route->first, resume_from);
    } else if (rl.method == "GET" && rl.path == "/docs") {
        std::string body_out = docs_registry_json(g_state.known_docs_);
        std::print(stderr,
                   "[DoomDraft server] GET /docs known_count={} body={}\n",
                   g_state.known_docs_.size(), body_out);
        co_await write_all(conn, http_response(200, "OK",
                                               "application/json", body_out));
    } else if (rl.method == "POST" && rl.path == "/docs") {
        auto doc = json_get_string(body, "id");
        if (!doc || !valid_doc_id(*doc)) {
            co_await write_all(conn, http_response(
                400, "Bad Request", "application/json",
                "{\"error\":\"body must be {\\\"id\\\":\\\"doc-name\\\"}\"}"));
            co_return;
        }
        g_state.ensure_doc(*doc);
        std::string reg = docs_registry_json(g_state.known_docs_);
        std::print(stderr,
                   "[DoomDraft server] POST /docs id={} registry={}\n",
                   *doc, reg);
        pancy::version_type v = co_await bridge.client->submit_put(std::string(kDocsRegistryKey), reg);
        if (v == 0) {
            co_await write_all(conn, http_response(
                504, "Gateway Timeout", "application/json",
                "{\"error\":\"paxos commit timed out\"}"));
            co_return;
        }
        co_await write_all(conn, http_response(
            200, "OK", "application/json",
            std::format("{{\"ok\":true,\"version\":{},\"id\":\"{}\"}}", v, json_escape(*doc))));
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
    g_state.ensure_doc(bridge.doc_id);
    {
        const auto& db = bridge.current_db();
        if (auto vv = db.get(std::string(kDocsRegistryKey)); vv) {
            for (const auto& d : parse_docs_registry(vv->value)) {
                g_state.ensure_doc(d);
            }
        }
    }
    poll_doc_cache(bridge).detach();
    while (true) {
        cot::fd conn;
        try {
            conn = co_await cot::tcp_accept(listen_fd);
        } catch (const std::exception& e) {
            std::print(std::cerr, "HTTP accept failed: {}\n", e.what());
            continue;
        }
        const uint64_t conn_id = g_next_conn_id.fetch_add(1);
        // Detach a per-connection task so the accept loop keeps running.
        handle_connection(std::move(conn), bridge, conn_id).detach();
    }
}

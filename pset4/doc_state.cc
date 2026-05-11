#include "doc_state.hh"
#include <algorithm>
#include <charconv>
#include <stdexcept>
namespace collab {

std::string op_key(std::string_view doc_id, uint64_t client_id, uint64_t client_seq) {
    std::string s;
    s.reserve(8 + doc_id.size() + 24);
    s.append("doc/");
    s.append(doc_id);
    s.append("/op/");
    s.append(std::to_string(client_id));
    s.push_back('/');
    s.append(std::to_string(client_seq));
    return s;
}

static std::string doc_op_prefix(std::string_view doc_id) {
    std::string p;
    p.reserve(6 + doc_id.size() + 4);
    p.append("doc/");
    p.append(doc_id);
    p.append("/op/");
    return p;
}

// Parse "<client_id>/<seq>" after the prefix; both must be non-empty digit runs.
static bool parse_op_suffix(std::string_view suffix, uint64_t& client_id,
                            uint64_t& client_seq) {
    auto slash = suffix.find('/');
    if (slash == std::string_view::npos || slash == 0 || slash + 1 >= suffix.size()) {
        return false;
    }
    std::string_view a = suffix.substr(0, slash);
    std::string_view b = suffix.substr(slash + 1);
    if (b.find('/') != std::string_view::npos) {
        return false;
    }
    const char* a_begin = a.data();
    const char* a_end = a.data() + a.size();
    auto [ptr1, ec1] = std::from_chars(a_begin, a_end, client_id);
    if (ec1 != std::errc() || ptr1 != a_end) {
        return false;
    }
    const char* b_begin = b.data();
    const char* b_end = b.data() + b.size();
    auto [ptr2, ec2] = std::from_chars(b_begin, b_end, client_seq);
    if (ec2 != std::errc() || ptr2 != b_end) {
        return false;
    }
    return true;
}

std::vector<committed_op> read_ops(const pancy::pancydb& db, std::string_view doc_id) {
    const std::string prefix = doc_op_prefix(doc_id);
    std::vector<committed_op> out;
    for (auto it = db.begin(); it != db.end(); ++it) {
        const std::string& key = it->first;
        if (key.size() <= prefix.size() || key.compare(0, prefix.size(), prefix) != 0) {
            continue;
        }
        std::string_view suffix(key);
        suffix.remove_prefix(prefix.size());
        uint64_t cid = 0;
        uint64_t seq = 0;
        if (!parse_op_suffix(suffix, cid, seq)) {
            continue;
        }
        const pancy::vv& cell = it->second;
        if (cell.version == pancy::nonexistent_version) {
            continue;
        }
        try {
            doc_op op = deserialize(cell.value);
            out.push_back(
                committed_op{cell.version, cid, seq, std::move(op), {}});
        } catch (const std::invalid_argument&) {
            continue;
        }
    }
    std::sort(out.begin(), out.end(), [](const committed_op& x, const committed_op& y) {
        if (x.version != y.version) {
            return x.version < y.version;
        }
        if (x.client_id != y.client_id) {
            return x.client_id < y.client_id;
        }
        return x.client_seq < y.client_seq;
    });
    return out;
}

std::string reconstruct(std::span<const committed_op> ops) {
    std::string text;
    text.reserve(64);
    for (const committed_op& c : ops) {
        apply_op(text, c.op);
    }
    return text;
}

#include <cstdlib>
#include <print>

#define CHECK(expr, msg)                                                                          \
    do {                                                                                          \
        if (!(expr)) {                                                                            \
            std::print(stderr, "FAIL {}: {}\n", msg, #expr);                                      \
            std::exit(1);                                                                         \
        }                                                                                         \
    } while (0)

void test_doc_state() {
    // Empty DB → no ops, empty string
    {
        pancy::pancydb db;
        auto ops = read_ops(db, "main");
        CHECK(ops.empty(), "empty-db-ops");
        CHECK(reconstruct(ops).empty(), "empty-db-text");
    }

    // Single insert at "main"
    {
        pancy::pancydb db;
        const std::string k = op_key("main", 0, 0);
        db.put(k, serialize(insert_op{0, std::string("hello")}));
        auto ops = read_ops(db, "main");
        CHECK(ops.size() == 1, "single-op-count");
        CHECK(ops[0].client_id == 0, "single-op-cid");
        CHECK(ops[0].client_seq == 0, "single-op-seq");
        CHECK(ops[0].version >= 1, "single-op-version");
        std::string text = reconstruct(ops);
        CHECK(text == "hello", "single-insert-text");
    }

    // Two ops: versions decide order (insert then append)
    {
        pancy::pancydb db;
        db.put(op_key("main", 0, 0), serialize(insert_op{0, std::string("ab")}));
        db.put(op_key("main", 0, 1), serialize(insert_op{2, std::string("z")}));
        auto ops = read_ops(db, "main");
        CHECK(ops.size() == 2, "two-op-count");
        std::string text = reconstruct(ops);
        CHECK(text == "abz", "two-inserts-linear");
    }

    // Two clients, interleaved keys; order by version not key string
    {
        pancy::pancydb db;
        db.put(op_key("x", 1, 0), serialize(insert_op{0, std::string("B")}));
        db.put(op_key("x", 0, 0), serialize(insert_op{0, std::string("A")}));
        auto ops = read_ops(db, "x");
        CHECK(ops.size() == 2, "two-clients-count");
        // Lower version first: "B" then insert "A" at 0 → "AB"
        std::string text = reconstruct(ops);
        CHECK(text == "AB", "version-order");
    }

    // Delete after insert
    {
        pancy::pancydb db;
        db.put(op_key("main", 0, 0), serialize(insert_op{0, std::string("abcdef")}));
        db.put(op_key("main", 0, 1), serialize(delete_op{3, 2}));
        auto ops = read_ops(db, "main");
        std::string text = reconstruct(ops);
        CHECK(text == "abcf", "insert-then-delete");
    }

    // Wrong doc id prefix → no ops
    {
        pancy::pancydb db;
        db.put(op_key("other", 0, 0), serialize(insert_op{0, std::string("nope")}));
        auto ops = read_ops(db, "main");
        CHECK(ops.empty(), "wrong-doc-filter");
    }

    // Garbage value under valid key → skipped, not thrown
    {
        pancy::pancydb db;
        db.put(op_key("main", 0, 0), "not an op");
        auto ops = read_ops(db, "main");
        CHECK(ops.empty(), "bad-value-skipped");
    }

    // Extra keys without op prefix still fine
    {
        pancy::pancydb db;
        db.put("docs/registry", "[\"main\"]");
        db.put(op_key("main", 0, 0), serialize(insert_op{0, std::string("ok")}));
        auto ops = read_ops(db, "main");
        CHECK(ops.size() == 1, "mixed-keys");
        CHECK(reconstruct(ops) == "ok", "mixed-keys-text");
    }
}

} // namespace collab

#ifdef RUN_DOC_STATE_TESTS
int main() {
    std::print("Running doc_state tests...\n");
    collab::test_doc_state();
    std::print("All doc_state tests passed.\n");
    return 0;
}
#endif

#pragma once
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <variant>

// doc_ops.hh
//   Character-level insert/delete operations and Operational Transformation
//   for the collaborative editor. The OT engine is deliberately independent of
//   Paxos — it can be unit-tested standalone (see doc_ops.cc, RUN_DOC_OPS_TESTS).
//
// Serialization format (stored as PancyDB values):
//   Insert: "I <pos> <text>"   e.g. "I 5 hello"
//   Delete: "D <pos> <len>"   e.g. "D 5 3"
//
// Transform convention:
//   transform(a, b) returns a' such that:
//     apply(apply(doc, b), a') == apply(apply(doc, a), transform(b, a))
//   i.e. a' is "a adjusted for the fact that b has already been applied".
//
//   Tie-breaks and edge cases (all consistent with the diamond property):
//   - Same-position inserts: b (already applied) goes first, so a shifts right.
//     The diamond property does NOT hold for concurrent same-position inserts
//     without client IDs; callers should break ties by client_id externally.
//   - Insert inside a concurrent delete: no-op (the insertion point was removed).
//   - Delete spanning a concurrent insert: expand the delete to cover the insert.
//   These two rules are the only way to satisfy the diamond property with a
//   single-op return type.

namespace collab {

struct insert_op {
    size_t pos;
    std::string text;
};

struct delete_op {
    size_t pos;
    size_t len;
};

using doc_op = std::variant<insert_op, delete_op>;

// Serialize/deserialize for PancyDB storage
std::string serialize(const doc_op& op);
doc_op deserialize(std::string_view s);

// Apply op to text in-place. Clamps out-of-bounds positions/lengths silently.
void apply_op(std::string& text, const doc_op& op);

// OT core. See transform convention above.
doc_op transform(const doc_op& a, const doc_op& b);

// Transform a against a sequence of already-committed ops (applied left to right).
doc_op transform_seq(doc_op a, std::span<const doc_op> committed);

} // namespace collab

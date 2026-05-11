#pragma once

#include "doc_ops.hh"
#include "pancydb.hh"
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

// doc_state.hh
//   Read committed editor operations from a pancy::pancydb and reconstruct
//   document text. Key layout (must match writers, e.g. collab_model):
//     doc/<doc_id>/op/<client_id>/<client_seq>
//   Values are collab::serialize(doc_op) strings. Total order for replay is
//   pancy::vv::version (monotonic per successful put on that replica view).

namespace collab {

struct committed_op {
    pancy::version_type version = 0;
    uint64_t client_id = 0;
    uint64_t client_seq = 0;
    doc_op op;
    // Optional human-readable client id (HTTP POST body). Empty when loaded
    // from storage via read_ops (only the hashed key prefix exists there).
    std::string client_id_label;
};

// Build PancyDB key for one op slot.
std::string op_key(std::string_view doc_id, uint64_t client_id, uint64_t client_seq);

// Collect all ops under doc/<doc_id>/op/* with valid keys and parseable values.
// Sorted by (version, client_id, client_seq). Skips keys that do not match the
// expected pattern or values that fail deserialize (invalid_argument).
std::vector<committed_op> read_ops(const pancy::pancydb& db, std::string_view doc_id);

// Replay ops in order to produce the current document text.
std::string reconstruct(std::span<const committed_op> ops);

// Unit tests for read_ops / reconstruct (used by standalone test-doc-state and
// by pt-collab --test). Safe to call from any binary that links doc_state.cc.
void test_doc_state();

} // namespace collab

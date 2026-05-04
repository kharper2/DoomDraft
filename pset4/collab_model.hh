#pragma once
#include "client_model.hh"
#include "doc_ops.hh"
#include <memory>
#include <string>
#include <vector>

// collab_model.hh
//   Simulated collaborative editor clients extending client_model.
//   Each editor coroutine:
//     1. Syncs newly committed ops from all peers via GET
//     2. OT-transforms its pending ops against remote committed ops
//     3. Generates random inserts/deletes and submits them via PUT
//
//   Convergence is verified externally: try_one_seed() in pt-collab.cc calls
//   reconstruct(read_ops(db)) on each live replica after the simulation and
//   asserts pairwise equality. check() here only validates per-replica
//   invariants (all op values are deserializable).

class collab_model : public client_model {
public:
    collab_model(size_t nreplicas, random_source& randomness,
                 std::string doc_id = "main", size_t nclients = 8);

    void start() override;
    void stop() override;
    std::optional<std::string> check(const pancy::pancydb& db) override;

    unsigned long ops_submitted = 0;
    unsigned long ops_committed = 0;
    unsigned long ops_transformed = 0;

private:
    std::string doc_id_;
    size_t nclients_;

    struct editor_state {
        cotamer::task<> task;
        size_t leader;
        std::string local_text;
        std::vector<collab::doc_op> pending;      // submitted but not yet confirmed committed
        std::vector<uint64_t> pending_seqs;       // client_seq for each pending op
        std::vector<uint64_t> peer_next_seq;      // [peer_cid] → next seq to fetch
        uint64_t next_seq = 0;                    // next seq for our own ops
    };

    std::vector<std::unique_ptr<editor_state>> editors_;
    cotamer::task<> editor(unsigned cid);
    cotamer::task<> sync_committed_ops(editor_state& es, unsigned cid, uint64_t& serial);
};

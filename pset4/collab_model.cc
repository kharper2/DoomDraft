#include "collab_model.hh"
#include "doc_state.hh"
#include <algorithm>
#include <cassert>

namespace cot = cotamer;
using namespace std::chrono_literals;

collab_model::collab_model(size_t nreplicas, random_source& randomness,
                            std::string doc_id, size_t nclients)
    : client_model(nreplicas, randomness),
      doc_id_(std::move(doc_id)),
      nclients_(nclients) {}

void collab_model::start() {
    assert(editors_.empty());
    set_nclients(nclients_);
    editors_.reserve(nclients_);
    while (editors_.size() < nclients_) {
        auto* es = new editor_state;
        es->leader = random_replica();
        es->peer_next_seq.assign(nclients_, 0);
        editors_.emplace_back(es);
        es->task = editor(static_cast<unsigned>(editors_.size() - 1));
    }
}

void collab_model::stop() {
    client_model::stop();
    editors_.clear();
}

// check: verify all doc op values under doc/<doc_id>/op/ are deserializable.
// Cross-replica convergence is checked in try_one_seed() via pairwise
// reconstruct(read_ops(...)) comparison, not here.
std::optional<std::string> collab_model::check(const pancy::pancydb& db) {
    const std::string prefix = "doc/" + doc_id_ + "/op/";
    for (auto it = db.begin(); it != db.end(); ++it) {
        const std::string& key = it->first;
        if (key.size() <= prefix.size() ||
            key.compare(0, prefix.size(), prefix) != 0) {
            continue;
        }
        if (it->second.version == pancy::nonexistent_version) {
            continue;
        }
        try {
            collab::deserialize(it->second.value);
        } catch (...) {
            return key;
        }
    }
    return std::nullopt;
}

// sync_committed_ops: pull newly committed ops in global commit order.
// Paxos assigns a monotonic version per committed key; the correct OT order
// is by that version, not by iterating peers 0..n-1. Each round we GET each
// peer's next expected seq, pick the op with minimum version (tie: lower
// peer), apply or pop pending, then advance that peer only; repeat until no
// peer has a ready next op.
cotamer::task<> collab_model::sync_committed_ops(
    editor_state& es, unsigned cid, uint64_t& serial) {
    while (true) {
        bool have_best = false;
        unsigned best_peer = 0;
        uint64_t best_seq = 0;
        pancy::version_type best_ver{};
        collab::doc_op best_op{};

        for (unsigned peer = 0; peer < nclients_; ++peer) {
            uint64_t seq = es.peer_next_seq[peer];
            std::string key = collab::op_key(doc_id_, peer, seq);
            serial += serial_step();
            co_await send_request<pancy::get_request>(es.leader, serial, key);
            auto resp = co_await cot::attempt(
                receive_response<pancy::get_response>(es.leader, serial),
                cot::after(randomness().normal(3s, 1s))
            );
            if (!resp || resp->errcode != pancy::errc::ok || resp->value.empty()) {
                continue;
            }
            collab::doc_op committed;
            try {
                committed = collab::deserialize(resp->value);
            } catch (...) {
                continue;
            }
            if (!have_best || resp->version < best_ver
                || (resp->version == best_ver && peer < best_peer)) {
                have_best = true;
                best_peer = peer;
                best_seq = seq;
                best_ver = resp->version;
                best_op = std::move(committed);
            }
        }

        if (!have_best) {
            break;
        }

        if (best_peer == cid) {
            // Our own op was committed: pop it from the front of pending.
            if (!es.pending.empty() && es.pending_seqs[0] == best_seq) {
                es.pending.erase(es.pending.begin());
                es.pending_seqs.erase(es.pending_seqs.begin());
                ++ops_committed;
            }
        } else {
            // Remote op: update local view and OT-transform pending against it.
            collab::apply_op(es.local_text, best_op);
            for (auto& p : es.pending) {
                p = collab::transform(p, best_op);
                ++ops_transformed;
            }
        }
        ++es.peer_next_seq[best_peer];
    }
}

cotamer::task<> collab_model::editor(unsigned cid) {
    editor_state& es = *editors_[cid];
    // serial starts at cid; each send advances by serial_step() (4096).
    // Lower 12 bits = client id, used by client_model to route responses.
    uint64_t serial = cid;

    while (true) {
        // Step 1: sync committed ops from all peers
        co_await sync_committed_ops(es, cid, serial);

        // Step 2: generate a random edit (probability 0.6)
        if (randomness().coin_flip(0.6)) {
            collab::doc_op op;
            if (es.local_text.empty() || randomness().coin_flip(0.5)) {
                // Insert: random position, 1–5 random lowercase letters
                size_t pos = es.local_text.empty()
                    ? 0
                    : randomness().uniform(size_t(0), es.local_text.size());
                size_t len = randomness().uniform(size_t(1), size_t(5));
                std::string text(len, ' ');
                for (auto& c : text) {
                    c = static_cast<char>('a' + randomness().uniform(0, 25));
                }
                op = collab::insert_op{pos, std::move(text)};
            } else {
                // Delete: random position and length ≤ min(5, remaining)
                size_t pos = randomness().uniform(size_t(0), es.local_text.size() - 1);
                size_t max_len = std::min(size_t(5), es.local_text.size() - pos);
                size_t len = randomness().uniform(size_t(1), max_len);
                op = collab::delete_op{pos, len};
            }
            collab::apply_op(es.local_text, op);
            es.pending.push_back(op);
            es.pending_seqs.push_back(es.next_seq++);
            ++ops_submitted;
        }

        // Step 3: submit pending[0] with retry on timeout/redirect
        if (!es.pending.empty()) {
            uint64_t seq = es.pending_seqs[0];
            std::string key = collab::op_key(doc_id_, cid, seq);
            std::string value = collab::serialize(es.pending[0]);
            serial += serial_step();
            for (unsigned tries = 0; ; ++tries) {
                co_await send_request<pancy::put_request>(
                    es.leader, serial, key, value);
                auto resp = co_await cot::attempt(
                    receive_response<pancy::put_response>(es.leader, serial),
                    cot::after(randomness().normal(3s, 1s))
                );
                if (!resp) {
                    // timeout or redirect (es.leader updated on redirect)
                    if (tries % 3 == 2) {
                        es.leader = random_replica();
                    }
                    continue;
                }
                break;
            }
        }

        // Step 4: brief pause before next iteration
        co_await cot::after(randomness().uniform(1ms, 20ms));
    }
}

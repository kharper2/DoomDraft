#pragma once

#include "client_model.hh"
#include "cotamer/cotamer.hh"
#include "pancydb.hh"
#include <atomic>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

// http_server.hh
//
// HTTP/1.1 + Server-Sent Events front end for the simulated Paxos cluster
// in pt-collab.cc. Built directly on cotamer::tcp_listen / tcp_accept; no
// external HTTP dependency.
//
// Two pieces:
//   - http_client_model: a client_model subclass that owns one simulated
//     client id and exposes submit_put(key, value) as a coroutine. The HTTP
//     handlers call submit_put to commit operations through Paxos.
//   - http_paxos_bridge: glue passed to run_http_server. It provides the
//     current leader's pancydb (for reconstruct) and the document id.

class http_client_model : public client_model {
  public:
    http_client_model(size_t nreplicas, random_source& randomness);

    void start() override;
    std::optional<std::string> check(const pancy::pancydb&) override {
        return std::nullopt;
    }

    // Submit a PUT for `key` -> `value` through Paxos. Blocks the awaiting
    // coroutine until commit succeeds. Returns the PancyDB version of the
    // committed put. Returns 0 if the retry budget is exhausted.
    cotamer::task<pancy::version_type> submit_put(std::string key,
                                                  std::string value);

    size_t leader_index() const { return leader_; }
    void set_leader(size_t idx) { leader_ = idx; }

  private:
    static constexpr size_t kMyCid = 0;
    size_t leader_ = 0;
    std::atomic<uint64_t> next_serial_offset_ = 0;
};

// Bridge between the HTTP server and the simulated Paxos cluster.
//   doc_id:        the (single) document id this server serves.
//   client:        the http_client_model that drives Paxos PUTs.
//   current_db:    returns the pancydb of the replica we should read from
//                  (typically the leader); used by GET / SSE.
struct http_paxos_bridge {
    std::string doc_id;
    http_client_model* client = nullptr;
    std::function<const pancy::pancydb&()> current_db;
    std::function<bool(size_t)> fail_replica;
};

// Listen on `port` and serve HTTP. Returns a task that runs forever; detach
// it (or co_await) inside cot::loop().
cotamer::task<> run_http_server(uint16_t port, http_paxos_bridge bridge);

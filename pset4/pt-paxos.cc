#include "lockseq_model.hh"
#include "netsim.hh"
#include "pancydb.hh"
#include <algorithm>
#include <cassert>
#include <deque>
#include <map>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace cot = cotamer;
using namespace std::chrono_literals;

enum class failure_schedule_kind {
  none,
  failover,
  recover,  // fail the initial leader, then later heal it
  split,    // partition one replica away from the others
  unstable, // combine partitioning, leader flaps, and follower churn
  torture,  // quorum-preserving leader pinball plus asymmetric partitions
  darias,   // pairwise directed link pinball
  eric,     // leader-client/replica connectivity schedule
  eric_recover, // temporarily fail the current leader, then restore it
  eric_split,   // isolate the current leader from replicas, then heal
  random    // randomly choose one of the above schedules
};

// testinfo
//    Holds configuration information about this test.

struct testinfo {
  random_source randomness;
  double loss = 0.01;
  bool verbose = false;
  bool print_db = false;
  bool latency_trace = false;
  std::optional<size_t> ignored_replica_for_check;
  size_t nreplicas = 3;
  size_t initial_leader = 0;

  // Per-message simulated network delays. Defaults match the historical
  // bench values so collab-bench.sh / pt-paxos / pt-collab behavior is
  // unchanged. The server (pt-collab-server) overrides these to 0 because
  // in-process replicas have no real link latency and the delays trigger
  // Paxos elections under wall-clock time.
  cot::duration link_delay = 5ms;
  cot::duration send_delay = 1ms;
  cot::duration receive_delay = 1ms;

  failure_schedule_kind failure_schedule = failure_schedule_kind::none;

  template <typename T> void configure_port(netsim::port<T> &port) {
    port.set_verbose(verbose);
    port.set_receive_delay(receive_delay);
  }
  template <typename T> void configure_channel(netsim::channel<T> &chan) {
    chan.set_loss(loss);
    chan.set_verbose(verbose);
    chan.set_link_delay(link_delay);
    chan.set_send_delay(send_delay);
  }
  template <typename T> void configure_quiet_channel(netsim::channel<T> &chan) {
    chan.set_loss(loss);
    chan.set_link_delay(link_delay);
    chan.set_send_delay(send_delay);
  }
};

struct pt_paxos_instance;

struct log_entry {
  uint64_t slot;  // position in the replicated log
  uint64_t round; // round/leadership epoch that created this entry
  pancy::request req;
};

struct probe_message {
  uint64_t round;          // candidate's proposed new round
  size_t leader;           // candidate replica id
  uint64_t last_log_index; // candidate's last slot
  uint64_t last_log_round; // round of candidate's last slot
};

struct prepare_message {
  uint64_t round;
  size_t server; // which replica sent the response
  bool promised;
};

struct propose_message {
  uint64_t round;
  size_t leader;
  uint64_t prev_log_index; // slot immediately before this batch
  uint64_t prev_log_round; // round at prev_log_index
  uint64_t decide_through; // leader's current commit watermark
  uint64_t trim_through;   // all log entries with slot <= trim_through_ may be
                           // discarded locally
  std::vector<log_entry>
      entries; // batched log entries where empty means heartbeat.
};

struct ack_message {
  uint64_t round;       // round this acknowledgement belongs to
  size_t server;        // follower that sent the ack
  bool success;         // whether append/prefix matching succeeded
  uint64_t match_index; // highest slot definitely matched on follower
  uint64_t next_index;  // next slot the leader should try to send
};

using paxos_message =
    std::variant<probe_message, prepare_message, propose_message, ack_message>;

enum class replica_role { follower, candidate, leader };

struct pt_paxos_replica {
  size_t index_;        // This replica's id
  size_t nreplicas_;    // Cluster size
  size_t leader_index_; // Best current guess of who the leader is
  netsim::port<pancy::request> from_clients_;   // Client requests arrive here
  netsim::port<paxos_message> from_replicas_;   // Replica RPCs arrive here
  netsim::channel<pancy::response> to_clients_; // Responses go back here
  std::vector<std::unique_ptr<netsim::channel<paxos_message>>> to_replicas_;
  pancy::pancydb db_; // Local replicated database state

  random_source &randomness_;
  replica_role role_ = replica_role::follower;
  uint64_t current_round_ = 0;
  std::optional<size_t> promised_leader_; // vote/promise recorded in this round
  std::vector<bool> prepare_ok_;          // candidate-side record of promises

  std::map<uint64_t, log_entry> log_; // slot->entry replicated log
  uint64_t next_slot_ = 1;            // one past the highest known slot
  uint64_t commit_index_ = 0;         // highest slot known committed by quorum
  uint64_t last_applied_ = 0;         // highest committed slot applied to db_
  uint64_t trim_through_ = 0; // old committed prefix that can be discarded
  uint64_t trim_round_ =
      0; // trim_round_ stores the round of the last slot that was trimmed away
         // it exists so the code can still verify the boundary between the
         // discarded prefix and the retained suffix

  std::vector<uint64_t>
      match_index_; // Leader: highest matched slot per follower
  std::vector<uint64_t> next_index_; // Leader: next slot to send per follower
  // maps client serial -> committed response, making retries idempotent
  std::unordered_map<uint64_t, pancy::response> response_cache_;
  // maps client serial -> slot while the leader is waiting for commit
  std::unordered_map<uint64_t, uint64_t> pending_clients_;
  // client requests waiting to be appended as a batch
  std::deque<pancy::request> pending_client_requests_;
  // serial numbers currently queued or awaiting commit
  std::unordered_set<uint64_t> pending_client_serials_;
  // deadline for flushing the current client batch
  std::optional<cot::steady_time_point> client_batch_deadline_;

  cot::duration election_timeout_ = 250ms;
  static constexpr cot::duration heartbeat_interval_ = 50ms;
  static constexpr cot::duration client_batch_window_ = 5ms;

  // catch-up sends up to this many entries at once
  static constexpr size_t append_batch_size_ = 16;
  // keep some committed history so lagging followers can still be repaired
  static constexpr uint64_t trim_retention_ = 256;

  pt_paxos_replica(size_t index, size_t nreplicas, random_source &);
  void initialize(pt_paxos_instance &);

  size_t quorum_size() const { return nreplicas_ / 2 + 1; }
  bool has_quorum(size_t n) const { return n >= quorum_size(); }
  uint64_t last_log_index() const { return next_slot_ - 1; } // Local log tail
  uint64_t last_log_round() const;
  uint64_t round_at(uint64_t slot) const;
  cot::duration random_election_timeout();
  void reset_election_timeout();
  bool leader_is_up_to_date(uint64_t last_log_index,
                            uint64_t last_log_round) const;
  bool prefix_matches(uint64_t prev_log_index, uint64_t prev_log_round) const;
  void become_follower(uint64_t round, size_t leader_hint);
  void become_leader();
  void trim_log_prefix();
  void update_commit_index();
  void update_trim_watermark();
  cot::task<> flush_pending_client_requests();

  cot::task<> apply_committed_entries();
  cot::task<> send_propose_to(size_t peer);
  cot::task<> broadcast_propose();
  cot::task<> start_probe_round();
  cot::task<> handle_probe(const probe_message &msg);
  cot::task<> handle_prepare(const prepare_message &msg);
  cot::task<> handle_propose(const propose_message &msg);
  cot::task<> handle_ack(const ack_message &msg);
  cot::task<> handle_replica_message(paxos_message msg);
  cot::task<> handle_client_request(pancy::request req);
  cot::task<> run();
};

struct pt_paxos_instance {
  testinfo &tester;
  client_model &clients;
  std::vector<std::unique_ptr<pt_paxos_replica>> replicas;
  std::vector<bool>
      replica_available; // which replicas are still "up" at test end

  pt_paxos_instance(testinfo &, client_model &);

  double base_loss() const { return tester.loss; }

  void heal_all_links();
  void fail_replica(size_t rid);
  void recover_replica(size_t rid);
  void partition_one_against_rest(size_t rid);
};

namespace std {

template <typename CharT> struct formatter<log_entry, CharT> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  // No custom format options; accept the default {} behavior
  template <typename FormatContext>
  auto format(const log_entry &m, FormatContext &ctx) const {
    auto req = std::visit(
        [](const auto &reqt) { return std::format("{}", reqt); }, m.req);
    // pancy::request is a variant, so print whichever operation it currently
    // holds
    return std::format_to(ctx.out(), "E(S{}, R{}, {})", m.slot, m.round, req);
  }
};

template <typename CharT> struct formatter<probe_message, CharT> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const probe_message &m, FormatContext &ctx) const {
    return std::format_to(ctx.out(), "PROBE(R{}, L{}, last=S{}/R{})", m.round,
                          m.leader, m.last_log_index, m.last_log_round);
  }
};

template <typename CharT> struct formatter<prepare_message, CharT> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const prepare_message &m, FormatContext &ctx) const {
    return std::format_to(ctx.out(), "PREPARE(R{}, S{}, {})", m.round, m.server,
                          m.promised ? "promise" : "deny");
  }
};

template <typename CharT> struct formatter<propose_message, CharT> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const propose_message &m, FormatContext &ctx) const {
    if (!m.entries.empty()) {
      std::string payload;
      if (m.entries.size() == 1) {
        // single-entry proposal prints the exact entry
        payload = std::format("{}", m.entries.front());
      } else {
        // batches print a compact range summary instead of every entry
        payload = std::format("{}..{} ({} entries)", m.entries.front(),
                              m.entries.back(), m.entries.size());
      }
      return std::format_to(
          ctx.out(),
          "PROPOSE(R{}, L{}, prev=S{}/R{}, decide=S{}, trim=S{}, {})", m.round,
          m.leader, m.prev_log_index, m.prev_log_round, m.decide_through,
          m.trim_through, payload);
    }
    // empty entries means this RPC is acting like a heartbeat/commit
    // propagation
    return std::format_to(
        ctx.out(), "DECIDE(R{}, L{}, prev=S{}/R{}, decide=S{}, trim=S{})",
        m.round, m.leader, m.prev_log_index, m.prev_log_round, m.decide_through,
        m.trim_through);
  }
};

template <typename CharT> struct formatter<ack_message, CharT> {
  constexpr auto parse(format_parse_context &ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const ack_message &m, FormatContext &ctx) const {
    return std::format_to(ctx.out(), "ACK(R{}, S{}, {}, match=S{}, next=S{})",
                          m.round, m.server, m.success ? "ok" : "retry",
                          m.match_index, m.next_index);
  }
};

} // namespace std

namespace netsim {

template <> struct message_traits<paxos_message> {
  static std::string print_transform(const paxos_message &m) {
    return std::visit([](const auto &msg) { return std::format("{}", msg); },
                      m);
    // netsim only sees a variant; delegate printing to the matching formatter
  }
};

} // namespace netsim

// sets up the replica’s own fields and allocates its ports/channels
pt_paxos_replica::pt_paxos_replica(size_t index, size_t nreplicas,
                                   random_source &randomness)
    : index_(index), nreplicas_(nreplicas), leader_index_(index),
      from_clients_(randomness, std::format("R{}", index_)),
      from_replicas_(randomness, std::format("R{}/r", index_)),
      to_clients_(randomness, from_clients_.id()), to_replicas_(nreplicas),
      randomness_(randomness), prepare_ok_(nreplicas, false),
      match_index_(nreplicas, 0), next_index_(nreplicas, 1) {
  // allocate one outgoing replica-RPC channel per peer
  for (size_t s = 0; s != nreplicas_; ++s) {
    to_replicas_[s].reset(
        new netsim::channel<paxos_message>(randomness, from_clients_.id()));
  }
  // followers and candidates should not share the same timeout schedule
  reset_election_timeout();
}
// connects ports/channels to the client model and to the other replicas
void pt_paxos_replica::initialize(pt_paxos_instance &inst) {
  // start with one designated leader and all other replicas following it
  leader_index_ = inst.tester.initial_leader;
  role_ =
      index_ == leader_index_ ? replica_role::leader : replica_role::follower;
  // wire the client model to this replica's request/response ports
  inst.clients.connect_replica(index_, from_clients_, to_clients_);
  inst.tester.configure_port(from_clients_);
  inst.tester.configure_port(from_replicas_);
  inst.tester.configure_channel(to_clients_);
  inst.tester.configure_quiet_channel(inst.clients.request_channel(index_));
  // wire every outgoing replica channel into the target replica's receive port
  for (size_t s = 0; s != nreplicas_; ++s) {
    to_replicas_[s]->connect(inst.replicas[s]->from_replicas_);
    inst.tester.configure_channel(*to_replicas_[s]);
  }
  if (role_ == replica_role::leader) {
    become_leader();
  }
}

// create paxos group
pt_paxos_instance::pt_paxos_instance(testinfo &tester, client_model &clients)
    : tester(tester), clients(clients), replicas(tester.nreplicas),
      replica_available(tester.nreplicas, true) {
  // build every replica before wiring the cluster together
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replicas[s].reset(
        new pt_paxos_replica(s, tester.nreplicas, tester.randomness));
  }
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replicas[s]->initialize(*this);
  }
}

void pt_paxos_instance::heal_all_links() {
  // restore client-facing links for all replicas
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    clients.request_channel(s).set_loss(base_loss());
    replicas[s]->to_clients_.set_loss(base_loss());
  }
  // restore all replica-to-replica links
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    for (size_t d = 0; d != tester.nreplicas; ++d) {
      replicas[s]->to_replicas_[d]->set_loss(base_loss());
    }
  }
}

void pt_paxos_instance::fail_replica(size_t rid) {
  // final correctness checks should treat this replica as down
  replica_available[rid] = false;
  // make the failed replica unreachable from clients in both directions
  clients.request_channel(rid).set_loss(1.0);
  replicas[rid]->to_clients_.set_loss(1.0);
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    // drop all consensus traffic to and from the failed replica
    replicas[rid]->to_replicas_[s]->set_loss(1.0);
    replicas[s]->to_replicas_[rid]->set_loss(1.0);
  }
}

void pt_paxos_instance::recover_replica(size_t rid) {
  replica_available[rid] = true;
  // the replica's in-memory state is preserved only connectivity changes
  clients.request_channel(rid).set_loss(base_loss());
  replicas[rid]->to_clients_.set_loss(base_loss());
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replicas[rid]->to_replicas_[s]->set_loss(base_loss());
    replicas[s]->to_replicas_[rid]->set_loss(base_loss());
  }
}

void pt_paxos_instance::partition_one_against_rest(size_t rid) {
  heal_all_links();
  // start from a known healed network before applying the partition
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    if (s == rid) {
      continue;
    }
    // clients can still reach rid, but replicas cannot exchange consensus
    // traffic
    replicas[rid]->to_replicas_[s]->set_loss(1.0);
    replicas[s]->to_replicas_[rid]->set_loss(1.0);
  }
}

// returns the round that created the log entry at a given slot
uint64_t pt_paxos_replica::round_at(uint64_t slot) const {
  if (slot == 0) {
    return 0; // slot 0 is the virtual empty prefix
  }
  if (slot == trim_through_) {
    return trim_round_;
    // even if that entry was trimmed physically, we still remember its round
  }
  // return the round from the <round, log_entry> map
  if (auto it = log_.find(slot); it != log_.end()) {
    return it->second.round;
  }
  return 0; // missing slot means "unknown / not present locally."
}

uint64_t pt_paxos_replica::last_log_round() const {
  return round_at(last_log_index());
}

cot::duration pt_paxos_replica::random_election_timeout() {
  // randomized timeouts reduce repeated election ties
  return randomness_.uniform(200ms, 350ms);
}

void pt_paxos_replica::reset_election_timeout() {
  // called whenever leader activity arrives or a new election begins
  election_timeout_ = random_election_timeout();
}

bool pt_paxos_replica::leader_is_up_to_date(uint64_t last_log_index,
                                            uint64_t last_log_round) const {
  // newer round beats older round when deciding whom to promise
  if (last_log_round != this->last_log_round()) {
    return last_log_round > this->last_log_round();
  }
  // if rounds tie, longer log wins
  return last_log_index >= this->last_log_index();
}

bool pt_paxos_replica::prefix_matches(uint64_t prev_log_index,
                                      uint64_t prev_log_round) const {
  // empty prefix only matches if we haven't trimmed past it
  if (prev_log_index == 0) {
    return trim_through_ == 0;
  }
  // leader is referring to a prefix older than we can verify safely
  if (prev_log_index < trim_through_) {
    return false;
  }
  // match exactly against the stored trimmed-boundary round
  if (prev_log_index == trim_through_) {
    return prev_log_round == trim_round_;
  }
  // otherwise we must still have the prefix entry in memory and its round must
  // match
  auto it = log_.find(prev_log_index);
  return it != log_.end() && it->second.round == prev_log_round;
}

void pt_paxos_replica::become_follower(uint64_t round, size_t leader_hint) {
  if (round > current_round_) {
    current_round_ = round;
    promised_leader_.reset();
    // a higher round invalidates any old vote/promise
  }
  role_ = replica_role::follower;
  leader_index_ = leader_hint;
  prepare_ok_.assign(nreplicas_, false);
  pending_clients_.clear();
  pending_client_requests_.clear();
  pending_client_serials_.clear();
  client_batch_deadline_.reset();
  // any uncommitted client work is no longer this replica's responsibility
  reset_election_timeout();
}

void pt_paxos_replica::become_leader() {
  role_ = replica_role::leader;
  leader_index_ = index_;
  // a leader implicitly promises itself in its own round
  promised_leader_ = index_;
  prepare_ok_.assign(nreplicas_, false);
  match_index_.assign(nreplicas_, 0);
  next_index_.assign(nreplicas_, last_log_index() + 1);
  // start by assuming followers may need catch-up from our current tail
  match_index_[index_] = last_log_index();
  next_index_[index_] = last_log_index() + 1;
  // the leader is always fully caught up with itself
}

void pt_paxos_replica::trim_log_prefix() {
  while (!log_.empty() && log_.begin()->first <= trim_through_) {
    // preserve the boundary round before deleting the physical entry
    trim_round_ = log_.begin()->second.round;
    log_.erase(log_.begin());
  }
}

void pt_paxos_replica::update_commit_index() {
  if (role_ != replica_role::leader) {
    return; // only leaders get to decide
  }
  for (uint64_t idx = last_log_index(); idx > commit_index_; --idx) {
    // only current-round entries may be newly committed by this leader
    if (round_at(idx) != current_round_) {
      continue;
    }
    size_t replicated = 1;
    // count the leader itself
    for (size_t s = 0; s != nreplicas_; ++s) {
      if (s != index_ && match_index_[s] >= idx) {
        ++replicated;
      }
    }
    if (has_quorum(replicated)) {
      commit_index_ = idx;
      return; // highest quorum-replicated current-round slot is committed
    }
  }
}

void pt_paxos_replica::update_trim_watermark() {
  if (role_ != replica_role::leader) {
    return;
  }
  // find lowest agreement between leader and followers
  // with regards to the slots
  uint64_t min_match = match_index_[index_];
  for (size_t s = 0; s != nreplicas_; ++s) {
    min_match = std::min(min_match, match_index_[s]);
  }
  // never trim beyond what is committed
  min_match = std::min(min_match, commit_index_);
  // keep some retained committed history so lagging followers can still be
  // repaired
  uint64_t target_trim =
      min_match > trim_retention_ ? min_match - trim_retention_ : 0;
  if (target_trim > trim_through_) {
    trim_through_ = target_trim;
    trim_log_prefix();
  }
}

cot::task<> pt_paxos_replica::apply_committed_entries() {
  while (last_applied_ < commit_index_) {
    ++last_applied_; // first not aplied slot
    // apply newly committed slots strictly in order
    auto it = log_.find(last_applied_);
    if (it == log_.end()) {
      continue;
    }
    uint64_t serial = pancy::message_serial(it->second.req);
    // execute the request against the replicated state machine only once
    if (!response_cache_.contains(serial)) {
      response_cache_.emplace(serial, db_.process_req(it->second.req));
    }
    // only leader sends client response
    if (role_ == replica_role::leader) {
      auto pit = pending_clients_.find(serial);
      if (pit != pending_clients_.end() && pit->second == last_applied_) {
        co_await to_clients_.send(response_cache_.at(serial));
        // leaders respond to clients only after the corresponding slot commits
        // and is applied
        pending_clients_.erase(pit);
        pending_client_serials_.erase(serial);
      }
    }
  }
  trim_log_prefix();
}

cot::task<> pt_paxos_replica::flush_pending_client_requests() {
  // if we are not the leader anymore, or there is nothing queued
  // there is nothing safe to flush
  if (role_ != replica_role::leader || pending_client_requests_.empty()) {
    client_batch_deadline_.reset();
    co_return;
  }
  // keep draining queued client requests while:
  // we are still the leader, and there is still work waiting to be batched
  while (role_ == replica_role::leader && !pending_client_requests_.empty()) {
    size_t batch =
        std::min(pending_client_requests_.size(), append_batch_size_);
    for (size_t i = 0; i != batch; ++i) {
      // Pull the oldest queued client request.
      auto req = std::move(pending_client_requests_.front());
      pending_client_requests_.pop_front();
      uint64_t serial = pancy::message_serial(req);
      uint64_t slot = next_slot_++;
      // record the request in the leader's log so it can be retransmitted
      // if followers drop the proposal
      log_.emplace(slot, log_entry{slot, current_round_, std::move(req)});
      pending_clients_.emplace(serial, slot);
    }

    match_index_[index_] = last_log_index();
    next_index_[index_] = last_log_index() + 1;
    // replicate the batch to followers
    co_await broadcast_propose();
    update_commit_index();
    co_await apply_committed_entries();
    update_trim_watermark();
  }
  // if we drained the queue, clear the deadline, otherwise, schedule another
  // flush soon so the remaining requests do not wait forever
  if (pending_client_requests_.empty()) {
    client_batch_deadline_.reset();
  } else {
    client_batch_deadline_ = cot::steady_now() + client_batch_window_;
  }
}

cot::task<> pt_paxos_replica::send_propose_to(size_t peer) {
  if (peer == index_ || role_ != replica_role::leader) {
    co_return; // only leaders send proposals, and never to themselves
  }
  // never try to resend earlier than the retained prefix boundary
  uint64_t next = std::max(next_index_[peer], trim_through_ + 1);
  next_index_[peer] = next;

  uint64_t prev = next - 1;
  std::vector<log_entry> entries;
  auto it = log_.lower_bound(next); // first log at or after next
  while (it != log_.end() && entries.size() < append_batch_size_) {
    // only send a contiguous suffix batch
    if (it->first != next + entries.size()) {
      break;
    }
    entries.push_back(it->second);
    ++it;
  }
  // empty entries acts like a heartbeat/decide message; non-empty entries
  // replicate log data
  co_await to_replicas_[peer]->send(
      propose_message{current_round_, index_, prev, round_at(prev),
                      commit_index_, trim_through_, std::move(entries)});
}

cot::task<> pt_paxos_replica::broadcast_propose() {
  for (size_t s = 0; s != nreplicas_; ++s) {
    if (s != index_) {
      co_await send_propose_to(s);
    }
  }
}

cot::task<> pt_paxos_replica::start_probe_round() {
  role_ = replica_role::candidate;
  ++current_round_;
  leader_index_ = index_;
  promised_leader_ = index_;
  // candidate promises/votes for itself
  prepare_ok_.assign(nreplicas_, false);
  prepare_ok_[index_] = true;
  // count self as one positive prepare response
  reset_election_timeout();

  probe_message msg{current_round_, index_, last_log_index(), last_log_round()};
  // include our log tip so peers can decide whether we're up to date enough to
  // lead
  for (size_t s = 0; s != nreplicas_; ++s) {
    if (s != index_) {
      co_await to_replicas_[s]->send(msg);
    }
  }
  // check immediately if our one promise is enough to become the leader
  if (has_quorum(1)) {
    become_leader();
    co_await broadcast_propose();
  }
}

cot::task<> pt_paxos_replica::handle_probe(const probe_message &msg) {
  if (msg.round < current_round_) {
    co_await to_replicas_[msg.leader]->send(
        prepare_message{current_round_, index_, false});
    co_return; // reject stale election attempts
  } // a newer round supersedes our old leadership/candidacy
  if (msg.round > current_round_) {
    become_follower(msg.round, leader_index_);
  }

  bool promise = false;
  if ((!promised_leader_ || *promised_leader_ == msg.leader) &&
      leader_is_up_to_date(msg.last_log_index, msg.last_log_round)) {
    // promise only if we have not already promised someone else in this round
    // and the candidate's log is at least as up to date as ours
    promised_leader_ = msg.leader;
    leader_index_ = msg.leader;
    reset_election_timeout();
    promise = true;
  }
  co_await to_replicas_[msg.leader]->send(
      prepare_message{current_round_, index_, promise});
}

cot::task<> pt_paxos_replica::handle_prepare(const prepare_message &msg) {
  // another replica has evidence of a newer round
  if (msg.round > current_round_) {
    become_follower(msg.round, leader_index_);
    co_return;
  }
  if (role_ != replica_role::candidate || msg.round != current_round_ ||
      !msg.promised || prepare_ok_[msg.server]) {
    co_return; // ignore stale, duplicate, denied, or irrelevant prepare
               // responses
  }
  prepare_ok_[msg.server] = true;
  size_t promises = 0;
  for (bool ok : prepare_ok_) {
    promises += ok;
  }
  if (has_quorum(promises)) {
    become_leader();
    co_await broadcast_propose();
    // Once a quorum promises, this candidate may start acting as leader
  }
}

cot::task<> pt_paxos_replica::handle_propose(const propose_message &msg) {
  if (msg.round < current_round_) {
    co_await to_replicas_[msg.leader]->send(ack_message{
        current_round_, index_, false, last_log_index(), last_log_index() + 1});
    co_return; // reject stale leaders immediately
  }

  // any valid current/newer leader message refreshes follower state
  become_follower(msg.round, msg.leader);

  // tell the leader where to back up to if our prefix does not match
  if (!prefix_matches(msg.prev_log_index, msg.prev_log_round)) {
    uint64_t next_index = trim_through_ + 1;
    if (msg.prev_log_index > trim_through_) {
      next_index = std::min(msg.prev_log_index, last_log_index() + 1);
    }
    co_await to_replicas_[msg.leader]->send(ack_message{
        current_round_, index_, false, last_log_index(), next_index});
    co_return;
  }

  uint64_t match_index = msg.prev_log_index;
  // everything through the matched prefix is already known to agree
  for (const auto &entry : msg.entries) {
    uint64_t slot = entry.slot;
    // entries in the already-trimmed committed prefix are implicitly
    // accepted
    if (slot <= trim_through_) {
      match_index = slot;
      continue;
    }
    if (auto it = log_.find(slot);
        it != log_.end() && it->second.round != entry.round) {
      // Never overwrite something we believe is already committed
      if (commit_index_ >= slot) {
        co_await to_replicas_[msg.leader]->send(
            ack_message{current_round_, index_, false, last_log_index(), slot});
        co_return;
      }
      // Conflicting uncommitted suffix is discarded so the leader can replace
      // it
      auto erase_it = log_.lower_bound(slot);
      while (erase_it != log_.end()) {
        erase_it = log_.erase(erase_it);
      }
      next_slot_ = slot;
    }
    // append any new entry we do not already have
    if (!log_.contains(slot)) {
      log_.emplace(slot, entry);
      next_slot_ = std::max(next_slot_, slot + 1);
    }
    match_index = slot;
  }
  // followers may only commit up to what the leader says and what they
  // possess
  if (msg.decide_through > commit_index_) {
    commit_index_ = std::min(msg.decide_through, last_log_index());
  }

  co_await apply_committed_entries();
  // never trim entries we have not yet applied to the local state machine
  if (msg.trim_through > trim_through_) {
    trim_through_ = std::min(msg.trim_through, last_applied_);
    trim_log_prefix();
  }
  // next_index is based on the last definitely matched slot, not just the tail
  co_await to_replicas_[msg.leader]->send(
      ack_message{current_round_, index_, true, match_index, match_index + 1});
}

cot::task<> pt_paxos_replica::handle_ack(const ack_message &msg) {
  if (msg.round > current_round_) {
    become_follower(msg.round, leader_index_);
    co_return; // another replica knows about a newer round
  }
  if (role_ != replica_role::leader || msg.round != current_round_) {
    co_return; // ignore acks not meant for this leader incarnation
  }

  if (msg.success) {
    // this follower is known to match farther now
    match_index_[msg.server] =
        std::max(match_index_[msg.server], msg.match_index);
    next_index_[msg.server] = std::max(next_index_[msg.server], msg.next_index);
    update_commit_index();
    co_await apply_committed_entries();
    update_trim_watermark();
    // new quorum knowledge may commit and trim more of the log
    if (next_index_[msg.server] <= last_log_index()) {
      co_await send_propose_to(msg.server);
      // if follower still lags behind, immediately send the next batch
    }
    co_return;
  }

  uint64_t fallback = msg.next_index;
  if (fallback == 0) {
    fallback = 1;
  }
  if (fallback >= next_index_[msg.server]) {
    fallback = next_index_[msg.server] > 1 ? next_index_[msg.server] - 1 : 1;
    // Ensure a retry actually backs up rather than stalling at the same index
  }
  next_index_[msg.server] = std::max(fallback, trim_through_ + 1);
  co_await send_propose_to(msg.server);
  // Retry from an earlier prefix to repair the follower's log
}

cot::task<> pt_paxos_replica::handle_replica_message(paxos_message msg) {
  if (auto *probe = std::get_if<probe_message>(&msg)) {
    co_await handle_probe(*probe);
  } else if (auto *prepare = std::get_if<prepare_message>(&msg)) {
    co_await handle_prepare(*prepare);
  } else if (auto *propose = std::get_if<propose_message>(&msg)) {
    co_await handle_propose(*propose);
  } else if (auto *ack = std::get_if<ack_message>(&msg)) {
    co_await handle_ack(*ack);
  }
  // One dispatcher unwraps the variant and forwards to the matching handler
}

cot::task<> pt_paxos_replica::handle_client_request(pancy::request req) {
  if (role_ != replica_role::leader) {
    size_t redirect = role_ == replica_role::candidate ? index_ : leader_index_;
    co_await to_clients_.send(pancy::redirection_response{
        pancy::response_header(req, pancy::errc::redirect), redirect});
    co_return; // only leaders serve requests; everyone else redirects
  }

  uint64_t serial = pancy::message_serial(req);
  if (auto it = response_cache_.find(serial); it != response_cache_.end()) {
    co_await to_clients_.send(it->second);
    co_return; // exact retry of a committed request gets the cached response
  }
  if (pending_client_serials_.contains(serial)) {
    co_return; // duplicate queued or in-flight request waits for original
  }
  pending_client_serials_.insert(serial);
  pending_client_requests_.push_back(std::move(req));
  if (!client_batch_deadline_) {
    client_batch_deadline_ = cot::steady_now() + client_batch_window_;
  }
  if (pending_client_requests_.size() >= append_batch_size_) {
    co_await flush_pending_client_requests();
  }
}

cot::task<> pt_paxos_replica::run() {
  while (true) {
    if (role_ == replica_role::leader && !pending_client_requests_.empty() &&
        client_batch_deadline_ &&
        cot::steady_now() >= *client_batch_deadline_) {
      co_await flush_pending_client_requests();
      continue;
    }

    if (role_ == replica_role::leader) {
      if (!pending_client_requests_.empty() && client_batch_deadline_) {
        auto delay = *client_batch_deadline_ - cot::steady_now();
        if (delay < 0ns) {
          delay = 0ns;
        }
        auto ret =
            co_await cot::first(from_clients_.receive(),
                                from_replicas_.receive(), cot::after(delay));
        if (ret.index() == 0) {
          co_await handle_client_request(std::get<0>(ret));
        } else if (ret.index() == 1) {
          co_await handle_replica_message(std::get<1>(ret));
        } else {
          co_await flush_pending_client_requests();
        }
      } else {
        auto ret = co_await cot::first(from_clients_.receive(),
                                       from_replicas_.receive(),
                                       cot::after(heartbeat_interval_));
        if (ret.index() == 0) {
          co_await handle_client_request(std::get<0>(ret));
        } else if (ret.index() == 1) {
          co_await handle_replica_message(std::get<1>(ret));
        } else {
          // idle leaders still send heartbeat/decide traffic to hold
          // leadership
          co_await broadcast_propose();
        }
      }
      continue;
    }

    auto ret =
        co_await cot::first(from_clients_.receive(), from_replicas_.receive(),
                            cot::after(election_timeout_));
    if (ret.index() == 0) {
      co_await handle_client_request(std::get<0>(ret));
    } else if (ret.index() == 1) {
      co_await handle_replica_message(std::get<1>(ret));
    } else {
      // no leader traffic arrived in time, so begin a new election round
      co_await start_probe_round();
    }
  }
}

cot::task<> clear_after(cot::duration d) {
  co_await cot::after(d);
  cot::clear();
}

cot::task<> stop_clients_after(client_model &clients, cot::duration d) {
  co_await cot::after(d);
  clients.stop();
}

void set_partition_loss(pt_paxos_instance &inst,
                        const std::vector<size_t> &left,
                        const std::vector<size_t> &right, double loss) {
  for (size_t l : left) {
    for (size_t r : right) {
      inst.replicas[l]->to_replicas_[r]->set_loss(loss);
      inst.replicas[r]->to_replicas_[l]->set_loss(loss);
    }
  }
}

void set_replica_link_loss(pt_paxos_instance &inst, size_t i, size_t j,
                           double loss) {
  if (i == j) {
    return;
  }
  inst.replicas[i]->to_replicas_[j]->set_loss(loss);
}

void set_replica_client_connectivity(pt_paxos_instance &inst, size_t replica,
                                     double request_loss,
                                     double response_loss) {
  inst.clients.request_channel(replica).set_loss(request_loss);
  inst.replicas[replica]->to_clients_.set_loss(response_loss);
}

void set_replica_interconnectivity(pt_paxos_instance &inst, size_t replica,
                                   double loss) {
  for (size_t s = 0; s != inst.replicas.size(); ++s) {
    if (s == replica) {
      continue;
    }
    inst.replicas[replica]->to_replicas_[s]->set_loss(loss);
    inst.replicas[s]->to_replicas_[replica]->set_loss(loss);
  }
}

void set_replica_failure(pt_paxos_instance &inst, size_t replica, double loss) {
  set_replica_client_connectivity(inst, replica, loss, loss);
  set_replica_interconnectivity(inst, replica, loss);
}

cot::task<> schedule_failover(pt_paxos_instance &inst) {
  // kill the original leader after some client work has accumulated
  co_await cot::after(20s);
  inst.fail_replica(inst.tester.initial_leader);
}

cot::task<> schedule_recovery(pt_paxos_instance &inst) {
  co_await cot::after(20s);
  inst.fail_replica(inst.tester.initial_leader);
  // later heal it so rejoin/catch-up code gets exercised
  co_await cot::after(25s);
  inst.recover_replica(inst.tester.initial_leader);
}

cot::task<> schedule_split_brain(pt_paxos_instance &inst) {
  if (inst.replicas.size() <= 1) {
    co_return; // No partition scenario for a single-replica cluster
  }
  co_await cot::after(20s);
  inst.partition_one_against_rest(inst.tester.initial_leader);
  // create, then later heal, a replica communication partition
  co_await cot::after(25s);
  inst.heal_all_links();
}

cot::task<> schedule_unstable_mixed(pt_paxos_instance &inst) {
  if (inst.tester.nreplicas <= 1) {
    co_return;
  }

  co_await cot::after(20s);

  std::vector<size_t> followers;
  followers.reserve(inst.tester.nreplicas - 1);
  for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
    if (s != inst.tester.initial_leader) {
      followers.push_back(s);
    }
  }
  std::shuffle(followers.begin(), followers.end(),
               inst.tester.randomness.engine());

  // Keep one connected group large enough to preserve quorum after one flap.
  size_t connected_count =
      std::min(inst.tester.nreplicas, inst.tester.nreplicas / 2 + 2);
  connected_count = std::max<size_t>(2, connected_count);
  std::vector<size_t> connected_group{inst.tester.initial_leader};
  connected_group.insert(connected_group.end(), followers.begin(),
                         followers.begin() + (connected_count - 1));
  std::vector<size_t> isolated_group(followers.begin() + (connected_count - 1),
                                     followers.end());

  if (!isolated_group.empty()) {
    set_partition_loss(inst, connected_group, isolated_group, 1.0);
  }

  auto partition_end = cot::steady_now() + 25s;
  while (cot::steady_now() < partition_end) {
    size_t victim = connected_group[inst.tester.randomness.uniform<size_t>(
        0, connected_group.size() - 1)];
    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(1500ms, 3500ms));
    inst.recover_replica(victim);
    if (!isolated_group.empty()) {
      set_partition_loss(inst, connected_group, isolated_group, 1.0);
    }
    co_await cot::after(inst.tester.randomness.uniform(500ms, 1500ms));
  }

  inst.heal_all_links();

  auto churn_end = cot::steady_now() + 20s;
  while (cot::steady_now() < churn_end) {
    size_t victim =
        inst.tester.randomness.uniform<size_t>(0, inst.tester.nreplicas - 1);
    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(1s, 4s));
    inst.recover_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(250ms, 2s));
  }

  inst.heal_all_links();
  for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
    if (!inst.replica_available[s]) {
      inst.recover_replica(s);
    }
  }
}

cot::task<> schedule_torture(pt_paxos_instance &inst) {
  if (inst.tester.nreplicas <= 1) {
    co_return;
  }

  co_await cot::after(12s);

  std::vector<size_t> followers;
  followers.reserve(inst.tester.nreplicas - 1);
  for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
    if (s != inst.tester.initial_leader) {
      followers.push_back(s);
    }
  }
  std::shuffle(followers.begin(), followers.end(),
               inst.tester.randomness.engine());

  // Keep one extra replica in the connected core so a single failure still
  // leaves a quorum available for consensus.
  size_t connected_count =
      std::min(inst.tester.nreplicas, inst.tester.nreplicas / 2 + 2);
  connected_count = std::max<size_t>(2, connected_count);
  std::vector<size_t> connected_group{inst.tester.initial_leader};
  connected_group.insert(connected_group.end(), followers.begin(),
                         followers.begin() + (connected_count - 1));
  std::vector<size_t> isolated_group(followers.begin() + (connected_count - 1),
                                     followers.end());

  auto choose_from = [&](const std::vector<size_t> &group) -> size_t {
    return group[inst.tester.randomness.uniform<size_t>(0, group.size() - 1)];
  };

  auto live_leader = [&]() -> std::optional<size_t> {
    for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
      if (inst.replica_available[s] &&
          inst.replicas[s]->role_ == replica_role::leader) {
        return s;
      }
    }
    return std::nullopt;
  };

  if (!isolated_group.empty()) {
    set_partition_loss(inst, connected_group, isolated_group, 1.0);
  }

  // Phase 1: keep a quorum-capable core alive, but repeatedly assassinate the
  // current leader and one border replica so elections and catch-up constantly
  // restart.
  auto partition_end = cot::steady_now() + 20s;
  while (cot::steady_now() < partition_end) {
    size_t victim = choose_from(connected_group);
    if (auto leader = live_leader();
        leader && std::find(connected_group.begin(), connected_group.end(),
                            *leader) != connected_group.end()) {
      victim = *leader;
    }

    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(250ms, 1200ms));
    inst.recover_replica(victim);

    if (!isolated_group.empty()) {
      set_partition_loss(inst, connected_group, isolated_group, 1.0);
      if (inst.tester.randomness.coin_flip(0.75)) {
        size_t fringe = choose_from(isolated_group);
        inst.fail_replica(fringe);
        co_await cot::after(inst.tester.randomness.uniform(200ms, 900ms));
        inst.recover_replica(fringe);
        set_partition_loss(inst, connected_group, isolated_group, 1.0);
      }
    }

    co_await cot::after(inst.tester.randomness.uniform(150ms, 850ms));
  }

  // Phase 2: heal everything and then keep killing whoever is leader now, one
  // at a time, so the protocol must keep re-electing and repairing while the
  // cluster is otherwise connected.
  inst.heal_all_links();

  auto leader_end = cot::steady_now() + 15s;
  while (cot::steady_now() < leader_end) {
    size_t victim = choose_from(connected_group);
    if (auto leader = live_leader()) {
      victim = *leader;
    }

    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(1s, 2400ms));
    inst.recover_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(100ms, 700ms));
  }

  // Phase 3: clean up and leave the system in a fully healed state so the
  // clients can eventually converge.
  // inst.heal_all_links();
  // for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
  //   if (!inst.replica_available[s]) {
  //     inst.recover_replica(s);
  //   }
  // }
}

// Build unordered pairs, shuffle then coin-flip to choose direction in each
// round. First round cuts one direction per pair, second round cuts the reverse.
cot::task<> schedule_darias(pt_paxos_instance &inst) {
  size_t n = inst.replicas.size();
  if (n <= 1) {
    co_return;
  }

  // build pairs {i, j}
  std::vector<std::pair<size_t, size_t>> pairs;
  for (size_t i = 0; i < n; ++i) {
    for (size_t j = i + 1; j < n; ++j) {
      pairs.push_back({i, j});
    }
  }

  // uniform permutation shuffle
  for (size_t i = pairs.size() - 1; i > 0; --i) {
    size_t j = inst.tester.randomness.uniform(size_t(0), i);
    std::swap(pairs[i], pairs[j]);
  }

  // coin-flip to decide which direction dies first
  std::vector<std::pair<size_t, size_t>> first_round;
  std::vector<std::pair<size_t, size_t>> second_round;
  first_round.reserve(pairs.size());
  second_round.reserve(pairs.size());
  for (auto &[a, b] : pairs) {
    if (inst.tester.randomness.coin_flip()) {
      first_round.push_back({a, b});
      second_round.push_back({b, a});
    } else {
      first_round.push_back({b, a});
      second_round.push_back({a, b});
    }
  }

  // cut one direction per pair
  for (auto &[src, dst] : first_round) {
    co_await cot::after(inst.tester.randomness.uniform(500ms, 2s));
    set_replica_link_loss(inst, src, dst, 1.0);
  }

  // cut the reverse direction
  for (auto &[src, dst] : second_round) {
    co_await cot::after(inst.tester.randomness.uniform(500ms, 2s));
    set_replica_link_loss(inst, src, dst, 1.0);
  }

  // hold block
  co_await cot::after(30s);

  // restore all
  double loss = inst.tester.loss;
  for (auto &[src, dst] : first_round) {
    set_replica_link_loss(inst, src, dst, loss);
  }
  for (auto &[src, dst] : second_round) {
    set_replica_link_loss(inst, src, dst, loss);
  }
}

// Permanently fail leader by disconnecting from client and other replicas.
cot::task<> schedule_eric(pt_paxos_instance &inst) {
  cot::duration start = 25s;
  co_await cot::after(start);
  size_t leader = inst.replicas.front()->leader_index_;
  inst.tester.ignored_replica_for_check = leader;
  set_replica_failure(inst, leader, 1.0);
}

// Temporarily fail leader.
cot::task<> schedule_eric_recover(pt_paxos_instance &inst) {
  cot::duration start = 25s;
  cot::duration downtime = 20s;
  co_await cot::after(start);
  size_t leader = inst.replicas.front()->leader_index_;
  set_replica_failure(inst, leader, 1.0);
  co_await cot::after(downtime);
  set_replica_failure(inst, leader, inst.tester.loss);
}

// Temporarily fail leader by disconnecting from other replicas (but not
// client).
cot::task<> schedule_eric_split_brain(pt_paxos_instance &inst) {
  cot::duration start = 25s;
  cot::duration duration = 15s;
  co_await cot::after(start);
  size_t leader = inst.replicas.front()->leader_index_;
  set_replica_interconnectivity(inst, leader, 1.0); // keep client connectivity
  co_await cot::after(duration);
  set_replica_interconnectivity(inst, leader, inst.tester.loss);
}

cot::task<> schedule_random(pt_paxos_instance &inst) {
  auto choice = inst.tester.randomness.uniform<int>(0, 3);
  if (choice == 0) {
    co_await schedule_failover(inst);
  } else if (choice == 1) {
    co_await schedule_recovery(inst);
  } else if (choice == 2) {
    co_await schedule_split_brain(inst);
  } else {
    co_await schedule_unstable_mixed(inst);
  }
}

cot::task<> run_failure_schedule(pt_paxos_instance &inst) {
  if (inst.tester.failure_schedule == failure_schedule_kind::failover) {
    co_await schedule_failover(inst);
  } else if (inst.tester.failure_schedule == failure_schedule_kind::recover) {
    co_await schedule_recovery(inst);
  } else if (inst.tester.failure_schedule == failure_schedule_kind::split) {
    co_await schedule_split_brain(inst);
  } else if (inst.tester.failure_schedule == failure_schedule_kind::unstable) {
    co_await schedule_unstable_mixed(inst);
  } else if (inst.tester.failure_schedule == failure_schedule_kind::torture) {
    co_await schedule_torture(inst);
  } else if (inst.tester.failure_schedule == failure_schedule_kind::darias) {
    co_await schedule_darias(inst);
  } else if (inst.tester.failure_schedule == failure_schedule_kind::eric) {
    co_await schedule_eric(inst);
  } else if (inst.tester.failure_schedule ==
             failure_schedule_kind::eric_recover) {
    co_await schedule_eric_recover(inst);
  } else if (inst.tester.failure_schedule ==
             failure_schedule_kind::eric_split) {
    co_await schedule_eric_split_brain(inst);
  } else if (inst.tester.failure_schedule == failure_schedule_kind::random) {
    co_await schedule_random(inst);
  }
}

bool try_one_seed(testinfo &tester, unsigned long seed) {
  cot::reset();
  tester.randomness.seed(seed);

  lockseq_model clients(tester.nreplicas, tester.randomness);
  pt_paxos_instance inst(tester, clients);

  clients.start();
  std::vector<cot::task<>> tasks;
  // Launch one long-lived coroutine per replica
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    tasks.push_back(inst.replicas[s]->run());
  }
  // Schedule failures, graceful client stop, and a hard simulation timeout
  cot::task<> failure_task = run_failure_schedule(inst);
  cot::task<> stop_task = stop_clients_after(clients, 85s);
  cot::task<> timeout_task = clear_after(100s);

  cot::loop();

  size_t best = 0;
  for (size_t s = 1; s != tester.nreplicas; ++s) {
    if (!inst.replica_available[s]) {
      continue; // permanently failed replica is not a valid final baseline
    }
    if (inst.tester.ignored_replica_for_check &&
        s == *inst.tester.ignored_replica_for_check) {
      continue; // explicitly ignored replica, e.g. permanently isolated leader
    }
    if (!inst.replica_available[best] ||
        inst.replicas[s]->last_applied_ > inst.replicas[best]->last_applied_) {
      best = s;
      // choose the most advanced live replica as the reference database
    }
  }

  std::print("{} lock, {} write, {} clear, {} unlock\n", clients.lock_complete,
             clients.write_complete, clients.clear_complete,
             clients.unlock_complete);
  pancy::pancydb &db = inst.replicas[best]->db_;
  if (auto problem = clients.check(db)) {
    std::print(std::clog, "*** FAILURE on seed {} at key {}\n", seed, *problem);
    db.print_near(*problem, std::clog);
    return false; // Client-model invariant failed on the chosen reference db
  } else if (tester.latency_trace) {
    clients.print_latency_samples(std::cout);
  }
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    // skip the baseline and any replicas intentionally left failed
    if (s == best || !inst.replica_available[s]) {
      continue;
    }
    if (auto problem = db.diff(inst.replicas[s]->db_, 2)) {
      std::print(
          std::clog,
          "*** REPLICA DIVERGENCE on seed {} between R{} and R{} at key {}\n",
          seed, best, s, *problem);
      return false; // Live replicas should not diverge in visible database
                    // state
    }
  }
  if (tester.print_db) {
    db.print(std::cout);
    // Optional debugging dump of the final chosen database
  }
  return true;
}

static std::optional<failure_schedule_kind>
parse_failure_schedule(const std::string &s) {
  if (s == "none") {
    return failure_schedule_kind::none;
  } else if (s == "failover") {
    return failure_schedule_kind::failover;
  } else if (s == "recover") {
    return failure_schedule_kind::recover;
  } else if (s == "split") {
    return failure_schedule_kind::split;
  } else if (s == "unstable") {
    return failure_schedule_kind::unstable;
  } else if (s == "torture") {
    return failure_schedule_kind::torture;
  } else if (s == "darias") {
    return failure_schedule_kind::darias;
  } else if (s == "eric") {
    return failure_schedule_kind::eric;
  } else if (s == "eric-recover") {
    return failure_schedule_kind::eric_recover;
  } else if (s == "eric-split") {
    return failure_schedule_kind::eric_split;
  } else if (s == "random") {
    return failure_schedule_kind::random;
  }
  return std::nullopt; // Unknown schedule string from the CLI
}

static struct option options[] = {
    {"count", required_argument, nullptr, 'n'},
    {"seed", required_argument, nullptr, 'S'},
    {"random-seeds", required_argument, nullptr, 'R'},
    {"loss", required_argument, nullptr, 'l'},
    {"failure", required_argument, nullptr, 'f'},
    {"verbose", no_argument, nullptr, 'V'},
    {"print-db", no_argument, nullptr, 'p'},
    {"latency-trace", no_argument, nullptr, 'L'},
    {"quiet", no_argument, nullptr, 'q'},
    {nullptr, 0, nullptr, 0}};

int main(int argc, char *argv[]) {
  testinfo tester;

  std::optional<unsigned long> first_seed;
  unsigned long seed_count = 1;
  // Default to a single random seed unless -S or -R says otherwise

  auto shortopts = short_options_for(options);
  int ch;
  while ((ch = getopt_long(argc, argv, shortopts.c_str(), options, nullptr)) !=
         -1) {
    if (ch == 'S') {
      first_seed = from_str_chars<unsigned long>(optarg);
    } else if (ch == 'R') {
      seed_count = from_str_chars<unsigned long>(optarg);
    } else if (ch == 'l') {
      tester.loss = from_str_chars<double>(optarg);
    } else if (ch == 'n') {
      tester.nreplicas = from_str_chars<size_t>(optarg);
    } else if (ch == 'f') {
      auto mode = parse_failure_schedule(optarg);
      if (!mode) {
        std::print(std::cerr, "Unknown failure schedule\n");
        return 1;
      }
      tester.failure_schedule = *mode;
    } else if (ch == 'V') {
      tester.verbose = true;
    } else if (ch == 'p') {
      tester.print_db = true;
    } else if (ch == 'L') {
      tester.latency_trace = true;
    } else {
      std::print(std::cerr, "Unknown option\n");
      return 1;
    }
  }

  bool ok;
  if (first_seed) {
    // Deterministic single-seed run
    ok = try_one_seed(tester, *first_seed);
  } else {
    std::mt19937_64 seed_generator = randomly_seeded<std::mt19937_64>();
    // Random seed campaign mode
    for (unsigned long i = 0; i != seed_count; ++i) {
      if (i > 0 && i % 1000 == 0) {
        std::print(std::cerr, ".");
      }
      unsigned long seed = seed_generator();
      ok = try_one_seed(tester, seed);
      if (!ok) {
        break; // Stop immediately on first failing seed to aid debugging
      }
    }
    if (ok && seed_count >= 1000) {
      std::print(std::cerr, "\n");
    }
  }
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}

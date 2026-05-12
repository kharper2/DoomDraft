#include "collab_model.hh"
#include "doc_state.hh"
#include "netsim.hh"
#include "pancydb.hh"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <deque>
#include <map>
#include <optional>
#include <print>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

namespace cot = cotamer;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Paxos infrastructure (copied from pt-paxos.cc; only try_one_seed and main
// differ from the original).
// ---------------------------------------------------------------------------

enum class failure_schedule_kind {
  none,
  failover,
  recover,
  split,
  unstable,
  torture,
  random
};

struct testinfo {
  random_source randomness;
  double loss = 0.01;
  bool verbose = false;
  bool print_db = false;
  size_t nreplicas = 3;
  size_t initial_leader = 0;
  std::optional<size_t> ignored_replica_for_check;
  failure_schedule_kind failure_schedule = failure_schedule_kind::none;

  // Per-message simulated network delays. Defaults match the historical bench
  // values so collab-bench.sh seeds run with the same timing as before. The
  // server (pt-collab-server) overrides these to 0 via CLI flags because
  // in-process replicas have no real link latency and the simulated delays
  // trigger Paxos elections under wall-clock time.
  cot::duration link_delay = 5ms;
  cot::duration send_delay = 1ms;
  cot::duration receive_delay = 1ms;

  template <typename T> void configure_port(netsim::port<T>& port) {
    port.set_verbose(verbose);
    port.set_receive_delay(receive_delay);
  }
  template <typename T> void configure_channel(netsim::channel<T>& chan) {
    chan.set_loss(loss);
    chan.set_verbose(verbose);
    chan.set_link_delay(link_delay);
    chan.set_send_delay(send_delay);
  }
  template <typename T> void configure_quiet_channel(netsim::channel<T>& chan) {
    chan.set_loss(loss);
    chan.set_link_delay(link_delay);
    chan.set_send_delay(send_delay);
  }
};

struct pt_paxos_instance;

struct log_entry {
  uint64_t slot;
  uint64_t round;
  pancy::request req;
};

struct probe_message {
  uint64_t round;
  size_t leader;
  uint64_t last_log_index;
  uint64_t last_log_round;
};

struct prepare_message {
  uint64_t round;
  size_t server;
  bool promised;
};

struct propose_message {
  uint64_t round;
  size_t leader;
  uint64_t prev_log_index;
  uint64_t prev_log_round;
  uint64_t decide_through;
  uint64_t trim_through;
  std::vector<log_entry> entries;
};

struct ack_message {
  uint64_t round;
  size_t server;
  bool success;
  uint64_t match_index;
  uint64_t next_index;
};

using paxos_message =
    std::variant<probe_message, prepare_message, propose_message, ack_message>;

enum class replica_role { follower, candidate, leader };

struct pt_paxos_replica {
  size_t index_;
  size_t nreplicas_;
  size_t leader_index_;
  netsim::port<pancy::request> from_clients_;
  netsim::port<paxos_message> from_replicas_;
  netsim::channel<pancy::response> to_clients_;
  std::vector<std::unique_ptr<netsim::channel<paxos_message>>> to_replicas_;
  pancy::pancydb db_;

  random_source& randomness_;
  replica_role role_ = replica_role::follower;
  uint64_t current_round_ = 0;
  std::optional<size_t> promised_leader_;
  std::vector<bool> prepare_ok_;

  std::map<uint64_t, log_entry> log_;
  uint64_t next_slot_ = 1;
  uint64_t commit_index_ = 0;
  uint64_t last_applied_ = 0;
  uint64_t trim_through_ = 0;
  uint64_t trim_round_ = 0;

  std::vector<uint64_t> match_index_;
  std::vector<uint64_t> next_index_;
  std::unordered_map<uint64_t, pancy::response> response_cache_;
  std::unordered_map<uint64_t, uint64_t> pending_clients_;
  std::deque<pancy::request> pending_client_requests_;
  std::unordered_set<uint64_t> pending_client_serials_;
  std::optional<cot::steady_time_point> client_batch_deadline_;

  cot::duration election_timeout_ = 250ms;
  static constexpr cot::duration heartbeat_interval_ = 50ms;
  static constexpr cot::duration client_batch_window_ = 5ms;
  static constexpr size_t append_batch_size_ = 16;
  static constexpr uint64_t trim_retention_ = 256;

  pt_paxos_replica(size_t index, size_t nreplicas, random_source&);
  void initialize(pt_paxos_instance&);

  size_t quorum_size() const { return nreplicas_ / 2 + 1; }
  bool has_quorum(size_t n) const { return n >= quorum_size(); }
  uint64_t last_log_index() const { return next_slot_ - 1; }
  uint64_t last_log_round() const;
  uint64_t round_at(uint64_t slot) const;
  cot::duration random_election_timeout();
  void reset_election_timeout();
  bool leader_is_up_to_date(uint64_t last_log_index, uint64_t last_log_round) const;
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
  cot::task<> handle_probe(const probe_message& msg);
  cot::task<> handle_prepare(const prepare_message& msg);
  cot::task<> handle_propose(const propose_message& msg);
  cot::task<> handle_ack(const ack_message& msg);
  cot::task<> handle_replica_message(paxos_message msg);
  cot::task<> handle_client_request(pancy::request req);
  cot::task<> run();
};

struct pt_paxos_instance {
  testinfo& tester;
  client_model& clients;
  std::vector<std::unique_ptr<pt_paxos_replica>> replicas;
  std::vector<bool> replica_available;

  pt_paxos_instance(testinfo&, client_model&);

  double base_loss() const { return tester.loss; }

  void heal_all_links();
  void fail_replica(size_t rid);
  void recover_replica(size_t rid);
  void partition_one_against_rest(size_t rid);
};

namespace std {

template <typename CharT> struct formatter<log_entry, CharT> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const log_entry& m, FormatContext& ctx) const {
    auto req = std::visit([](const auto& reqt) { return std::format("{}", reqt); }, m.req);
    return std::format_to(ctx.out(), "E(S{}, R{}, {})", m.slot, m.round, req);
  }
};

template <typename CharT> struct formatter<probe_message, CharT> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const probe_message& m, FormatContext& ctx) const {
    return std::format_to(ctx.out(), "PROBE(R{}, L{}, last=S{}/R{})",
                          m.round, m.leader, m.last_log_index, m.last_log_round);
  }
};

template <typename CharT> struct formatter<prepare_message, CharT> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const prepare_message& m, FormatContext& ctx) const {
    return std::format_to(ctx.out(), "PREPARE(R{}, S{}, {})",
                          m.round, m.server, m.promised ? "promise" : "deny");
  }
};

template <typename CharT> struct formatter<propose_message, CharT> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const propose_message& m, FormatContext& ctx) const {
    if (!m.entries.empty()) {
      std::string payload;
      if (m.entries.size() == 1) {
        payload = std::format("{}", m.entries.front());
      } else {
        payload = std::format("{}..{} ({} entries)", m.entries.front(),
                              m.entries.back(), m.entries.size());
      }
      return std::format_to(ctx.out(),
          "PROPOSE(R{}, L{}, prev=S{}/R{}, decide=S{}, trim=S{}, {})",
          m.round, m.leader, m.prev_log_index, m.prev_log_round,
          m.decide_through, m.trim_through, payload);
    }
    return std::format_to(ctx.out(),
        "DECIDE(R{}, L{}, prev=S{}/R{}, decide=S{}, trim=S{})",
        m.round, m.leader, m.prev_log_index, m.prev_log_round,
        m.decide_through, m.trim_through);
  }
};

template <typename CharT> struct formatter<ack_message, CharT> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const ack_message& m, FormatContext& ctx) const {
    return std::format_to(ctx.out(), "ACK(R{}, S{}, {}, match=S{}, next=S{})",
                          m.round, m.server, m.success ? "ok" : "retry",
                          m.match_index, m.next_index);
  }
};

} // namespace std

namespace netsim {

template <> struct message_traits<paxos_message> {
  static std::string print_transform(const paxos_message& m) {
    return std::visit([](const auto& msg) { return std::format("{}", msg); }, m);
  }
};

} // namespace netsim

pt_paxos_replica::pt_paxos_replica(size_t index, size_t nreplicas,
                                   random_source& randomness)
    : index_(index), nreplicas_(nreplicas), leader_index_(index),
      from_clients_(randomness, std::format("R{}", index_)),
      from_replicas_(randomness, std::format("R{}/r", index_)),
      to_clients_(randomness, from_clients_.id()), to_replicas_(nreplicas),
      randomness_(randomness), prepare_ok_(nreplicas, false),
      match_index_(nreplicas, 0), next_index_(nreplicas, 1) {
  for (size_t s = 0; s != nreplicas_; ++s) {
    to_replicas_[s].reset(
        new netsim::channel<paxos_message>(randomness, from_clients_.id()));
  }
  reset_election_timeout();
}

void pt_paxos_replica::initialize(pt_paxos_instance& inst) {
  leader_index_ = inst.tester.initial_leader;
  role_ = index_ == leader_index_ ? replica_role::leader : replica_role::follower;
  inst.clients.connect_replica(index_, from_clients_, to_clients_);
  inst.tester.configure_port(from_clients_);
  inst.tester.configure_port(from_replicas_);
  inst.tester.configure_channel(to_clients_);
  inst.tester.configure_quiet_channel(inst.clients.request_channel(index_));
  for (size_t s = 0; s != nreplicas_; ++s) {
    to_replicas_[s]->connect(inst.replicas[s]->from_replicas_);
    inst.tester.configure_channel(*to_replicas_[s]);
  }
  if (role_ == replica_role::leader) {
    become_leader();
  }
}

pt_paxos_instance::pt_paxos_instance(testinfo& tester, client_model& clients)
    : tester(tester), clients(clients), replicas(tester.nreplicas),
      replica_available(tester.nreplicas, true) {
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replicas[s].reset(new pt_paxos_replica(s, tester.nreplicas, tester.randomness));
  }
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replicas[s]->initialize(*this);
  }
}

void pt_paxos_instance::heal_all_links() {
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    clients.request_channel(s).set_loss(base_loss());
    replicas[s]->to_clients_.set_loss(base_loss());
  }
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    for (size_t d = 0; d != tester.nreplicas; ++d) {
      replicas[s]->to_replicas_[d]->set_loss(base_loss());
    }
  }
}

void pt_paxos_instance::fail_replica(size_t rid) {
  replica_available[rid] = false;
  clients.request_channel(rid).set_loss(1.0);
  replicas[rid]->to_clients_.set_loss(1.0);
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replicas[rid]->to_replicas_[s]->set_loss(1.0);
    replicas[s]->to_replicas_[rid]->set_loss(1.0);
  }
}

void pt_paxos_instance::recover_replica(size_t rid) {
  replica_available[rid] = true;
  clients.request_channel(rid).set_loss(base_loss());
  replicas[rid]->to_clients_.set_loss(base_loss());
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replicas[rid]->to_replicas_[s]->set_loss(base_loss());
    replicas[s]->to_replicas_[rid]->set_loss(base_loss());
  }
}

void pt_paxos_instance::partition_one_against_rest(size_t rid) {
  heal_all_links();
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    if (s == rid) continue;
    replicas[rid]->to_replicas_[s]->set_loss(1.0);
    replicas[s]->to_replicas_[rid]->set_loss(1.0);
  }
}

uint64_t pt_paxos_replica::round_at(uint64_t slot) const {
  if (slot == 0) return 0;
  if (slot == trim_through_) return trim_round_;
  if (auto it = log_.find(slot); it != log_.end()) return it->second.round;
  return 0;
}

uint64_t pt_paxos_replica::last_log_round() const {
  return round_at(last_log_index());
}

cot::duration pt_paxos_replica::random_election_timeout() {
  return randomness_.uniform(200ms, 350ms);
}

void pt_paxos_replica::reset_election_timeout() {
  election_timeout_ = random_election_timeout();
}

bool pt_paxos_replica::leader_is_up_to_date(uint64_t last_log_index,
                                             uint64_t last_log_round) const {
  if (last_log_round != this->last_log_round()) {
    return last_log_round > this->last_log_round();
  }
  return last_log_index >= this->last_log_index();
}

bool pt_paxos_replica::prefix_matches(uint64_t prev_log_index,
                                      uint64_t prev_log_round) const {
  if (prev_log_index == 0) return trim_through_ == 0;
  if (prev_log_index < trim_through_) return false;
  if (prev_log_index == trim_through_) return prev_log_round == trim_round_;
  auto it = log_.find(prev_log_index);
  return it != log_.end() && it->second.round == prev_log_round;
}

void pt_paxos_replica::become_follower(uint64_t round, size_t leader_hint) {
  if (round > current_round_) {
    current_round_ = round;
    promised_leader_.reset();
  }
  role_ = replica_role::follower;
  leader_index_ = leader_hint;
  prepare_ok_.assign(nreplicas_, false);
  pending_clients_.clear();
  pending_client_requests_.clear();
  pending_client_serials_.clear();
  client_batch_deadline_.reset();
  reset_election_timeout();
}

void pt_paxos_replica::become_leader() {
  role_ = replica_role::leader;
  leader_index_ = index_;
  promised_leader_ = index_;
  prepare_ok_.assign(nreplicas_, false);
  match_index_.assign(nreplicas_, 0);
  next_index_.assign(nreplicas_, last_log_index() + 1);
  match_index_[index_] = last_log_index();
  next_index_[index_] = last_log_index() + 1;
}

void pt_paxos_replica::trim_log_prefix() {
  while (!log_.empty() && log_.begin()->first <= trim_through_) {
    trim_round_ = log_.begin()->second.round;
    log_.erase(log_.begin());
  }
}

void pt_paxos_replica::update_commit_index() {
  if (role_ != replica_role::leader) return;
  for (uint64_t idx = last_log_index(); idx > commit_index_; --idx) {
    if (round_at(idx) != current_round_) continue;
    size_t replicated = 1;
    for (size_t s = 0; s != nreplicas_; ++s) {
      if (s != index_ && match_index_[s] >= idx) ++replicated;
    }
    if (has_quorum(replicated)) { commit_index_ = idx; return; }
  }
}

void pt_paxos_replica::update_trim_watermark() {
  if (role_ != replica_role::leader) return;
  uint64_t min_match = match_index_[index_];
  for (size_t s = 0; s != nreplicas_; ++s) {
    min_match = std::min(min_match, match_index_[s]);
  }
  min_match = std::min(min_match, commit_index_);
  uint64_t target_trim = min_match > trim_retention_ ? min_match - trim_retention_ : 0;
  if (target_trim > trim_through_) {
    trim_through_ = target_trim;
    trim_log_prefix();
  }
}

cot::task<> pt_paxos_replica::apply_committed_entries() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    auto it = log_.find(last_applied_);
    if (it == log_.end()) continue;
    uint64_t serial = pancy::message_serial(it->second.req);
    if (!response_cache_.contains(serial)) {
      response_cache_.emplace(serial, db_.process_req(it->second.req));
    }
    if (role_ == replica_role::leader) {
      auto pit = pending_clients_.find(serial);
      if (pit != pending_clients_.end() && pit->second == last_applied_) {
        co_await to_clients_.send(response_cache_.at(serial));
        pending_clients_.erase(pit);
        pending_client_serials_.erase(serial);
      }
    }
  }
  trim_log_prefix();
}

cot::task<> pt_paxos_replica::flush_pending_client_requests() {
  if (role_ != replica_role::leader || pending_client_requests_.empty()) {
    client_batch_deadline_.reset();
    co_return;
  }
  while (role_ == replica_role::leader && !pending_client_requests_.empty()) {
    size_t batch = std::min(pending_client_requests_.size(), append_batch_size_);
    for (size_t i = 0; i != batch; ++i) {
      auto req = std::move(pending_client_requests_.front());
      pending_client_requests_.pop_front();
      uint64_t serial = pancy::message_serial(req);
      uint64_t slot = next_slot_++;
      log_.emplace(slot, log_entry{slot, current_round_, std::move(req)});
      pending_clients_.emplace(serial, slot);
    }
    match_index_[index_] = last_log_index();
    next_index_[index_] = last_log_index() + 1;
    co_await broadcast_propose();
    update_commit_index();
    co_await apply_committed_entries();
    update_trim_watermark();
  }
  if (pending_client_requests_.empty()) {
    client_batch_deadline_.reset();
  } else {
    client_batch_deadline_ = cot::steady_now() + client_batch_window_;
  }
}

cot::task<> pt_paxos_replica::send_propose_to(size_t peer) {
  if (peer == index_ || role_ != replica_role::leader) co_return;
  uint64_t next = std::max(next_index_[peer], trim_through_ + 1);
  next_index_[peer] = next;
  uint64_t prev = next - 1;
  std::vector<log_entry> entries;
  auto it = log_.lower_bound(next);
  while (it != log_.end() && entries.size() < append_batch_size_) {
    if (it->first != next + entries.size()) break;
    entries.push_back(it->second);
    ++it;
  }
  co_await to_replicas_[peer]->send(
      propose_message{current_round_, index_, prev, round_at(prev),
                      commit_index_, trim_through_, std::move(entries)});
}

cot::task<> pt_paxos_replica::broadcast_propose() {
  for (size_t s = 0; s != nreplicas_; ++s) {
    if (s != index_) co_await send_propose_to(s);
  }
}

cot::task<> pt_paxos_replica::start_probe_round() {
  role_ = replica_role::candidate;
  ++current_round_;
  leader_index_ = index_;
  promised_leader_ = index_;
  prepare_ok_.assign(nreplicas_, false);
  prepare_ok_[index_] = true;
  reset_election_timeout();
  probe_message msg{current_round_, index_, last_log_index(), last_log_round()};
  for (size_t s = 0; s != nreplicas_; ++s) {
    if (s != index_) co_await to_replicas_[s]->send(msg);
  }
  if (has_quorum(1)) {
    become_leader();
    co_await broadcast_propose();
  }
}

cot::task<> pt_paxos_replica::handle_probe(const probe_message& msg) {
  if (msg.round < current_round_) {
    co_await to_replicas_[msg.leader]->send(
        prepare_message{current_round_, index_, false});
    co_return;
  }
  if (msg.round > current_round_) become_follower(msg.round, leader_index_);
  bool promise = false;
  if ((!promised_leader_ || *promised_leader_ == msg.leader) &&
      leader_is_up_to_date(msg.last_log_index, msg.last_log_round)) {
    promised_leader_ = msg.leader;
    leader_index_ = msg.leader;
    reset_election_timeout();
    promise = true;
  }
  co_await to_replicas_[msg.leader]->send(
      prepare_message{current_round_, index_, promise});
}

cot::task<> pt_paxos_replica::handle_prepare(const prepare_message& msg) {
  if (msg.round > current_round_) {
    become_follower(msg.round, leader_index_);
    co_return;
  }
  if (role_ != replica_role::candidate || msg.round != current_round_ ||
      !msg.promised || prepare_ok_[msg.server]) {
    co_return;
  }
  prepare_ok_[msg.server] = true;
  size_t promises = 0;
  for (bool ok : prepare_ok_) promises += ok;
  if (has_quorum(promises)) {
    become_leader();
    co_await broadcast_propose();
  }
}

cot::task<> pt_paxos_replica::handle_propose(const propose_message& msg) {
  if (msg.round < current_round_) {
    co_await to_replicas_[msg.leader]->send(ack_message{
        current_round_, index_, false, last_log_index(), last_log_index() + 1});
    co_return;
  }
  become_follower(msg.round, msg.leader);
  if (!prefix_matches(msg.prev_log_index, msg.prev_log_round)) {
    uint64_t next_index = trim_through_ + 1;
    if (msg.prev_log_index > trim_through_) {
      next_index = std::min(msg.prev_log_index, last_log_index() + 1);
    }
    co_await to_replicas_[msg.leader]->send(
        ack_message{current_round_, index_, false, last_log_index(), next_index});
    co_return;
  }
  uint64_t match_index = msg.prev_log_index;
  for (const auto& entry : msg.entries) {
    uint64_t slot = entry.slot;
    if (slot <= trim_through_) { match_index = slot; continue; }
    if (auto it = log_.find(slot);
        it != log_.end() && it->second.round != entry.round) {
      if (commit_index_ >= slot) {
        co_await to_replicas_[msg.leader]->send(
            ack_message{current_round_, index_, false, last_log_index(), slot});
        co_return;
      }
      auto erase_it = log_.lower_bound(slot);
      while (erase_it != log_.end()) erase_it = log_.erase(erase_it);
      next_slot_ = slot;
    }
    if (!log_.contains(slot)) {
      log_.emplace(slot, entry);
      next_slot_ = std::max(next_slot_, slot + 1);
    }
    match_index = slot;
  }
  if (msg.decide_through > commit_index_) {
    commit_index_ = std::min(msg.decide_through, last_log_index());
  }
  co_await apply_committed_entries();
  if (msg.trim_through > trim_through_) {
    trim_through_ = std::min(msg.trim_through, last_applied_);
    trim_log_prefix();
  }
  co_await to_replicas_[msg.leader]->send(
      ack_message{current_round_, index_, true, match_index, match_index + 1});
}

cot::task<> pt_paxos_replica::handle_ack(const ack_message& msg) {
  if (msg.round > current_round_) {
    become_follower(msg.round, leader_index_);
    co_return;
  }
  if (role_ != replica_role::leader || msg.round != current_round_) co_return;
  if (msg.success) {
    match_index_[msg.server] = std::max(match_index_[msg.server], msg.match_index);
    next_index_[msg.server] = std::max(next_index_[msg.server], msg.next_index);
    update_commit_index();
    co_await apply_committed_entries();
    update_trim_watermark();
    if (next_index_[msg.server] <= last_log_index()) {
      co_await send_propose_to(msg.server);
    }
    co_return;
  }
  uint64_t fallback = msg.next_index;
  if (fallback == 0) fallback = 1;
  if (fallback >= next_index_[msg.server]) {
    fallback = next_index_[msg.server] > 1 ? next_index_[msg.server] - 1 : 1;
  }
  next_index_[msg.server] = std::max(fallback, trim_through_ + 1);
  co_await send_propose_to(msg.server);
}

cot::task<> pt_paxos_replica::handle_replica_message(paxos_message msg) {
  if (auto* probe = std::get_if<probe_message>(&msg)) {
    co_await handle_probe(*probe);
  } else if (auto* prepare = std::get_if<prepare_message>(&msg)) {
    co_await handle_prepare(*prepare);
  } else if (auto* propose = std::get_if<propose_message>(&msg)) {
    co_await handle_propose(*propose);
  } else if (auto* ack = std::get_if<ack_message>(&msg)) {
    co_await handle_ack(*ack);
  }
}

cot::task<> pt_paxos_replica::handle_client_request(pancy::request req) {
  if (role_ != replica_role::leader) {
    size_t redirect = role_ == replica_role::candidate ? index_ : leader_index_;
    co_await to_clients_.send(pancy::redirection_response{
        pancy::response_header(req, pancy::errc::redirect), redirect});
    co_return;
  }
  uint64_t serial = pancy::message_serial(req);
  if (auto it = response_cache_.find(serial); it != response_cache_.end()) {
    co_await to_clients_.send(it->second);
    co_return;
  }
  if (pending_client_serials_.contains(serial)) co_return;
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
        client_batch_deadline_ && cot::steady_now() >= *client_batch_deadline_) {
      co_await flush_pending_client_requests();
      continue;
    }
    if (role_ == replica_role::leader) {
      if (!pending_client_requests_.empty() && client_batch_deadline_) {
        auto delay = *client_batch_deadline_ - cot::steady_now();
        if (delay < 0ns) delay = 0ns;
        auto ret = co_await cot::first(from_clients_.receive(),
                                       from_replicas_.receive(),
                                       cot::after(delay));
        if (ret.index() == 0) co_await handle_client_request(std::get<0>(ret));
        else if (ret.index() == 1) co_await handle_replica_message(std::get<1>(ret));
        else co_await flush_pending_client_requests();
      } else {
        auto ret = co_await cot::first(from_clients_.receive(),
                                       from_replicas_.receive(),
                                       cot::after(heartbeat_interval_));
        if (ret.index() == 0) co_await handle_client_request(std::get<0>(ret));
        else if (ret.index() == 1) co_await handle_replica_message(std::get<1>(ret));
        else co_await broadcast_propose();
      }
      continue;
    }
    auto ret = co_await cot::first(from_clients_.receive(),
                                   from_replicas_.receive(),
                                   cot::after(election_timeout_));
    if (ret.index() == 0) co_await handle_client_request(std::get<0>(ret));
    else if (ret.index() == 1) co_await handle_replica_message(std::get<1>(ret));
    else co_await start_probe_round();
  }
}

// ---------------------------------------------------------------------------
// Failure schedules (subset of pt-paxos.cc, enough for collab-bench.sh)
// ---------------------------------------------------------------------------

cot::task<> clear_after(cot::duration d) {
  co_await cot::after(d);
  cot::clear();
}

cot::task<> stop_clients_after(client_model& clients, cot::duration d) {
  co_await cot::after(d);
  clients.stop();
}

cot::task<> schedule_failover(pt_paxos_instance& inst) {
  co_await cot::after(20s);
  inst.fail_replica(inst.tester.initial_leader);
}

cot::task<> schedule_recovery(pt_paxos_instance& inst) {
  co_await cot::after(20s);
  inst.fail_replica(inst.tester.initial_leader);
  co_await cot::after(25s);
  inst.recover_replica(inst.tester.initial_leader);
}

cot::task<> schedule_split_brain(pt_paxos_instance& inst) {
  if (inst.replicas.size() <= 1) co_return;
  co_await cot::after(20s);
  inst.partition_one_against_rest(inst.tester.initial_leader);
  co_await cot::after(25s);
  inst.heal_all_links();
}

cot::task<> schedule_unstable_mixed(pt_paxos_instance& inst) {
  if (inst.tester.nreplicas <= 1) co_return;
  co_await cot::after(20s);
  std::vector<size_t> followers;
  followers.reserve(inst.tester.nreplicas - 1);
  for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
    if (s != inst.tester.initial_leader) followers.push_back(s);
  }
  std::shuffle(followers.begin(), followers.end(),
               inst.tester.randomness.engine());
  size_t connected_count = std::min(inst.tester.nreplicas,
                                    inst.tester.nreplicas / 2 + 2);
  connected_count = std::max<size_t>(2, connected_count);
  std::vector<size_t> connected_group{inst.tester.initial_leader};
  connected_group.insert(connected_group.end(), followers.begin(),
                         followers.begin() + (connected_count - 1));
  std::vector<size_t> isolated_group(followers.begin() + (connected_count - 1),
                                     followers.end());
  auto set_partition_loss = [&](double loss) {
    for (size_t l : connected_group)
      for (size_t r : isolated_group) {
        inst.replicas[l]->to_replicas_[r]->set_loss(loss);
        inst.replicas[r]->to_replicas_[l]->set_loss(loss);
      }
  };
  if (!isolated_group.empty()) set_partition_loss(1.0);
  auto partition_end = cot::steady_now() + 25s;
  while (cot::steady_now() < partition_end) {
    size_t victim = connected_group[inst.tester.randomness.uniform<size_t>(
        0, connected_group.size() - 1)];
    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(1500ms, 3500ms));
    inst.recover_replica(victim);
    if (!isolated_group.empty()) set_partition_loss(1.0);
    co_await cot::after(inst.tester.randomness.uniform(500ms, 1500ms));
  }
  inst.heal_all_links();
  auto churn_end = cot::steady_now() + 20s;
  while (cot::steady_now() < churn_end) {
    size_t victim = inst.tester.randomness.uniform<size_t>(0, inst.tester.nreplicas - 1);
    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(1s, 4s));
    inst.recover_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(250ms, 2s));
  }
  inst.heal_all_links();
  for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
    if (!inst.replica_available[s]) inst.recover_replica(s);
  }
}

cot::task<> schedule_torture(pt_paxos_instance& inst) {
  if (inst.tester.nreplicas <= 1) co_return;
  co_await cot::after(12s);
  std::vector<size_t> followers;
  for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
    if (s != inst.tester.initial_leader) followers.push_back(s);
  }
  std::shuffle(followers.begin(), followers.end(), inst.tester.randomness.engine());
  size_t connected_count = std::min(inst.tester.nreplicas,
                                    inst.tester.nreplicas / 2 + 2);
  connected_count = std::max<size_t>(2, connected_count);
  std::vector<size_t> connected_group{inst.tester.initial_leader};
  connected_group.insert(connected_group.end(), followers.begin(),
                         followers.begin() + (connected_count - 1));
  std::vector<size_t> isolated_group(followers.begin() + (connected_count - 1),
                                     followers.end());
  auto choose_from = [&](const std::vector<size_t>& group) -> size_t {
    return group[inst.tester.randomness.uniform<size_t>(0, group.size() - 1)];
  };
  auto live_leader = [&]() -> std::optional<size_t> {
    for (size_t s = 0; s != inst.tester.nreplicas; ++s) {
      if (inst.replica_available[s] &&
          inst.replicas[s]->role_ == replica_role::leader) return s;
    }
    return std::nullopt;
  };
  auto set_partition_loss = [&](double loss) {
    for (size_t l : connected_group)
      for (size_t r : isolated_group) {
        inst.replicas[l]->to_replicas_[r]->set_loss(loss);
        inst.replicas[r]->to_replicas_[l]->set_loss(loss);
      }
  };
  if (!isolated_group.empty()) set_partition_loss(1.0);
  auto partition_end = cot::steady_now() + 20s;
  while (cot::steady_now() < partition_end) {
    size_t victim = choose_from(connected_group);
    if (auto leader = live_leader();
        leader && std::find(connected_group.begin(), connected_group.end(), *leader)
                      != connected_group.end()) {
      victim = *leader;
    }
    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(250ms, 1200ms));
    inst.recover_replica(victim);
    if (!isolated_group.empty()) {
      set_partition_loss(1.0);
      if (inst.tester.randomness.coin_flip(0.75)) {
        size_t fringe = choose_from(isolated_group);
        inst.fail_replica(fringe);
        co_await cot::after(inst.tester.randomness.uniform(200ms, 900ms));
        inst.recover_replica(fringe);
        set_partition_loss(1.0);
      }
    }
    co_await cot::after(inst.tester.randomness.uniform(150ms, 850ms));
  }
  inst.heal_all_links();
  auto leader_end = cot::steady_now() + 15s;
  while (cot::steady_now() < leader_end) {
    size_t victim = choose_from(connected_group);
    if (auto leader = live_leader()) victim = *leader;
    inst.fail_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(1s, 2400ms));
    inst.recover_replica(victim);
    co_await cot::after(inst.tester.randomness.uniform(100ms, 700ms));
  }
}

cot::task<> schedule_random(pt_paxos_instance& inst) {
  auto choice = inst.tester.randomness.uniform<int>(0, 3);
  if (choice == 0) co_await schedule_failover(inst);
  else if (choice == 1) co_await schedule_recovery(inst);
  else if (choice == 2) co_await schedule_split_brain(inst);
  else co_await schedule_unstable_mixed(inst);
}

cot::task<> run_failure_schedule(pt_paxos_instance& inst) {
  if (inst.tester.failure_schedule == failure_schedule_kind::failover)
    co_await schedule_failover(inst);
  else if (inst.tester.failure_schedule == failure_schedule_kind::recover)
    co_await schedule_recovery(inst);
  else if (inst.tester.failure_schedule == failure_schedule_kind::split)
    co_await schedule_split_brain(inst);
  else if (inst.tester.failure_schedule == failure_schedule_kind::unstable)
    co_await schedule_unstable_mixed(inst);
  else if (inst.tester.failure_schedule == failure_schedule_kind::torture)
    co_await schedule_torture(inst);
  else if (inst.tester.failure_schedule == failure_schedule_kind::random)
    co_await schedule_random(inst);
}

// ---------------------------------------------------------------------------
// try_one_seed: run one simulation, then verify cross-replica convergence.
// Unlike pt-paxos.cc which uses db.diff(), we compare reconstructed text
// from every live replica using reconstruct(read_ops(db, doc_id)).
// ---------------------------------------------------------------------------

static constexpr std::string_view DOC_ID = "main";
static constexpr size_t NCLIENTS = 8;

bool try_one_seed(testinfo& tester, unsigned long seed) {
  cot::reset();
  tester.randomness.seed(seed);

  collab_model clients(tester.nreplicas, tester.randomness,
                       std::string(DOC_ID), NCLIENTS);
  pt_paxos_instance inst(tester, clients);

  clients.start();
  std::vector<cot::task<>> tasks;
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    tasks.push_back(inst.replicas[s]->run());
  }
  cot::task<> failure_task = run_failure_schedule(inst);
  cot::task<> stop_task = stop_clients_after(clients, 85s);
  cot::task<> timeout_task = clear_after(100s);

  cot::loop();

  std::print("{} submitted, {} committed, {} transformed\n",
             clients.ops_submitted, clients.ops_committed, clients.ops_transformed);

  // Find the best (most advanced) live replica as the reference.
  size_t best = 0;
  for (size_t s = 1; s != tester.nreplicas; ++s) {
    if (!inst.replica_available[s]) continue;
    if (!inst.replica_available[best] ||
        inst.replicas[s]->last_applied_ > inst.replicas[best]->last_applied_) {
      best = s;
    }
  }

  // Per-replica invariant check (op deserialization).
  pancy::pancydb& db = inst.replicas[best]->db_;
  if (auto problem = clients.check(db)) {
    std::print(std::clog, "*** INVARIANT FAILURE on seed {} at key {}\n",
               seed, *problem);
    return false;
  }

  // Cross-replica convergence: all live replicas must reconstruct the same text.
  auto best_ops = collab::read_ops(db, DOC_ID);
  std::string expected_text = collab::reconstruct(best_ops);
  if (tester.print_db) {
    std::print("Reconstructed text (R{}): {:?} ({} chars, {} ops)\n",
               best, expected_text, expected_text.size(), best_ops.size());
  }
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    if (s == best || !inst.replica_available[s]) continue;
    auto s_ops = collab::read_ops(inst.replicas[s]->db_, DOC_ID);
    std::string s_text = collab::reconstruct(s_ops);
    if (s_text != expected_text) {
      std::print(std::clog,
                 "*** TEXT DIVERGENCE on seed {} between R{} and R{}\n"
                 "    R{}: {:?}\n    R{}: {:?}\n",
                 seed, best, s, best, expected_text, s, s_text);
      return false;
    }
  }

  // Stricter invariant for writeups: every *available* replica matches the
  // current Raft leader's reconstructed doc (not only the max-applied peer).
  size_t leader_idx = tester.nreplicas;
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    if (!inst.replica_available[s]) continue;
    if (inst.replicas[s]->role_ == replica_role::leader) {
      leader_idx = s;
      break;
    }
  }
  if (leader_idx < tester.nreplicas) {
    auto leader_ops = collab::read_ops(inst.replicas[leader_idx]->db_, DOC_ID);
    const std::string leader_text = collab::reconstruct(leader_ops);
    for (size_t s = 0; s != tester.nreplicas; ++s) {
      if (!inst.replica_available[s]) continue;
      auto s_ops = collab::read_ops(inst.replicas[s]->db_, DOC_ID);
      const std::string s_text = collab::reconstruct(s_ops);
      if (s_text != leader_text) {
        std::print(std::clog,
                   "*** TEXT DIVERGENCE vs leader R{} on seed {} (leader R{})\n"
                   "    R{}: {:?}\n    R{}: {:?}\n",
                   s, seed, leader_idx, leader_idx, leader_text, s, s_text);
        return false;
      }
    }
  }

  return true;
}

#ifndef PT_COLLAB_SERVER
static const char* failure_schedule_label(failure_schedule_kind k) {
  switch (k) {
  case failure_schedule_kind::none:
    return "none";
  case failure_schedule_kind::failover:
    return "failover";
  case failure_schedule_kind::recover:
    return "recover";
  case failure_schedule_kind::split:
    return "split";
  case failure_schedule_kind::unstable:
    return "unstable";
  case failure_schedule_kind::torture:
    return "torture";
  case failure_schedule_kind::random:
    return "random";
  default:
    return "?";
  }
}

// Fixed-seed Paxos+collab runs with failure schedules (fast simulated time).
// Encodes: after schedule completes, no TEXT DIVERGENCE and all live replicas
// match the leader's reconstructed document.
static bool run_scheduled_collab_convergence_tests() {
  const struct {
    failure_schedule_kind kind;
    unsigned long seed;
  } cases[] = {
      {failure_schedule_kind::unstable, 0x42},
      {failure_schedule_kind::unstable, 0xC0DA26200427ULL},
      {failure_schedule_kind::failover, 99},
      {failure_schedule_kind::recover, 7},
  };
  testinfo t{};
  t.nreplicas = 3;
  t.loss = 0.0;
  for (const auto& c : cases) {
    t.failure_schedule = c.kind;
    std::print("collab convergence: -f {} -S {}\n",
               failure_schedule_label(c.kind), c.seed);
    if (!try_one_seed(t, c.seed)) {
      std::print(std::clog,
                 "*** scheduled collab convergence failed (-f {} -S {})\n",
                 failure_schedule_label(c.kind), c.seed);
      return false;
    }
  }
  return true;
}

static std::optional<failure_schedule_kind>
parse_failure_schedule(const std::string& s) {
  if (s == "none")     return failure_schedule_kind::none;
  if (s == "failover") return failure_schedule_kind::failover;
  if (s == "recover")  return failure_schedule_kind::recover;
  if (s == "split")    return failure_schedule_kind::split;
  if (s == "unstable") return failure_schedule_kind::unstable;
  if (s == "torture")  return failure_schedule_kind::torture;
  if (s == "random")   return failure_schedule_kind::random;
  return std::nullopt;
}

static struct option options[] = {
    {"count",        required_argument, nullptr, 'n'},
    {"seed",         required_argument, nullptr, 'S'},
    {"random-seeds", required_argument, nullptr, 'R'},
    {"loss",         required_argument, nullptr, 'l'},
    {"failure",      required_argument, nullptr, 'f'},
    {"verbose",      no_argument,       nullptr, 'V'},
    {"print-db",     no_argument,       nullptr, 'p'},
    {"test",         no_argument,       nullptr, 't'},
    {nullptr, 0, nullptr, 0}};

int main(int argc, char* argv[]) {
  testinfo tester;
  std::optional<unsigned long> first_seed;
  unsigned long seed_count = 1;
  bool run_tests = false;

  auto shortopts = short_options_for(options);
  int ch;
  while ((ch = getopt_long(argc, argv, shortopts.c_str(), options, nullptr)) != -1) {
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
    } else if (ch == 't') {
      run_tests = true;
    } else {
      std::print(std::cerr, "Unknown option\n");
      return 1;
    }
  }

  if (run_tests) {
    std::print("Running doc_state tests...\n");
    collab::test_doc_state();
    std::print("Running scheduled collab Paxos+failure convergence (fixed seeds)...\n");
    if (!run_scheduled_collab_convergence_tests()) {
      return 1;
    }
    std::print(
        "All tests passed. (Also run ./build/test-doc-ops for full OT tests; "
        "bash collab-bench.sh for a larger random-seed campaign.)\n");
    return 0;
  }

  bool ok = true;
  if (first_seed) {
    ok = try_one_seed(tester, *first_seed);
  } else {
    std::mt19937_64 seed_generator = randomly_seeded<std::mt19937_64>();
    for (unsigned long i = 0; i != seed_count; ++i) {
      if (i > 0 && i % 1000 == 0) std::print(std::cerr, ".");
      unsigned long seed = seed_generator();
      ok = try_one_seed(tester, seed);
      if (!ok) break;
    }
    if (ok && seed_count >= 1000) std::print(std::cerr, "\n");
  }
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}

#endif // !PT_COLLAB_SERVER

// ---------------------------------------------------------------------------
// Server main: pt-collab-server. Starts a 3-replica Paxos cluster (no
// failures, no message loss) inside the same cot::loop as a real-time HTTP
// server (see http_server.{hh,cc}). HTTP handlers submit puts through one
// reserved simulated client; reads come straight from the leader's pancydb.
// ---------------------------------------------------------------------------

#ifdef PT_COLLAB_SERVER
#include "http_server.hh"
#include <filesystem>

static struct option server_options[] = {
    {"port",          required_argument, nullptr, 'p'},
    {"replicas",      required_argument, nullptr, 'n'},
    {"doc",           required_argument, nullptr, 'd'},
    {"link-delay-ms", required_argument, nullptr, 'L'},
    {"send-delay-ms", required_argument, nullptr, 'S'},
    {"recv-delay-ms", required_argument, nullptr, 'R'},
    {nullptr, 0, nullptr, 0}};

int main(int argc, char* argv[]) {
  uint16_t port = 8080;
  size_t nreplicas = 3;
  std::string doc_id = "main";

  // Server defaults match the bench's historical netsim values (5/1/1 ms).
  // Pass --link-delay-ms 0 --send-delay-ms 0 --recv-delay-ms 0 to remove the
  // simulated latency, which is what you want for a smooth in-process demo —
  // under wall-clock time the bench values can trigger Paxos election storms.
  unsigned link_delay_ms = 5;
  unsigned send_delay_ms = 1;
  unsigned recv_delay_ms = 1;

  auto shortopts = short_options_for(server_options);
  int ch;
  while ((ch = getopt_long(argc, argv, shortopts.c_str(), server_options, nullptr)) != -1) {
    if (ch == 'p') {
      port = static_cast<uint16_t>(from_str_chars<unsigned>(optarg));
    } else if (ch == 'n') {
      nreplicas = from_str_chars<size_t>(optarg);
    } else if (ch == 'd') {
      doc_id = optarg;
    } else if (ch == 'L') {
      link_delay_ms = from_str_chars<unsigned>(optarg);
    } else if (ch == 'S') {
      send_delay_ms = from_str_chars<unsigned>(optarg);
    } else if (ch == 'R') {
      recv_delay_ms = from_str_chars<unsigned>(optarg);
    } else {
      std::print(std::cerr,
                 "Usage: pt-collab-server [--port N] [--replicas N] [--doc ID]\n"
                 "                        [--link-delay-ms N] [--send-delay-ms N] [--recv-delay-ms N]\n");
      return 1;
    }
  }

  cot::set_clock(cot::clock::real_time);

  std::filesystem::create_directories("logs");
  if (!std::freopen("logs/server.log", "a", stderr)) {
    std::print(std::cerr, "warning: could not redirect server logs to logs/server.log\n");
  }
  std::print(std::cerr, "\n[DoomDraft server] ===== server start doc={} replicas={} port={} =====\n",
             doc_id, nreplicas, port);

  testinfo tester;
  tester.loss = 0.0;
  tester.nreplicas = nreplicas;
  tester.failure_schedule = failure_schedule_kind::none;
  tester.randomness.seed(0xD00Du);
  tester.link_delay    = std::chrono::milliseconds(link_delay_ms);
  tester.send_delay    = std::chrono::milliseconds(send_delay_ms);
  tester.receive_delay = std::chrono::milliseconds(recv_delay_ms);
  std::print(std::cerr,
             "[DoomDraft server] netsim delays link={}ms send={}ms recv={}ms\n",
             link_delay_ms, send_delay_ms, recv_delay_ms);

  http_client_model client(tester.nreplicas, tester.randomness);
  pt_paxos_instance inst(tester, client);
  client.start();

  std::vector<cot::task<>> replica_tasks;
  replica_tasks.reserve(tester.nreplicas);
  for (size_t s = 0; s != tester.nreplicas; ++s) {
    replica_tasks.push_back(inst.replicas[s]->run());
  }

  http_paxos_bridge bridge;
  bridge.doc_id = doc_id;
  bridge.client = &client;
  bridge.current_db = [&inst, &client]() -> const pancy::pancydb& {
    size_t leader = client.leader_index();
    if (leader >= inst.replicas.size()) leader = 0;
    return inst.replicas[leader]->db_;
  };
  bridge.fail_replica = [&inst](size_t rid) -> bool {
    if (rid >= inst.replicas.size()) {
      return false;
    }
    inst.fail_replica(rid);
    return true;
  };

  cot::task<> http_task = run_http_server(port, std::move(bridge));

  std::print("DoomDraft server: doc=\"{}\" replicas={} listening on http://localhost:{}\n",
             doc_id, tester.nreplicas, port);
  std::fflush(stdout);

  // cot::loop() returns when the driver thinks there is no pending work. We
  // register an untriggered keepalive so that should not happen; detached
  // long-lived tasks are not tied to task<> destructors. If the driver still
  // returns (cotamer edge cases, clearing paths, etc.), re-arm keepalive and
  // call loop() again so the HTTP server process does not exit until SIGINT.
  http_task.detach();
  for (auto& t : replica_tasks) {
    t.detach();
  }

  for (;;) {
    cot::event server_lifetime;
    cot::keepalive(std::move(server_lifetime));
    cot::loop();
    std::print(std::cerr,
               "pt-collab-server: cot::loop() returned unexpectedly; "
               "re-arming keepalive and continuing.\n");
  }
}

#endif // PT_COLLAB_SERVER

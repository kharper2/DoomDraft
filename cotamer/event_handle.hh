#pragma once
#include <cstddef>

// event_handle.hh
//    Declares the cotamer::detail::event_handle class, a smart pointer
//    specialized for Cotamer events. A full definition of this class is
//    required in order to declare the public cotamer::event class.

namespace cotamer {
class event;
template <typename T> class task;
class driver;
class fd;
class mutex;
template <bool shared> class mutex_event;
template <typename T> task<T> forward(task<T>);
namespace detail {
struct event_body;
struct fd_body;
struct quorum_event_body;
template <typename T> struct task_promise;
template <typename T> struct task_awaiter;
struct task_event_awaiter;
template <bool shared> struct task_mutex_event_awaiter;
struct task_resolution_awaiter;
struct task_final_awaiter;
struct interest_event_awaiter;
struct describe_task_awaiter;

class event_handle {
public:
    event_handle() = default;
    explicit inline event_handle(event_body*) noexcept;
    inline event_handle(const event_handle&) noexcept;
    inline event_handle(event_handle&&) noexcept;
    inline event_handle& operator=(const event_handle&);
    inline event_handle& operator=(event_handle&&) noexcept;
    inline event_handle& operator=(std::nullptr_t);
    inline void swap(event_handle&) noexcept;
    inline ~event_handle();

    explicit operator bool() const noexcept { return eb_ != nullptr; }
    inline bool triggered() const noexcept;
    inline bool idle() const noexcept;
    inline bool empty() const noexcept;
    event_body* get() const noexcept { return eb_; }
    event_body& operator*() const { return *eb_; }
    event_body* operator->() const noexcept { return eb_; }

private:
    event_body* eb_ = nullptr;
};

inline void swap(event_handle& a, event_handle& b) noexcept { a.swap(b); }


struct fd_update {
    int fd;
    fdevent mask;
    unsigned epoch;
};

class fd_event_set {
public:
    fd_event_set() = default;
    fd_event_set(const fd_event_set&) = delete;
    fd_event_set(fd_event_set&&) = delete;
    fd_event_set& operator=(const fd_event_set&) = delete;
    fd_event_set& operator=(fd_event_set&&) = delete;
    void deref_all(driver*);
    ~fd_event_set();

    // Epochs tag fd registrations to detect stale kernel notifications.
    // Bumped on body change and on `fd_close`. `internal_epoch`
    // is reserved for driver-internal fds (wakefd).
    static constexpr unsigned empty_epoch = 0;
    static constexpr unsigned internal_epoch = 1;
    static constexpr unsigned user_epoch = 2;

    // Install a watch on `fd` for the fdevents in `mask`.
    // Returns the event_handle to await, queues the fd for kernel
    // reconciliation, and registers interest in `body`.
    event_handle watch(int fd, fdevent mask, fd_body* body, driver*);

    // Detach and return all watchrecs on `fd` whose mask intersects with
    // `mask`. Returns the detached list as a 1-based index, which must be
    // drained and freed via `pop_watch_list_event`. The returned list also
    // includes watches that should be freed because obsolete (empty or
    // different epoch).
    inline unsigned take_watch_list(int fd, fdevent mask, unsigned epoch);

    // Pop the event off the head of a list returned by `take_watch_list`.
    // Advances `wix`, recycles the slot, returns the watchrec's event.
    inline event_handle pop_watch_list_event(unsigned& wix, fdevent mask = fdevent::none);

    // Called from `driver::notify_close` once the fd is closed. Triggers
    // and reclaims every watchrec on `fd`, bumps the epoch, clears the
    // body, queues the fd for kernel reconciliation. Returns body and
    // pre-bump epoch; nullopt if no body or fd not marked closed.
    std::optional<std::pair<fd_body*, unsigned>> fd_close(int fd);

    // Are there any updates (fds whose interest mask may differ from the kernel
    // event notifier)?
    inline bool has_update() const noexcept;

    // Pop one fd from the update list and recompute its mask. Bumps epoch
    // when mask drops to 0; promotes to `user_epoch` on first non-zero use.
    inline std::optional<fd_update> pop_update() noexcept;

    // First fd > `fd` with any watchrec.
    inline std::optional<fd_update> next_known(int fd) const noexcept;

    // Introspection (primarily for tests)
    // Watchrecs not on the freelist.
    inline size_t active_watch_count() const noexcept;
    // fdevent mask of interest in `fd`.
    inline fdevent fd_mask(int fd) const noexcept;

private:
    static constexpr unsigned first_capacity = 128;
    static constexpr unsigned block_capacity = 1024;

    // link values for fdrec and watchrec linked lists
    // `update_link` links fdrecs that may be updated relative to the kernel
    // event notifier; `whead/wtail/wlink` links active watchrecs per fdrec.
    // Link values are index+1 (so fdrs_[0]/ws_[0] maps to link 1).
    // link_clean marks end of watch list, or fdrec that is not on update list.
    // link_sentinel marks end of update list.
    static constexpr unsigned link_sentinel = -1;
    static constexpr unsigned link_clean = 0;

    struct fdrec {
        fd_body* body = nullptr;           // weak ref to owning fd_body
        unsigned epoch = 0;                // epoch registered with kernel event notifier
        unsigned whead = link_clean;       // head of watch list
        unsigned wtail = link_clean;       // tail of watch list
        unsigned update_link = link_clean; // link in update list

        inline bool known() const noexcept {
            return whead != link_clean;
        }
    };

    struct watchrec {
        event_handle ev;
        fdevent mask;
        unsigned wlink;
    };

    fdrec* fdrs_ = nullptr;
    unsigned fdr_capacity_ = 0;
    unsigned update_link_ = -1;   // head of update list; see encoding above
    std::vector<watchrec> ws_;
    unsigned free_wlink_ = 0;

    inline fdevent watch_list_mask(unsigned wix) const;
    void hard_ensure(unsigned fd);
};

struct fd_batch;

}


// Type helpers.
template <typename T> struct task_value_type { using type = void; };
template <typename T> struct task_value_type<task<T>> { using type = T; };

template <typename T> struct task_alternative_type {
    using value_type = typename task_value_type<T>::type;
    using type = std::conditional_t<std::is_void_v<value_type>, std::monostate, value_type>;
};

template <typename T> struct task_attempt_type {
    using type = typename task_alternative_type<T>::type;
};
template <typename T> struct task_attempt_type<task<std::optional<T>>> {
    using type = T;
};

template <typename... Ts> struct common_task_value_type
    : std::common_type<typename task_value_type<Ts>::type...> { };

template <typename T> using task_value_type_t =
    typename task_value_type<std::remove_cvref_t<T>>::type;
template <typename T> using task_alternative_type_t =
    typename task_alternative_type<std::remove_cvref_t<T>>::type;
template <typename T> using task_attempt_type_t =
    typename task_attempt_type<std::remove_cvref_t<T>>::type;
template <typename... Ts> using common_task_value_type_t =
    typename common_task_value_type<std::remove_cvref_t<Ts>...>::type;

}

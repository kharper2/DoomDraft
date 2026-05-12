#include "cotamer/io.hh"
#include <ranges>
#include <thread>
#include <netdb.h>
#include <unistd.h>
#include <sys/uio.h>

namespace cotamer {

struct timespec duration_timespec(duration d) {
    if (d <= duration::zero()) {
        return {0, 0};
    }
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(d);
    auto nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(d - sec);
    return {sec.count(), nsec.count()};
}

int duration_milliseconds(duration d) {
    if (d <= duration::zero()) {
        return 0;
    }
    auto msec = std::chrono::duration_cast<std::chrono::milliseconds>(d);
    return msec.count();
}


// file descriptor functions

namespace detail {

void fd_event_set::deref_all(driver* drv) {
    // must be called before destroying `fd_event_set`
    if (!fdr_capacity_) {
        return;
    }
    for (fdrec* fdr = fdrs_; fdr != fdrs_ + fdr_capacity_; ++fdr) {
        if (fdr->body) {
            fdr->body->remove_listener(drv);
        }
    }
}

void fd_event_set::hard_ensure(unsigned ufd) {
    assert(ufd >= fdr_capacity_);
    // choose new capacity to fit; double up to `block_capacity`
    unsigned ncapacity;
    if (ufd >= block_capacity) {
        ncapacity = ((ufd / block_capacity) + 1) * block_capacity;
    } else {
        ncapacity = std::max(first_capacity, fdr_capacity_ * 2);
        while (ufd >= ncapacity) {
            ncapacity *= 2;
        }
    }
    // allocate, copy, initialize
    fdrec* nfdrs = std::allocator<fdrec>().allocate(ncapacity);
    std::uninitialized_move(fdrs_, fdrs_ + fdr_capacity_, nfdrs);
    std::uninitialized_default_construct(nfdrs + fdr_capacity_, nfdrs + ncapacity);
    if (fdr_capacity_) {
        std::destroy(fdrs_, fdrs_ + fdr_capacity_);
        std::allocator<fdrec>().deallocate(fdrs_, fdr_capacity_);
    }
    // assign
    fdrs_ = nfdrs;
    fdr_capacity_ = ncapacity;
}

void fd_body::close(bool because_deref) {
    lock();
    // If this is the first close() call (fd_>=0), mark closed (fd_=-1) and
    // notify drivers. (The OS fd must remain open until no driver has it
    // registered on a kernel notifier -- otherwise epoll's EPOLL_CTL_DEL will
    // fail. If there are no drivers, close base_fd now; otherwise, the last
    // remove_listener will close it. We need local copies of base_fd and
    // drivers in case of concurrent `delete this`.)
    int base_fd = -1;
    small_vector<driver*, 4> drivers;
    if (fd_.load(std::memory_order_relaxed) >= 0) {
        fd_.store(-1, std::memory_order_relaxed);
        for (auto dx : drivers_) {
            drivers.push_back(dx);
        }
        base_fd = base_fd_;
        if (base_fd >= 0 && drivers.empty()) {
            // will ::close now
            base_fd_ = -1;
        }
    }
    bool deletable = because_deref && drivers_.empty();
    if (because_deref) {
        ref_.fetch_sub(1, std::memory_order_release);
    }
    unlock();

    if (base_fd < 0) {
        // do nothing
    } else if (drivers.empty()) {
        ::close(base_fd);
    } else {
        driver* my_driver = driver::current.get();
        for (auto dx : drivers) {
            if (dx == my_driver) {
                dx->notify_close(base_fd);
            } else {
                dx->migrate_fd_close(base_fd);
            }
        }
    }
    if (deletable) {
        delete this;
    }
}


event_handle fd_event_set::watch(int fd, fdevent imask, fd_body* body,
                                 driver* drv) {
    if (fd < 0 || imask == fdevent::none) {
        return event_handle{new event_body};
    }
    unsigned ufd = fd;
    if (ufd >= fdr_capacity_) {
        hard_ensure(ufd);
    }

    // ensure `fdrec`
    fdrec& fdi = fdrs_[ufd];
    if (fdi.body != body) {
        if (fdi.body) {
            // This should not happen: the user has closed and reopened the
            // file descriptor, or, worse, constructed a new `fd_body` for
            // the same descriptor. Trigger all events.
            auto fx = fd_close(fd);
            fx->first->remove_listener(drv);
        }
        fdi.body = body;
        body->add_listener(drv);
    }
    if (fdi.update_link == link_clean) {
        fdi.update_link = update_link_;
        update_link_ = ufd + 1;
    }

    // append `watchrec`
    auto wix = free_wlink_;
    watchrec* wr;
    if (wix) {
        wr = &ws_[wix - 1];
        free_wlink_ = wr->wlink;
        wr->mask = imask;
        wr->wlink = link_clean;
    } else {
        ws_.push_back({event_handle{}, imask, link_clean});
        wix = ws_.size();
        wr = &ws_[wix - 1];
    }
    if (fdi.whead) {
        ws_[fdi.wtail - 1].wlink = wix;
    } else {
        fdi.whead = wix;
    }
    fdi.wtail = wix;

    // construct and return event
    wr->ev = event_handle{new event_body};
    return wr->ev;
}

std::optional<std::pair<fd_body*, unsigned>> fd_event_set::fd_close(int fd) {
    unsigned ufd = fd;
    if (ufd >= fdr_capacity_) {
        return std::nullopt;
    }
    fdrec& fdi = fdrs_[ufd];
    if (!fdi.body || fdi.body->fileno() >= 0) {
        return std::nullopt;
    }

    while (auto wix = fdi.whead) {
        auto& wr = ws_[wix - 1];
        fdi.whead = wr.wlink;
        std::exchange(wr.ev, nullptr)->trigger();
        wr.wlink = free_wlink_;
        free_wlink_ = wix;
    }
    fdi.wtail = link_clean;

    ++fdi.epoch;
    if (fdi.update_link == link_clean) {
        fdi.update_link = update_link_;
        update_link_ = ufd + 1;
    }
    return {{std::exchange(fdi.body, nullptr), fdi.epoch - 1}};
}

void fd_batch::print_changes() {
#if COTAMER_USE_KQUEUE
    for (int i = 0; i != changes; ++i) {
        const auto& e = ev[i];
        std::string t;
        switch (e.filter) {
        case EVFILT_READ:   t = std::format("READ {}", e.ident);    break;
        case EVFILT_EXCEPT: t = std::format("EXCEPT {}", e.ident);  break;
        case EVFILT_WRITE:  t = std::format("WRITE {}", e.ident);   break;
        case EVFILT_VNODE:  t = std::format("VNODE {}", e.ident);   break;
        case EVFILT_PROC:   t = std::format("PROC {}", e.ident);    break;
        case EVFILT_SIGNAL: t = std::format("SIGNAL {}", e.ident);  break;
        case EVFILT_TIMER:  t = std::format("TIMER {}", e.ident);   break;
        case EVFILT_USER:   t = "USER";                             break;
        default:            t = std::format("#{} {}", e.filter, e.ident); break;
        }
        if (e.flags & EV_ADD) {
            t += " ADD";
        }
        if (e.flags & EV_DELETE) {
            t += " DELETE";
        }
        std::print(std::cerr, "<{} U{}>\n", t, e.udata);
    }
#endif
}

inline int fd_batch::mask_out(fdevent mask) noexcept {
#if COTAMER_USE_KQUEUE
    // should never be called
    (void) mask;
    return 0;
#elif COTAMER_USE_EPOLL
    return (int(mask & fdevent::read) ? int(EPOLLIN | EPOLLRDHUP) : 0)
        | (int(mask & fdevent::write) ? int(EPOLLOUT) : 0)
        | (int(mask & fdevent::close) ? int(EPOLLRDHUP) : 0);
#else
    return (int(mask & fdevent::read) ? int(POLLIN) : 0)
        | (int(mask & fdevent::write) ? int(POLLOUT) : 0);
#endif
}

inline fdevent fd_batch::mask_in(const system_event_type& se) noexcept {
#if COTAMER_USE_KQUEUE
    return (se.filter == EVFILT_READ ? fdevent::read : fdevent::none)
        | (se.filter == EVFILT_WRITE ? fdevent::write : fdevent::none)
        | (se.flags & EV_EOF ? fdevent::all : fdevent::none);
#elif COTAMER_USE_EPOLL
    return (se.events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR) ? fdevent::read : fdevent::none)
        | (se.events & (EPOLLOUT | EPOLLHUP | EPOLLERR) ? fdevent::write : fdevent::none)
        | (se.events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR) ? fdevent::close : fdevent::none);
#else
    return (se.revents & (POLLIN | POLLERR | POLLHUP | POLLNVAL) ? fdevent::read : fdevent::none)
        | (se.revents & (POLLOUT | POLLERR | POLLHUP | POLLNVAL) ? fdevent::write : fdevent::none)
        | (se.revents & (POLLERR | POLLHUP | POLLNVAL) ? fdevent::close : fdevent::none);
#endif
}

inline void fd_batch::add(int pollfd, const fd_update& fdu, fdevent old_mask) {
#if COTAMER_USE_KQUEUE
    if (changes == capacity) {
        clear(pollfd);
    }
    void* udata = reinterpret_cast<void*>(uintptr_t(fdu.epoch));
    int oldr = int(old_mask & fdevent::read_close), newr = int(fdu.mask & fdevent::read_close);
    if (bool(oldr) != bool(newr)) {
        EV_SET(&ev[changes], fdu.fd, EVFILT_READ, newr ? EV_ADD : EV_DELETE, 0, 0, udata);
        ++changes;
    }
    int oldw = int(old_mask & fdevent::write_close), neww = int(fdu.mask & fdevent::write_close);
    if (bool(oldw) == bool(neww)) {
        return;
    }
    if (changes == capacity) {
        clear(pollfd);
    }
    EV_SET(&ev[changes], fdu.fd, EVFILT_WRITE, neww ? EV_ADD : EV_DELETE, 0, 0, udata);
    ++changes;
#elif COTAMER_USE_EPOLL
    epoll_event epev;
    epev.events = mask_out(fdu.mask);
    epev.data.u64 = fdu.fd | (uint64_t(fdu.epoch) << 32);
    int op = fdu.mask != fdevent::none
        ? (old_mask != fdevent::none ? EPOLL_CTL_MOD : EPOLL_CTL_ADD)
        : EPOLL_CTL_DEL;
    if (epoll_ctl(pollfd, op, fdu.fd, &epev) < 0) {
        throw errno_error();
    }
#else
    (void) pollfd, (void) fdu, (void) old_mask;
#endif
}

inline std::optional<fd_update> fd_batch::pop() noexcept {
    while (index < size) {
        auto& e = ev[index];
        ++index;
#if COTAMER_USE_KQUEUE
        int fd = e.ident;
        unsigned epoch = reinterpret_cast<uintptr_t>(e.udata);
#elif COTAMER_USE_EPOLL
        int fd = e.data.u64 & 0xFFFF'FFFF;
        unsigned epoch = e.data.u64 >> 32;
#else
        int fd = e.fd;
        unsigned epoch = fd_event_set::empty_epoch;
#endif
        auto mask = mask_in(e);
        if (mask != fdevent::none) {
            return {{fd, mask, epoch}};
        }
    }
    return std::nullopt;
}

}

void driver::hard_pollfd() {
#if COTAMER_USE_KQUEUE
    if ((pollfd_ = kqueue()) < 0) {
        throw errno_error();
    }
    fcntl(pollfd_, F_SETFD, FD_CLOEXEC);
    // prepare wake event for cross-thread wake
    struct kevent ev;
    EV_SET(&ev, -1, EVFILT_USER, EV_ADD | EV_CLEAR, 0, 0, nullptr);
    if (kevent(pollfd_, &ev, 1, nullptr, 0, nullptr) < 0) {
        throw errno_error();
    }
#elif COTAMER_USE_EPOLL
    if ((pollfd_ = epoll_create1(EPOLL_CLOEXEC)) < 0) {
        throw errno_error();
    }
    if ((epoll_wakefd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) < 0) {
        int saved = errno;
        ::close(pollfd_);
        pollfd_ = -1;
        throw std::system_error(saved, std::generic_category());
    }
    epoll_event epev;
    epev.events = EPOLLIN;
    epev.data.u64 = epoll_wakefd_ | (uint64_t(detail::fd_event_set::internal_epoch) << 32);
    if (epoll_ctl(pollfd_, EPOLL_CTL_ADD, epoll_wakefd_, &epev) < 0) {
        throw errno_error();
    }
#endif
}

void driver::apply_fd_update(detail::fd_batch& batch,
                             const detail::fd_update& fdu) {
    // look up events currently registered on our event notification fd
    // for file `fdu.fd`; we store them in `fdctl_` in bit-packed form
    unsigned fdci = unsigned(fdu.fd) / 16,
        fdcs = (unsigned(fdu.fd) % 16) * 4;
    if (fdci >= fdctl_.size()) {
        fdctl_.resize(fdci + 1, uint64_t(0));
    }

    // return if no change (e.g., firing a read event removed the readable
    // watch, but application code re-installed that watch)
    fdevent old_mask = fdevent((fdctl_[fdci] >> fdcs) & 15);
    if (old_mask == fdu.mask) {
        return;
    }

    // add to the batch of event notification fd updates
    batch.add(pollfd(), fdu, old_mask);

    // record the new notification state in `fdctl_`
    fdctl_[fdci] ^= uint64_t(int(old_mask) ^ int(fdu.mask)) << fdcs;
    if (old_mask == fdevent::none) {
        ++nfdctl_;
    } else if (fdu.mask == fdevent::none) {
        --nfdctl_;
    }
}

bool driver::watch_fds(detail::fd_batch& batch, duration timeout) {
    // Ensure pollfd if we are asked to block before any fd registrations
    (void) pollfd();

#if COTAMER_USE_KQUEUE
    // Cross-thread wake injects an EVFILT_USER event onto the kqueue fd
    wakefd_.store(pollfd_, std::memory_order_seq_cst);
#elif COTAMER_USE_EPOLL
    // Cross-thread wake writes to an eventfd()
    wakefd_.store(epoll_wakefd_, std::memory_order_seq_cst);
#endif
#if COTAMER_USE_KQUEUE || COTAMER_USE_EPOLL
    if (lock_.load(std::memory_order_seq_cst)) {
        timeout = duration::zero();
    }
#endif

#if COTAMER_USE_KQUEUE
    // block in kernel
    struct timespec ts = duration_timespec(timeout);
    batch.size = kevent(pollfd_, batch.ev, batch.changes, batch.ev, batch.capacity, &ts);
    batch.changes = 0;
#elif COTAMER_USE_EPOLL
    batch.size = epoll_wait(pollfd_, batch.ev, batch.capacity, duration_milliseconds(timeout));
#else
    // The poll() fallback has no cross-thread wake mechanism (no equivalent
    // of EVFILT_USER or eventfd). Cross-thread triggers will be delayed
    // until the poll timeout expires. This is an acceptable tradeoff for a
    // portability fallback; kqueue/epoll should be used on production systems.
    //
    // NB: rebuilds the entire pollfd array on every call (O(max_fd) scan).
    // kqueue/epoll maintain kernel-side state and avoid this cost.
    batch.ev.clear();
    int fd = -1;
    while (auto fdu = fds_.next_known(fd)) {
        if (fdu->mask != fdevent::none) {
            batch.ev.emplace_back(fdu->fd, batch.mask_out(fdu->mask), 0);
        }
        fd = fdu->fd;
    }
    poll(batch.ev.begin(), batch.ev.size(), duration_milliseconds(timeout));
    batch.size = batch.ev.size();
#endif
    batch.index = 0;

    // check for unexpected error
    if (batch.size < 0 && errno != EINTR) {
        throw errno_error();
    }

#if COTAMER_USE_KQUEUE || COTAMER_USE_EPOLL
    wakefd_.store(-1, std::memory_order_relaxed);
#endif

    // process returned events
    while (auto fdu = batch.pop()) {
#if COTAMER_USE_EPOLL
        if (fdu->fd == epoll_wakefd_) {
            uint64_t v;
            ssize_t nr = ::read(epoll_wakefd_, &v, sizeof(v));
            (void) nr;
            continue;
        }
#endif
        auto wix = fds_.take_watch_list(fdu->fd, fdu->mask, fdu->epoch);
        while (wix) {
            auto eh = fds_.pop_watch_list_event(wix, fdu->mask);
            while (auto coh = eh->driver_trigger(this)) {
                coh();
                step_time();
            }
        }
    }

    // return true if we saw any fd activity, possibly including internal
    // activity (like a notification from another thread)
    return batch.size > 0;
}


namespace {
// ensure that the return value from `getaddrinfo` is properly freed
struct getaddrinfo_value {
    struct addrinfo hints{};
    struct addrinfo* ai = nullptr;
    int status = 0;
    ~getaddrinfo_value() {
        if (status == 0 && ai) {
            freeaddrinfo(ai);
        }
    }
};

struct getaddrinfo_error_category_impl : public std::error_category {
public:
    const char* name() const noexcept override { return "getaddrinfo"; }
    std::string message(int value) const override {
        return gai_strerror(value);
    }
};
}

const std::error_category& getaddrinfo_error_category() noexcept {
    static getaddrinfo_error_category_impl c;
    return c;
}

static void getaddrinfo_thread(std::string address,
                               std::shared_ptr<getaddrinfo_value> res,
                               event notifier) {
    auto colon = address.rfind(':');
    if (colon == std::string::npos) {
        res->status = EAI_NONAME;
        notifier.trigger();
        return;
    }
    auto host = address.substr(0, colon);
    auto port = address.substr(colon + 1);
    res->status = getaddrinfo(host.empty() ? nullptr : host.c_str(),
                              port.c_str(), &res->hints, &res->ai);
    notifier.trigger();
}


// Create a TCP socket passively listening on `address`, which should follow
// the pattern `ADDR:PORT`. If `ADDR` is empty, accepts connections from any
// address; if nonempty, accepts connections only from the specified address.
// Throws on error.

task<cotamer::fd> tcp_listen(std::string address, int backlog) {
    // DNS lookup can block, so do it on a separate thread
    auto res = std::make_shared<getaddrinfo_value>();
    res->hints.ai_family = AF_UNSPEC;
    res->hints.ai_socktype = SOCK_STREAM;
    res->hints.ai_flags = AI_PASSIVE;

    // start thread for lookup, co_await result
    event notifier;
    driver_guard guard;
    std::thread(getaddrinfo_thread, std::move(address), res, notifier).detach();
    co_await notifier;
    if (res->status != 0) {
        throw std::system_error(std::error_code(res->status, getaddrinfo_error_category()));
    }

    // `getaddrinfo` can return multiple addresses (e.g., IPv4 and IPv6);
    // try each one in turn
    int last_errno = EADDRNOTAVAIL;
    for (auto ai = res->ai; ai; ai = ai->ai_next) {
        int fileno = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fileno < 0) {
            last_errno = errno;
            continue;
        }

        set_nonblocking(fileno);
#ifdef SO_NOSIGPIPE
        set_no_sigpipe(fileno);
#endif
        int flag = 1;
        setsockopt(fileno, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
        if (ai->ai_family == AF_INET6) {
            setsockopt(fileno, IPPROTO_IPV6, IPV6_V6ONLY, &flag, sizeof(flag));
        }

        if (bind(fileno, ai->ai_addr, ai->ai_addrlen) == 0
            && listen(fileno, backlog) == 0) {
            // success
            co_return cotamer::fd(fileno);
        }

        last_errno = errno;
        ::close(fileno);
        fileno = -1;
    }
    throw std::system_error(last_errno, std::generic_category());
}


// Create a TCP socket actively connected to `address`, which should follow
// the pattern `ADDR:PORT`. Throws on error.

task<cotamer::fd> tcp_connect(std::string address) {
    auto res = std::make_shared<getaddrinfo_value>();
    res->hints.ai_family = AF_UNSPEC;
    res->hints.ai_socktype = SOCK_STREAM;

    // start thread for lookup, co_await result
    event notifier;
    driver_guard guard;
    std::thread(getaddrinfo_thread, std::move(address), res, notifier).detach();
    co_await notifier;
    if (res->status) {
        throw std::system_error(std::error_code(res->status, getaddrinfo_error_category()));
    }

    // try each address in turn
    std::exception_ptr last_err;
    for (auto ai = res->ai; ai; ai = ai->ai_next) {
        int fileno = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fileno < 0) {
            last_err = std::make_exception_ptr(errno_error());
            continue;
        }

        set_nonblocking(fileno);
#ifdef SO_NOSIGPIPE
        set_no_sigpipe(fileno);
#endif
        int flag = 1;
        setsockopt(fileno, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        cotamer::fd f(fileno);
        try {
            co_await connect(f, ai->ai_addr, ai->ai_addrlen);
            co_return std::move(f);
        } catch (...) {
            last_err = std::current_exception();
        }
    }
    std::rethrow_exception(last_err);
}


// Create a UDP socket passively bound to `address`, which should follow the
// pattern `ADDR:PORT`. The returned descriptor is ready to receive datagrams
// from any peer (use `recvfrom`/`recvmsg` to capture the sender’s address)
// and to send replies (use `sendto`/`sendmsg`). Throws on error.

task<cotamer::fd> udp_listen(std::string address) {
    auto res = std::make_shared<getaddrinfo_value>();
    res->hints.ai_family = AF_UNSPEC;
    res->hints.ai_socktype = SOCK_DGRAM;
    res->hints.ai_flags = AI_PASSIVE;

    event notifier;
    driver_guard guard;
    std::thread(getaddrinfo_thread, std::move(address), res, notifier).detach();
    co_await notifier;
    if (res->status != 0) {
        throw std::system_error(std::error_code(res->status, getaddrinfo_error_category()));
    }

    int last_errno = EADDRNOTAVAIL;
    for (auto ai = res->ai; ai; ai = ai->ai_next) {
        int fileno = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fileno < 0) {
            last_errno = errno;
            continue;
        }

        set_nonblocking(fileno);
#ifdef SO_NOSIGPIPE
        set_no_sigpipe(fileno);
#endif
        int flag = 1;
        setsockopt(fileno, SOL_SOCKET, SO_REUSEADDR, &flag, sizeof(flag));
        if (ai->ai_family == AF_INET6) {
            setsockopt(fileno, IPPROTO_IPV6, IPV6_V6ONLY, &flag, sizeof(flag));
        }

        if (bind(fileno, ai->ai_addr, ai->ai_addrlen) == 0) {
            co_return cotamer::fd(fileno);
        }

        last_errno = errno;
        ::close(fileno);
    }
    throw std::system_error(last_errno, std::generic_category());
}


// Create a UDP socket actively connected to `address`. After connection, the
// socket only accepts datagrams from that peer, and `send`/`recv` can be used
// directly without needing to specify the destination on each call. Throws on
// error.

task<cotamer::fd> udp_connect(std::string address) {
    auto res = std::make_shared<getaddrinfo_value>();
    res->hints.ai_family = AF_UNSPEC;
    res->hints.ai_socktype = SOCK_DGRAM;

    event notifier;
    driver_guard guard;
    std::thread(getaddrinfo_thread, std::move(address), res, notifier).detach();
    co_await notifier;
    if (res->status) {
        throw std::system_error(std::error_code(res->status, getaddrinfo_error_category()));
    }

    std::exception_ptr last_err;
    for (auto ai = res->ai; ai; ai = ai->ai_next) {
        int fileno = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fileno < 0) {
            last_err = std::make_exception_ptr(errno_error());
            continue;
        }

        set_nonblocking(fileno);
#ifdef SO_NOSIGPIPE
        set_no_sigpipe(fileno);
#endif

        cotamer::fd f(fileno);
        try {
            co_await connect(f, ai->ai_addr, ai->ai_addrlen);
            co_return std::move(f);
        } catch (...) {
            last_err = std::current_exception();
        }
    }
    std::rethrow_exception(last_err);
}


namespace {
template <typename Iovcnt>
inline void update_iovec(iovec*& iov, Iovcnt& iovcnt,
                         std::vector<iovec>& iovcopy, size_t r) {
    while (iovcnt > 0 && r >= iov->iov_len) {
        r -= iov->iov_len;
        ++iov;
        --iovcnt;
    }
    if (r != 0) {
        if (iovcopy.empty()) {
            iovcopy.append_range(std::span(iov, iovcnt));
            iov = iovcopy.data();
        }
        iov->iov_base = reinterpret_cast<char*>(iov->iov_base) + r;
        iov->iov_len -= r;
    }
}

inline bool msghdr_done(const msghdr* mh, size_t r) {
    const iovec* iov = mh->msg_iov;
    auto iovcnt = mh->msg_iovlen;
    while (iovcnt > 0 && r >= iov->iov_len) {
        r -= iov->iov_len;
        ++iov;
        --iovcnt;
    }
    return iovcnt <= 0;
}
}


// Write all bytes in the `iovec`s, suspending as needed. Returns bytes
// written.
task<ioresult> writev(fd f, const iovec* iov, size_t iovcnt) {
    std::vector<iovec> iovcopy;
    size_t nw = 0;
    do {
        ssize_t r = ::writev(f.fileno(), iov, iovcnt);
        if (r > 0) {
            nw += r;
            update_iovec(const_cast<iovec*&>(iov), iovcnt, iovcopy, r);
        } else if (r == 0) {
            // This result is only expected if the original `count` was 0.
            // At other times, treat it like EOF on read.
            break;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            co_await writable(f);
        } else if (nw > 0) {
            break;
        } else {
            co_return std::unexpected(errno_code());
        }
    } while (iovcnt != 0);
    co_return nw;
}


// Send the message in `mh`. Suspends on EAGAIN. If `flags & MSG_WAITALL`,
// suspends as needed until all bytes in `mh->msg_iov` are written (or error);
// this should be used only on connected sockets (e.g., TCP). Returns bytes
// written or error code.

task<ioresult> sendmsg(fd f, const msghdr* mh, int flags) {
    size_t nw = 0;
    msghdr mhx;
    std::vector<iovec> iovcopy;
    int xflags = (flags & ~MSG_WAITALL) | MSG_DONTWAIT | MSG_NOSIGNAL;
    do {
        ssize_t r = ::sendmsg(f.fileno(), mh, xflags);
        if (r >= 0) {
            nw += r;
            if (r == 0 || !(flags & MSG_WAITALL) || msghdr_done(mh, r)) {
                break;
            }
            if (mh != &mhx) {
                mhx = msghdr{};
                mhx.msg_iov = mh->msg_iov;
                mhx.msg_iovlen = mh->msg_iovlen;
                mh = &mhx;
            }
            update_iovec(mhx.msg_iov, mhx.msg_iovlen, iovcopy, r);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            co_await writable(f);
        } else if (nw > 0) {
            break;
        } else {
            co_return std::unexpected(errno_code());
        }
    } while (mh->msg_iovlen > 0);
    co_return nw;
}


// Receive the message to `mh`. Suspends on EAGAIN. If `flags & MSG_WAITALL`,
// additionally suspends as needed until all bytes in `mh->msg_iov` are read (or
// error); this should be used only on connected sockets (e.g., TCP). Returns
// bytes read or error code.

task<ioresult> recvmsg(fd f, msghdr* mh, int flags) {
    size_t nr = 0;
    msghdr mhx;
    std::vector<iovec> iovcopy;
    int xflags = (flags & ~MSG_WAITALL) | MSG_DONTWAIT;
    do {
        ssize_t r = ::recvmsg(f.fileno(), mh, xflags);
        if (r >= 0) {
            nr += r;
            if (r == 0 || !(flags & MSG_WAITALL) || msghdr_done(mh, r)) {
                break;
            }
            if (mh != &mhx) {
                mhx = msghdr{};
                mhx.msg_iov = mh->msg_iov;
                mhx.msg_iovlen = mh->msg_iovlen;
                mh = &mhx;
            }
            update_iovec(mhx.msg_iov, mhx.msg_iovlen, iovcopy, r);
        } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            co_await readable(f);
        } else if (nr > 0) {
            break;
        } else {
            co_return std::unexpected(errno_code());
        }
    } while (mh->msg_iovlen > 0);
    co_return nr;
}

}

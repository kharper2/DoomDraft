#pragma once
#include "cotamer/cotamer.hh"
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

#if !COTAMER_USE_KQUEUE && !COTAMER_USE_EPOLL && !COTAMER_USE_POLL
# if defined(__APPLE__) || defined(__FreeBSD__)
#  define COTAMER_USE_KQUEUE 1
# elif defined(__linux__)
#  define COTAMER_USE_EPOLL 1
# else
#  define COTAMER_USE_POLL 1
# endif
#endif
#if COTAMER_USE_KQUEUE
# include <sys/event.h>
#elif COTAMER_USE_EPOLL
# include <sys/epoll.h>
# include <sys/eventfd.h>
#else
# include <poll.h>
#endif

// cotamer/io.hh
//    Async I/O primitives built on cotamer::readable() and cotamer::writable().

namespace cotamer {

// Return the current error as a C++ standard object.

inline std::error_code errno_code() {
    return std::error_code(errno, std::generic_category());
}

inline std::system_error errno_error() {
    return std::system_error(errno, std::generic_category());
}


// Set a raw file descriptor to non-blocking mode.

inline void set_nonblocking(int fileno) {
    int flags = fcntl(fileno, F_GETFL, 0);
    if (flags < 0 || fcntl(fileno, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw errno_error();
    }
}

inline void set_nonblocking(const fd& f) {
    if (f.fileno() >= 0) {
        set_nonblocking(f.fileno());
    }
}


// Ignore SIGPIPE. Prefer `set_no_sigpipe` or using `send`/`recv` to ignore
// SIGPIPE on a specific socket; as a last resort, fall back to
// `ignore_sigpipe()` process-wide.

#ifdef SO_NOSIGPIPE
inline void set_no_sigpipe(int fileno) {
    int flag = 1;
    if (setsockopt(fileno, SOL_SOCKET, SO_NOSIGPIPE, &flag, sizeof(flag)) < 0) {
        throw errno_error();
    }
}
#endif

#ifndef MSG_NOSIGNAL
# define MSG_NOSIGNAL 0
#endif

inline void ignore_sigpipe() {
    signal(SIGPIPE, SIG_IGN);
}


// Read up to count bytes. Suspends on EAGAIN. Returns bytes read or error
// code.

inline task<ioresult> read_once(fd f, void* buf, size_t count) {
    while (true) {
        ssize_t r = ::read(f.fileno(), buf, count);
        if (r >= 0) {
            co_return r;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            co_await readable(f);
        } else {
            co_return std::unexpected(errno_code());
        }
    }
}


// Write up to count bytes. Suspends on EAGAIN. Returns bytes written or error
// code.

inline task<ioresult> write_once(fd f, const void* buf, size_t count) {
    while (true) {
        ssize_t r = ::write(f.fileno(), buf, count);
        if (r >= 0) {
            co_return r;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            co_await writable(f);
        } else {
            co_return std::unexpected(errno_code());
        }
    }
}


// Read n bytes, suspending as needed. Returns bytes read or error code.

inline task<ioresult> read(fd f, void* buf, size_t count) {
    char* p = static_cast<char*>(buf);
    size_t nr = 0;
    do {
        ssize_t r = ::read(f.fileno(), p + nr, count - nr);
        if (r > 0) {
            nr += r;
        } else if (r == 0) {
            break;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            co_await readable(f);
        } else if (nr > 0) {
            break;
        } else {
            co_return std::unexpected(errno_code());
        }
    } while (nr != count);
    co_return nr;
}


// Write n bytes, suspending as needed. Returns bytes written or error code.

inline task<ioresult> write(fd f, const void* buf, size_t count) {
    const char* p = static_cast<const char*>(buf);
    size_t nw = 0;
    do {
        ssize_t r = ::write(f.fileno(), p + nw, count - nw);
        if (r > 0) {
            nw += r;
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
    } while (nw != count);
    co_return nw;
}


// Receive a message of up to `count` bytes. Suspends on EAGAIN. Returns bytes
// read or error code.

inline task<ioresult> recv(fd f, void* buf, size_t count, int flags) {
    struct msghdr mh{};
    struct iovec iov[1];
    mh.msg_iov = iov;
    mh.msg_iovlen = 1;
    iov[0].iov_base = buf;
    iov[0].iov_len = count;
    co_return co_await recvmsg(std::move(f), &mh, flags);
}


// Send a message of up to `count` bytes. Suspends on EAGAIN. Returns bytes
// written or error code.

inline task<ioresult> send(fd f, const void* buf, size_t count, int flags) {
    struct msghdr mh{};
    struct iovec iov[1];
    mh.msg_iov = iov;
    mh.msg_iovlen = 1;
    iov[0].iov_base = const_cast<void*>(buf);
    iov[0].iov_len = count;
    co_return co_await sendmsg(std::move(f), &mh, flags);
}


// Send a message defined by `iov`. Suspends on EAGAIN. Returns bytes written
// or error code.

inline task<ioresult> sendv(fd f, const iovec* iov, size_t iovcnt, int flags) {
    struct msghdr mh{};
    mh.msg_iov = const_cast<iovec*>(iov);
    mh.msg_iovlen = iovcnt;
    co_return co_await sendmsg(std::move(f), &mh, flags);
}


// Send a message of `count` bytes, suspending as needed. Returns bytes written
// or error code.

inline task<ioresult> send_all(fd f, const void* buf, size_t count, int flags) {
    return send(std::move(f), buf, count, flags | MSG_WAITALL);
}


// Send a message defined by `iov`, suspending as needed. Returns bytes written
// or error code.

inline task<ioresult> sendv_all(fd f, const iovec* iov, size_t iovcnt, int flags) {
    return sendv(std::move(f), iov, iovcnt, flags | MSG_WAITALL);
}


// Receive a message of up to `count` bytes, capturing the sender's address
// into `*addr`. On entry, `*addrlen` is the capacity of `addr`; on return, it
// holds the actual address length. Suspends on EAGAIN. Returns bytes read or
// error code.

inline task<ioresult> recvfrom(fd f, void* buf, size_t count,
                               sockaddr* addr, socklen_t* addrlen, int flags) {
    struct msghdr mh{};
    struct iovec iov[1];
    iov[0].iov_base = buf;
    iov[0].iov_len = count;
    mh.msg_iov = iov;
    mh.msg_iovlen = 1;
    mh.msg_name = addr;
    mh.msg_namelen = addrlen ? *addrlen : 0;
    auto r = co_await recvmsg(std::move(f), &mh, flags);
    if (addrlen) {
        *addrlen = mh.msg_namelen;
    }
    co_return r;
}


// Send a single datagram of `count` bytes to `addr`. Suspends on EAGAIN.
// Returns bytes written or error code.

inline task<ioresult> sendto(fd f, const void* buf, size_t count,
                             const sockaddr* addr, socklen_t addrlen, int flags) {
    struct msghdr mh{};
    struct iovec iov[1];
    iov[0].iov_base = const_cast<void*>(buf);
    iov[0].iov_len = count;
    mh.msg_iov = iov;
    mh.msg_iovlen = 1;
    mh.msg_name = const_cast<sockaddr*>(addr);
    mh.msg_namelen = addrlen;
    co_return co_await sendmsg(std::move(f), &mh, flags);
}


// Deprecated (backward compatibility)

inline task<ioresult> recv_once(fd f, void* buf, size_t count) {
    return recv(std::move(f), buf, count);
}

inline task<ioresult> send_once(fd f, const void* buf, size_t count) {
    return send(std::move(f), buf, count);
}


// Connects to an address. Suspends until connected. Throws on error.

inline task<> connect(fd f, const struct sockaddr* addr, socklen_t len) {
    int r = ::connect(f.fileno(), addr, len), err = 0;
    if (r == 0) {
        co_return;
    } else if (errno != EINPROGRESS) {
        err = errno;
    } else {
        co_await writable(f);
        // Check for connection error
        socklen_t errlen = sizeof(err);
        if (getsockopt(f.fileno(), SOL_SOCKET, SO_ERROR, &err, &errlen) < 0) {
            err = errno;
        }
    }
    if (err != 0) {
        throw std::system_error(err, std::generic_category());
    }
}


// Accepts a connection. Returns new fd (with ownership). Throws on error.

inline task<fd> accept(fd listen_fd) {
    while (true) {
        int fileno = ::accept(listen_fd.fileno(), nullptr, nullptr);
        if (fileno >= 0) {
            set_nonblocking(fileno);
#ifdef SO_NOSIGPIPE
            set_no_sigpipe(fileno);
#endif
            co_return fd(fileno);
        } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            throw errno_error();
        }
        co_await readable(listen_fd);
    }
}


// Accepts a connection from a TCP socket. In addition to `cotamer::accept`,
// this function sets the TCP_NODELAY option.

inline task<fd> tcp_accept(fd listen_fd) {
    auto f = co_await accept(listen_fd);
    // disable Nagle’s algorithm
    int flag = 1;
    setsockopt(f.fileno(), IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    co_return std::move(f);
}


namespace detail {

struct fd_batch {
#if COTAMER_USE_KQUEUE
    using system_event_type = struct kevent;
    static constexpr size_t capacity = 256;
    struct kevent ev[capacity];
    int changes = 0;
#elif COTAMER_USE_EPOLL
    using system_event_type = struct epoll_event;
    static constexpr size_t capacity = 256;
    struct epoll_event ev[capacity];
#else
    using system_event_type = struct pollfd;
    small_vector<struct pollfd, 16> ev;
#endif
    int size = 0;
    int index = 0;

    static inline int mask_out(fdevent mask) noexcept;
    static inline fdevent mask_in(const system_event_type&) noexcept;

    inline void add(int pollfd, const fd_update& fdu, fdevent old_mask);
    inline void clear(int pollfd);
    inline std::optional<fd_update> pop() noexcept;
    void print_changes();
};

inline void fd_batch::clear(int pollfd) {
#if COTAMER_USE_KQUEUE
    if (changes) {
        kevent(pollfd, ev, changes, nullptr, 0, nullptr);
        changes = 0;
    }
#else
    (void) pollfd;
#endif
}

inline fd_event_set::~fd_event_set() {
    if (fdr_capacity_ != 0) {
        std::destroy(fdrs_, fdrs_ + fdr_capacity_);
        std::allocator<fdrec>().deallocate(fdrs_, fdr_capacity_);
    }
}

} // namespace detail


inline int driver::pollfd() {
    if (pollfd_ < 0) {
        hard_pollfd();
    }
    return pollfd_;
}

inline void driver::notify_close(int base_fd) {
    if (auto pair = fds_.fd_close(base_fd)) {
        detail::fd_batch batch;
        apply_fd_update(batch, {base_fd, fdevent::none, pair->second});
        batch.clear(pollfd());
        pair->first->remove_listener(this);
    }
}

} // namespace cotamer

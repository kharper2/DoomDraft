#pragma once
#include "cotamer/config.hh"
#include "cotamer/io.hh"
#include "llhttp.h"
#include <vector>
#include <string>
#include <memory>
#include <cstring>
#include <ctime>
namespace cotamer {
class http_parser;

struct http_kvoffsets {
    unsigned name1;
    unsigned name2;
    unsigned value1;
    unsigned value2;

    constexpr std::string_view name(const char* base) const noexcept {
        return {base + name1, name2 - name1};
    }
    constexpr std::string_view value(const char* base) const noexcept {
        return {base + value1, value2 - value1};
    }
};

struct http_header_iterator_proxy {
    using value_type = std::pair<std::string_view, std::string_view>;
    value_type pair;
    const value_type* operator->() const noexcept { return &pair; }
};

class http_header_iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<std::string_view, std::string_view>;
    using difference_type = std::ptrdiff_t;
    using reference = value_type;
    using pointer = http_header_iterator_proxy;

    http_header_iterator() = default;
    http_header_iterator(const char* base, const http_kvoffsets* pair)
        : base_(base), pair_(pair) {
    }
    http_header_iterator(const http_header_iterator&) = default;
    http_header_iterator(http_header_iterator&&) = default;
    http_header_iterator& operator=(const http_header_iterator&) = default;
    http_header_iterator& operator=(http_header_iterator&&) = default;

    constexpr std::string_view name() const noexcept {
        return pair_->name(base_);
    }
    constexpr bool name_eq(std::string_view str) const noexcept {
        return pair_->name(base_) == str;
    }
    constexpr bool name_eq_case(std::string_view str) const noexcept {
        return strings::ieq(pair_->name(base_), str);
    }

    constexpr std::string_view value() const noexcept {
        return pair_->value(base_);
    }

    value_type operator*() const noexcept { return {name(), value()}; }
    pointer operator->() const noexcept { return {operator*()}; }

    auto operator<=>(const http_header_iterator& x) const noexcept {
        return pair_ <=> x.pair_;
    }
    bool operator==(const http_header_iterator& x) const noexcept {
        return pair_ == x.pair_;
    }

    http_header_iterator& operator++() { ++pair_; return *this; }
    http_header_iterator operator++(int) { auto tmp{*this}; ++pair_; return tmp; }

    http_header_iterator& operator--() { --pair_; return *this; }
    http_header_iterator operator--(int) { auto tmp{*this}; --pair_; return tmp; }

    difference_type operator-(const http_header_iterator& x) const noexcept {
        return pair_ - x.pair_;
    }

private:
    const char* base_;
    const http_kvoffsets* pair_;
};

class http_message {
public:
    typedef http_header_iterator header_iterator;

    inline http_message();
    inline http_message(llhttp_method method, std::string url = std::string());
    inline http_message(std::string_view method, std::string url = std::string());
    inline http_message(unsigned status_code);
    http_message(http_message&&) = default;
    http_message& operator=(http_message&&) = default;
    inline http_message(const http_message&);
    inline http_message& operator=(const http_message&);

    inline constexpr bool ok() const;
    explicit inline constexpr operator bool() const;
    inline constexpr bool operator!() const;
    inline constexpr llhttp_errno error() const;
    inline const char* error_name() const;

    inline constexpr unsigned http_major() const;
    inline constexpr unsigned http_minor() const;
    inline constexpr unsigned status_code() const;
    inline const std::string& status_message() const;
    inline constexpr llhttp_method method() const;
    inline const char* method_name() const;

    // Examine headers
    inline bool has_header(std::string_view name) const;
    header_iterator find_header(std::string_view name) const;
    std::string header(std::string_view name) const;
    inline header_iterator header_begin() const;
    inline header_iterator header_end() const;

    // Examine URL and URL parts
    inline constexpr const std::string& url() const;   // `/path?a=b&c=d#hash`
    inline bool has_valid_url() const;
    inline bool has_path() const;
    inline bool has_search() const;
    inline bool has_hash() const;
    inline std::string_view path() const;      // → `/path`
    inline std::string_view search() const;    // → `?a=b&c=d`
    inline std::string_view hash() const;      // → `#hash`
    bool has_search_param(std::string_view name) const;
    std::string_view search_param(std::string_view name) const;
    inline header_iterator search_param_begin() const;
    inline header_iterator search_param_end() const;

    // Examine body
    inline const std::string& body() const;

    // Modify message
    inline http_message& clear();
    void add_header(std::string key, std::string value);
    inline void add_header(std::string key, size_t value);

    inline http_message& http_major(unsigned v);
    inline http_message& http_minor(unsigned v);
    inline http_message& error(llhttp_errno e);
    inline http_message& status_code(unsigned code);
    inline http_message& status_code(unsigned code, std::string message);
    inline http_message& method(llhttp_method method);
    inline http_message& method(std::string_view method);
    inline http_message& method_name(std::string_view method);
    inline http_message& url(std::string url);
    inline http_message& header(std::string key, std::string value);
    inline http_message& header(std::string key, size_t value);
    inline http_message& date_header(std::string key, time_t value);

    inline http_message& body(std::string body);
    inline http_message& append_body(const std::string& x);
    template <detail::nlohmann_basic_json_type Json>
    http_message& body(const Json& j);

    static const char* default_status_message(unsigned code);

    // Deprecated accessors
    [[deprecated]] inline bool has_header(const char* s, size_t count) const;
    [[deprecated]] inline header_iterator find_header(const char* s, size_t count) const;
    [[deprecated]] inline std::string header(const char* s, size_t count) const;

private:
    enum {
        info_url = 1, info_params = 2
    };

    struct info_type {
        unsigned flags = 0;
        std::pair<unsigned, unsigned> f[3];
        std::string qurl;
        std::vector<http_kvoffsets> qpairs;
        inline info_type()
            : flags(0) {
        }
    };

    unsigned char major_;
    unsigned char minor_;
    unsigned char method_;
    unsigned char error_;
    unsigned short status_code_;
    unsigned upgrade_ : 1;
    unsigned has_body_ : 1;

    std::string url_;
    std::string status_message_;
    std::string headers_;
    std::vector<http_kvoffsets> hpairs_;
    std::string body_;
    mutable std::unique_ptr<info_type> info_;

    inline void kill_info(unsigned f) const;
    inline info_type& info(unsigned f) const;
    void make_info(unsigned f) const;
    inline bool has_url_field(int field) const;
    inline std::string_view url_field(int field) const;
    void do_clear();
    friend class http_parser;
};

class http_parser {
public:
    enum parser_type { client, server };
    http_parser(fd f, parser_type direction = server,
                std::string host = std::string());
    http_parser(fd f, enum llhttp_type receive_type);

    inline bool ok() const;
    inline constexpr llhttp_errno error() const;
    inline const char* error_name() const;

    void clear();

    inline const std::string& host() const;
    inline http_parser& set_host(const std::string& host);
    inline http_parser& clear_host();

    inline task<http_message> receive();
    using ticket_type = mutex_event<false>;
    task<http_message> receive(ticket_type);

    task<> send(http_message);
    task<ticket_type> send_request(http_message);
    task<> send_response(http_message);
    task<> send_response_chunk(std::string);
    task<> send_response_end_chunk();

    inline bool should_keep_alive() const;
    inline void clear_should_keep_alive();

    // Data buffered after parsing the HTTP message
    inline const std::string& receive_buffer() const;
    inline std::string& receive_buffer();

    // Underlying fd
    inline const fd& file() const;
    // Release ownership of the underlying fd (e.g., for protocol upgrades)
    inline fd take_file();

    static llhttp_method parse_method(std::string_view);

private:
    static constexpr size_t bufsize = 8192;

    ::llhttp_t hp_;
    fd f_;
    mutex m_[2];
    std::string receive_buffer_;
    std::string host_;

    enum { state_unknown, state_header_name, state_header_value, state_done };

    struct message_data {
        http_message hm;
        int state = state_unknown;
        unsigned name1;
        unsigned name2;
        unsigned value1;
    };

    static const llhttp_settings_t settings;
    static http_parser* get_parser(::llhttp_t* hp);
    static message_data* get_message_data(::llhttp_t* hp);
    static int on_message_begin(::llhttp_t* hp);
    static int on_url(::llhttp_t* hp, const char* s, size_t len);
    static int on_status(::llhttp_t* hp, const char* s, size_t len);
    static int on_header_field(::llhttp_t* hp, const char* s, size_t len);
    static int on_header_field_complete(::llhttp_t* hp);
    static int on_header_value(::llhttp_t* hp, const char* s, size_t len);
    static int on_header_value_complete(::llhttp_t* hp);
    static int on_headers_complete(::llhttp_t* hp);
    static int on_chunk_header(::llhttp_t* hp);
    static int on_body(::llhttp_t* hp, const char* s, size_t len);
    static int on_message_complete(::llhttp_t* hp);
    inline void copy_parser_status(message_data& md);
};

inline http_message::http_message()
    : major_(1), minor_(1), method_(HTTP_GET), error_(HPE_OK),
      status_code_(200), upgrade_(0), has_body_(0) {
}

inline http_message::http_message(llhttp_method method, std::string url)
    : major_(1), minor_(1), method_(method), error_(HPE_OK),
      status_code_(200), upgrade_(0), has_body_(0), url_(std::move(url)) {
}

inline http_message::http_message(std::string_view method, std::string url)
    : major_(1), minor_(1), method_(http_parser::parse_method(method)), error_(HPE_OK),
      status_code_(200), upgrade_(0), has_body_(0), url_(std::move(url)) {
}

inline http_message::http_message(unsigned status_code)
    : major_(1), minor_(1), method_(HTTP_GET), error_(HPE_OK),
      status_code_(status_code), upgrade_(0), has_body_(0) {
}

inline http_message::http_message(const http_message& x)
    : major_(x.major_), minor_(x.minor_), method_(x.method_), error_(x.error_),
      status_code_(x.status_code_), upgrade_(x.upgrade_), has_body_(x.has_body_),
      url_(x.url_), status_message_(x.status_message_), headers_(x.headers_),
      body_(x.body_) {
}

inline http_message& http_message::operator=(const http_message& x) {
    if (&x != this) {
        major_ = x.major_;
        minor_ = x.minor_;
        method_ = x.method_;
        error_ = x.error_;
        status_code_ = x.status_code_;
        upgrade_ = x.upgrade_;
        has_body_ = x.has_body_;
        url_ = x.url_;
        status_message_ = x.status_message_;
        headers_ = x.headers_;
        hpairs_ = x.hpairs_;
        body_ = x.body_;
        if (info_) {
            info_->flags = 0;
        }
    }
    return *this;
}

inline void http_message::kill_info(unsigned fl) const {
    if (info_) {
        info_->flags &= ~fl;
    }
}

inline constexpr unsigned http_message::http_major() const {
    return major_;
}

inline constexpr unsigned http_message::http_minor() const {
    return minor_;
}

inline constexpr bool http_message::ok() const {
    return error_ == HPE_OK;
}

inline constexpr http_message::operator bool() const {
    return ok();
}

inline constexpr bool http_message::operator!() const {
    return !ok();
}

inline constexpr llhttp_errno http_message::error() const {
    return (llhttp_errno) error_;
}

inline const char* http_message::error_name() const {
    return llhttp_errno_name((llhttp_errno) error_);
}

inline constexpr unsigned http_message::status_code() const {
    return status_code_;
}

inline const std::string& http_message::status_message() const {
    return status_message_;
}

inline constexpr llhttp_method http_message::method() const {
    return (llhttp_method) method_;
}

inline const char* http_message::method_name() const {
    return llhttp_method_name((llhttp_method) method_);
}

inline constexpr const std::string& http_message::url() const {
    return url_;
}

inline bool http_message::has_header(const char* name, size_t length) const {
    return find_header({name, length}) != header_end();
}

inline bool http_message::has_header(std::string_view name) const {
    return find_header(name) != header_end();
}

inline http_message::header_iterator http_message::find_header(const char* name, size_t length) const {
    return find_header({name, length});
}

inline std::string http_message::header(const char* name, size_t length) const {
    return header({name, length});
}

inline const std::string& http_message::body() const {
    return body_;
}

inline bool http_message::has_url_field(int field) const {
    auto& inf = info(info_url);
    return inf.f[field].first != inf.f[field].second;
}

inline std::string_view http_message::url_field(int field) const {
    auto& inf = info(info_url);
    if (inf.f[field].first != inf.f[field].second) {
        return std::string_view(url_.data() + inf.f[field].first,
                                url_.data() + inf.f[field].second);
    }
    return std::string_view();
}

inline bool http_message::has_valid_url() const {
    return has_url_field(0);
}

inline std::string_view http_message::path() const {
    return url_field(0);
}

inline bool http_message::has_search() const {
    return has_url_field(1);
}

inline std::string_view http_message::search() const {
    return url_field(1);
}

inline bool http_message::has_hash() const {
    return has_url_field(2);
}

inline std::string_view http_message::hash() const {
    return url_field(2);
}

inline http_message& http_message::http_major(unsigned v) {
    major_ = v;
    return *this;
}

inline http_message& http_message::http_minor(unsigned v) {
    minor_ = v;
    return *this;
}

inline http_message& http_message::clear() {
    do_clear();
    return *this;
}

inline http_message& http_message::error(llhttp_errno e) {
    error_ = e;
    return *this;
}

inline http_message& http_message::status_code(unsigned code) {
    status_code_ = code;
    status_message_ = std::string();
    return *this;
}

inline http_message& http_message::status_code(unsigned code, std::string message) {
    status_code_ = code;
    status_message_ = std::move(message);
    return *this;
}

inline http_message& http_message::method(llhttp_method method) {
    method_ = (unsigned) method;
    return *this;
}

inline http_message& http_message::method(std::string_view method) {
    method_ = (unsigned) http_parser::parse_method(method);
    return *this;
}

inline http_message& http_message::method_name(std::string_view method) {
    method_ = (unsigned) http_parser::parse_method(method);
    return *this;
}

inline http_message& http_message::url(std::string url) {
    url_ = std::move(url);
    kill_info(info_url | info_params);
    return *this;
}

inline void http_message::add_header(std::string key, size_t value) {
    add_header(std::move(key), std::to_string(value));
}

inline http_message& http_message::header(std::string key, std::string value) {
    add_header(std::move(key), std::move(value));
    return *this;
}

inline http_message& http_message::header(std::string key, size_t value) {
    add_header(std::move(key), value);
    return *this;
}

inline http_message& http_message::date_header(std::string key, time_t value) {
    char buf[128];
    // XXX current locale
    strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", gmtime(&value));
    add_header(std::move(key), std::string(buf));
    return *this;
}

inline http_message& http_message::body(std::string body) {
    body_ = std::move(body);
    has_body_ = 1;
    return *this;
}

inline http_message& http_message::append_body(const std::string& x) {
    body_ += x;
    has_body_ = 1;
    return *this;
}

template <detail::nlohmann_basic_json_type Json>
http_message& http_message::body(const Json& j) {
    if (!has_header("Content-Type")) {
        add_header("Content-Type", "application/json");
    }
    body_ = j.dump();
    has_body_ = 1;
    return *this;
}

inline http_message::info_type& http_message::info(unsigned f) const {
    if (!info_ || (info_->flags & f) != f) {
        make_info(f);
    }
    return *info_.get();
}

inline http_message::header_iterator http_message::header_begin() const {
    return http_header_iterator{headers_.data(), hpairs_.data()};
}

inline http_message::header_iterator http_message::header_end() const {
    return http_header_iterator{headers_.data(), hpairs_.data() + hpairs_.size()};
}

inline http_message::header_iterator http_message::search_param_begin() const {
    auto& inf = info(info_params);
    return http_header_iterator{inf.qurl.data(), inf.qpairs.data()};
}

inline http_message::header_iterator http_message::search_param_end() const {
    auto& inf = info(info_params);
    return http_header_iterator{inf.qurl.data(), inf.qpairs.data() + inf.qpairs.size()};
}

inline bool http_parser::ok() const {
    return hp_.error == (unsigned) HPE_OK;
}

inline constexpr llhttp_errno http_parser::error() const {
    return (llhttp_errno) hp_.error;
}

inline const char* http_parser::error_name() const {
    return llhttp_errno_name((llhttp_errno) hp_.error);
}

inline const std::string& http_parser::host() const {
    return host_;
}

inline http_parser& http_parser::set_host(const std::string& host) {
    host_ = host;
    return *this;
}

inline http_parser& http_parser::clear_host() {
    host_.clear();
    return *this;
}

inline bool http_parser::should_keep_alive() const {
    return llhttp_should_keep_alive(&hp_);
}

inline task<http_message> http_parser::receive() {
    return receive(m_[0].lock());
}

inline void http_parser::clear_should_keep_alive() {
    hp_.flags = (hp_.flags & ~F_CONNECTION_KEEP_ALIVE) | F_CONNECTION_CLOSE;
}

inline const fd& http_parser::file() const {
    return f_;
}

inline fd http_parser::take_file() {
    return std::move(f_);
}

inline const std::string& http_parser::receive_buffer() const {
    return receive_buffer_;
}

inline std::string& http_parser::receive_buffer() {
    return receive_buffer_;
}

} // namespace cotamer

#include "cotamer/http.hh"
#if COTAMER_HAVE_NLOHMANN_JSON
# include <nlohmann/json.hpp>
#endif

namespace {
struct status_code_map {
    unsigned code;
    const char* message;
} default_status_codes[] = {
    {100, "Continue"},
    {101, "Switching Protocols"},
    {102, "Processing"},                // RFC 2518, obsoleted by RFC 4918
    {103, "Early Hints"},               // RFC 8297
    {110, "Early Hints"},
    {111, "Early Hints"},
    {112, "Early Hints"},
    {113, "Early Hints"},
    {200, "OK"},
    {201, "Created"},
    {202, "Accepted"},
    {203, "Non-Authoritative Information"},
    {204, "No Content"},
    {205, "Reset Content"},
    {206, "Partial Content"},
    {207, "Multi-Status"},              // RFC 4918
    {208, "Already Reported"},          // RFC 5842
    {226, "IM Used"},                   // RFC 3229
    {300, "Multiple Choices"},
    {301, "Moved Permanently"},
    {302, "Found"},
    {303, "See Other"},
    {304, "Not Modified"},
    {305, "Use Proxy"},
    {307, "Temporary Redirect"},
    {308, "Permanent Redirect"},
    {400, "Bad Request"},
    {401, "Unauthorized"},
    {402, "Payment Required"},
    {403, "Forbidden"},
    {404, "Not Found"},
    {405, "Method Not Allowed"},
    {406, "Not Acceptable"},
    {407, "Proxy Authentication Required"},
    {408, "Request Timeout"},
    {409, "Conflict"},
    {410, "Gone"},
    {411, "Length Required"},
    {412, "Precondition Failed"},
    {413, "Content Too Large"},
    {414, "URI Too Long"},
    {415, "Unsupported Media Type"},
    {416, "Range Not Satisfiable"},
    {417, "Expectation Failed"},
    {418, "I\'m a teapot"},             // RFC 2324
    {421, "Misdirected Request"},
    {422, "Unprocessable Content"},     // RFC 4918
    {423, "Locked"},                    // RFC 4918
    {424, "Failed Dependency"},         // RFC 4918
    {425, "Too Early"},                 // RFC 8470
    {426, "Upgrade Required"},          // RFC 2817
    {428, "Precondition Required"},     // RFC 6585
    {429, "Too Many Requests"},         // RFC 6585
    {431, "Request Header Fields Too Large"}, // RFC 6585
    {451, "Unavailable For Legal Reasons"}, // RFC 7725
    {500, "Internal Server Error"},
    {501, "Not Implemented"},
    {502, "Bad Gateway"},
    {503, "Service Unavailable"},
    {504, "Gateway Timeout"},
    {505, "HTTP Version Not Supported"},
    {506, "Variant Also Negotiates"},   // RFC 2295
    {507, "Insufficient Storage"},      // RFC 4918
    {508, "Loop Detected"},
    {509, "Bandwidth Limit Exceeded"},
    {510, "Not Extended"},              // RFC 2774
    {511, "Network Authentication Required"} // RFC 6585
};
struct status_code_map_comparator {
    bool operator()(const status_code_map& a, unsigned b) {
        return a.code < b;
    }
};

constexpr uint8_t us_safe = 1;  // all URL-safe characters except ?
constexpr uint8_t us_hex = 2;
const uint8_t urlsafe[256] = {
    /* 0x00-0x0F */ 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
    /* 0x10-0x1F */ 0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
    /* 0x20-0x2F */ 0, 1, 0, 0, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
    /* 0x30-0x3F */ 0x03, 0x13, 0x23, 0x33, 0x43, 0x53, 0x63, 0x73,
                    0x83, 0x93, 1, 1, 0, 1, 0, 0,
    /* 0x40-0x4F */ 1, 0xA3, 0xB3, 0xC3, 0xD3, 0xE3, 0xF3, 1,
                    1, 1, 1, 1, 1, 1, 1, 1,
    /* 0x50-0x5F */ 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 1,
    /* 0x60-0x6F */ 1, 0xA3, 0xB3, 0xC3, 0xD3, 0xE3, 0xF3, 1,
                    1, 1, 1, 1, 1, 1, 1, 1,
    /* 0x70-0x7F */ 1, 1, 1, 1, 1, 1, 1, 1,  1, 1, 1, 1, 1, 1, 1, 0,
};
inline int xvalue(unsigned char ch) {
    return urlsafe[ch] >> 4;
}

int parse_method_on_method_complete(llhttp_t*) {
    return -1;
}

const llhttp_settings_t parse_method_settings = []{
    llhttp_settings_t s{};
    s.on_method_complete = parse_method_on_method_complete;
    return s;
}();

}

namespace cotamer {

const llhttp_settings_t http_parser::settings = []{
    llhttp_settings_t s{};
    s.on_message_begin = &http_parser::on_message_begin;
    s.on_url = &http_parser::on_url;
    s.on_status = &http_parser::on_status;
    s.on_header_field = &http_parser::on_header_field;
    s.on_header_value = &http_parser::on_header_value;
    s.on_headers_complete = &http_parser::on_headers_complete;
    s.on_body = &http_parser::on_body;
    s.on_message_complete = &http_parser::on_message_complete;
    s.on_header_field_complete = &http_parser::on_header_field_complete;
    s.on_header_value_complete = &http_parser::on_header_value_complete;
    s.on_chunk_header = &http_parser::on_chunk_header;
    return s;
}();

const char* http_message::default_status_message(unsigned code) {
    size_t ncodes = sizeof(default_status_codes) / sizeof(status_code_map);
    status_code_map* m = std::lower_bound(default_status_codes,
                                          default_status_codes + ncodes,
                                          code,
                                          status_code_map_comparator());
    if (m != default_status_codes + ncodes && m->code == code) {
        return m->message;
    }
    return llhttp_status_name((llhttp_status_t) code);
}

http_message::header_iterator http_message::find_header(std::string_view name) const {
    auto it = header_begin(), end = header_end();
    while (it != end && !it.name_eq_case(name)) {
        ++it;
    }
    return it;
}

std::string http_message::header(std::string_view name) const {
    std::string result;
    bool any = false;
    auto it = header_begin(), end = header_end();
    while (it != end) {
        if (it.name_eq_case(name)) {
            if (any) {
                result += ", ";
                result += it.value();
            } else {
                result = it.value();
                any = true;
            }
        }
        ++it;
    }
    return result;
}

void http_message::do_clear() {
    major_ = minor_ = 1;
    status_code_ = 200;
    method_ = HTTP_GET;
    error_ = HPE_OK;
    upgrade_ = has_body_ = 0;
    url_.clear();
    status_message_.clear();
    body_.clear();
    hpairs_.clear();
    if (info_) {
        info_->flags = 0;
    }
}

void http_message::add_header(std::string key, std::string value) {
    if (headers_.empty()) {
        headers_.reserve(2048);
    }
    size_t name1 = headers_.length(), name2 = name1 + key.length();
    headers_ += key;
    headers_.append(": ", 2);
    headers_ += value;
    headers_.append("\r\n", 2);
    hpairs_.emplace_back(name1, name2, name2 + 2, name2 + 2 + value.length());
}

void http_message::make_info(unsigned fl) const {
    if (!info_) {
        info_ = std::make_unique<info_type>();
    }
    info_type& inf = *info_;

    if (!(inf.flags & info_url) && (fl & (info_url | info_params))) {
        const unsigned char* s1 = reinterpret_cast<const unsigned char*>(url_.data());
        const unsigned char* s2 = s1 + url_.length();
        const unsigned char* s = s1;
        int fpos = 0;
        if (s == s2 || *s != '/') {
            goto fail;
        }
        while (s != s2) {
            if (urlsafe[*s] & us_safe) {
                ++s;
            } else if (*s == '?') {
                if (fpos == 0) {
                    inf.f[fpos].first = 0;
                    inf.f[fpos].second = s - s1;
                    fpos = 1;
                }
                ++s;
            } else if (*s == '#') {
                while (fpos < 2) {
                    inf.f[fpos].first = fpos ? inf.f[fpos - 1].second : 0;
                    inf.f[fpos].second = s - s1;
                    ++fpos;
                }
                ++s;
            } else {
            fail:
                fpos = 0;
                s = s2 = s1;
            }
        }
        while (fpos != 3) {
            inf.f[fpos].first = fpos ? inf.f[fpos - 1].second : 0;
            inf.f[fpos].second = s - s1;
            ++fpos;
        }
    }

    if (!(inf.flags & info_params) && (fl & info_params)) {
        inf.qurl.clear();
        inf.qpairs.clear();
        if (inf.f[1].first != inf.f[1].second) {
            const char* s1 = url_.data() + inf.f[1].first + 1;
            const char* s2 = url_.data() + inf.f[1].second;
            inf.qurl.reserve(s2 - s1);
            int state = 0; // 0: beginning, 1: in name, 2: in value
            const char* s = s1;
            const char* last = s1;
            unsigned nstart = 0, nend = 0, vstart = 0, vend = 0;
            while (s != s2) {
                if (*s == '&') {
                    if (state != 0) {
                        vend = inf.qurl.length() + s - last;
                        if (state == 1) {
                            nend = vstart = vend;
                        }
                        inf.qpairs.emplace_back(nstart, nend, vstart, vend);
                        state = 0;
                    }
                    ++s;
                    nstart = nend = inf.qurl.length() + s - last;
                    continue;
                }
                if (*s == '=' && state <= 1) {
                    nend = inf.qurl.length() + s - last;
                    vstart = nend + 1;
                    state = 2;
                    ++s;
                    continue;
                }
                if (state == 0) {
                    state = 1;
                }
                if (*s == '%'
                    && s2 - s >= 2
                    && (urlsafe[(unsigned char) s[1]] & us_hex)
                    && (urlsafe[(unsigned char) s[2]] & us_hex)) {
                    inf.qurl.append(last, s);
                    inf.qurl.push_back(xvalue(s[1]) * 16 + xvalue(s[2]));
                    last = s = s + 3;
                } else if (*s == '+') {
                    inf.qurl.append(last, s);
                    inf.qurl.push_back(' ');
                    last = s = s + 1;
                } else {
                    ++s;
                }
            }
            if (last != s) {
                inf.qurl.append(last, s);
            }
            if (state != 0) {
                vend = inf.qurl.length();
                if (state == 1) {
                    nend = vstart = vend;
                }
                inf.qpairs.emplace_back(nstart, nend, vstart, vend);
            }
        }
        inf.flags |= info_params;
    }
}

bool http_message::has_search_param(std::string_view name) const {
    const info_type& inf = info(info_params);
    auto it = inf.qpairs.begin(), end = inf.qpairs.end();
    while (it != end) {
        if (it->name(inf.qurl.data()) == name) {
            return true;
        }
        ++it;
    }
    return false;
}

std::string_view http_message::search_param(std::string_view name) const {
    const info_type& inf = info(info_params);
    auto it = inf.qpairs.begin(), end = inf.qpairs.end();
    while (it != end) {
        if (it->name(inf.qurl.data()) == name) {
            return it->value(inf.qurl.data());
        }
        ++it;
    }
    return std::string_view{};
}


llhttp_method http_parser::parse_method(std::string_view s) {
    llhttp_t hp;
    llhttp_init(&hp, HTTP_REQUEST, &parse_method_settings);
    llhttp_errno_t err = llhttp_execute(&hp, s.data(), s.size());
    const char space[] = " ";
    if (err == HPE_OK) {
        err = llhttp_execute(&hp, space, 1);
    }
    if (err == HPE_CB_METHOD_COMPLETE
        && (hp.error_pos == s.data() + s.size() || hp.error_pos == space)) {
        return (llhttp_method) hp.method;
    }
    throw std::invalid_argument("unknown HTTP method");
}


http_parser::http_parser(fd f, parser_type direction, std::string host)
    : f_(std::move(f)), host_(std::move(host)) {
    auto hp_type = direction == client ? HTTP_RESPONSE : HTTP_REQUEST;
    llhttp_init(&hp_, hp_type, &settings);
}

http_parser::http_parser(fd f, enum llhttp_type receive_type)
    : f_(std::move(f)) {
    llhttp_init(&hp_, receive_type, &settings);
}

void http_parser::clear() {
    llhttp_reset(&hp_);
    receive_buffer_.clear();
}

inline void http_parser::copy_parser_status(message_data& md) {
    md.hm.major_ = hp_.http_major;
    md.hm.minor_ = hp_.http_minor;
    md.hm.status_code_ = hp_.status_code;
    md.hm.method_ = hp_.method;
    md.hm.error_ = hp_.error;
    md.hm.upgrade_ = hp_.upgrade;
}

inline http_parser* http_parser::get_parser(::llhttp_t* hp) {
    return reinterpret_cast<http_parser*>(hp);
}

inline http_parser::message_data* http_parser::get_message_data(::llhttp_t* hp) {
    return static_cast<message_data*>(hp->data);
}

int http_parser::on_message_begin(::llhttp_t* hp) {
    message_data* md = get_message_data(hp);
    md->hm.clear();
    md->state = state_unknown;
    return 0;
}

int http_parser::on_url(::llhttp_t* hp, const char* s, size_t len) {
    message_data* md = get_message_data(hp);
    md->hm.url_.append(s, len);
    md->state = state_unknown;
    return 0;
}

int http_parser::on_status(::llhttp_t* hp, const char* s, size_t len) {
    message_data* md = get_message_data(hp);
    if (md->hm.major_ == 0) {
        get_parser(hp)->copy_parser_status(*md);
    }
    md->hm.status_message_.append(s, len);
    md->state = state_unknown;
    return 0;
}

int http_parser::on_header_field(::llhttp_t* hp, const char* s, size_t len) {
    message_data* md = get_message_data(hp);
    if (md->state != state_header_name) {
        md->name1 = md->hm.headers_.length();
        md->state = state_header_name;
    }
    md->hm.headers_.append(s, len);
    return 0;
}

int http_parser::on_header_field_complete(::llhttp_t* hp) {
    message_data* md = get_message_data(hp);
    if (md->state != state_header_name) {
        md->name1 = md->hm.headers_.length();
    }
    md->name2 = md->hm.headers_.length();
    md->hm.headers_.append(": ", 2);
    md->value1 = md->name2 + 2;
    md->state = state_header_value;
    return 0;
}

int http_parser::on_header_value(::llhttp_t* hp, const char* s, size_t len) {
    message_data* md = get_message_data(hp);
    md->hm.headers_.append(s, len);
    return 0;
}

int http_parser::on_header_value_complete(::llhttp_t* hp) {
    message_data* md = get_message_data(hp);
    if (md->state == state_header_value) {
        md->hm.hpairs_.emplace_back(md->name1, md->name2, md->value1, md->hm.headers_.length());
    }
    md->hm.headers_.append("\r\n", 2);
    md->state = state_unknown;
    return 0;
}

int http_parser::on_headers_complete(::llhttp_t* hp) {
    message_data* md = get_message_data(hp);
    get_parser(hp)->copy_parser_status(*md);
    if (md->hm.body_.capacity() < md->hm.body_.length() + hp->content_length) {
        md->hm.body_.reserve(md->hm.body_.length() + hp->content_length);
    }
    return 0;
}

int http_parser::on_chunk_header(::llhttp_t* hp) {
    message_data* md = get_message_data(hp);
    if (md->hm.body_.capacity() < md->hm.body_.length() + hp->content_length) {
        md->hm.body_.reserve(md->hm.body_.length() + hp->content_length);
    }
    return 0;
}

int http_parser::on_body(::llhttp_t* hp, const char* s, size_t len) {
    message_data* md = get_message_data(hp);
    md->hm.body_.append(s, len);
    return 0;
}

int http_parser::on_message_complete(::llhttp_t* hp) {
    message_data* md = get_message_data(hp);
    md->state = state_done;
    // Pause llhttp so receive() task can yield
    return HPE_PAUSED;
}

task<http_message> http_parser::receive(ticket_type ticket) {
    assert(ticket.mutex() == &m_[0]);
    char stackbuf[bufsize];
    message_data md;
    md.hm.status_code(0);
    unique_lock guard(co_await ticket);
    hp_.data = &md;

    while (true) {
        const char* buf;
        size_t nr;
        if (receive_buffer_.empty()) {
            buf = stackbuf;
            auto nx = co_await recv(f_, stackbuf, sizeof(stackbuf));
            if (!nx || *nx == 0) {
                break;
            }
            nr = *nx;
        } else {
            // Drain bytes left from the previous call (pipelined data, or a
            // partial frame we couldn't process in one llhttp_execute).
            buf = receive_buffer_.data();
            nr = receive_buffer_.size();
        }

        llhttp_errno_t err = llhttp_execute(&hp_, buf, nr);

        // save unconsumed bytes in `receive_buffer_`
        size_t consumed = nr;
        if (err != HPE_OK) {
            consumed = llhttp_get_error_pos(&hp_) - buf;
        }
        if (consumed == nr) {
            receive_buffer_.clear();
        } else {
            receive_buffer_ = std::string(buf + consumed, buf + nr);
        }

        if (err == HPE_PAUSED_UPGRADE) {
            llhttp_resume_after_upgrade(&hp_);
            md.state = state_done;
        } else if (err == HPE_PAUSED) {
            // we only pause at a message boundary
            llhttp_resume(&hp_);
            md.state = state_done;
        }
        if (err != HPE_OK || md.state == state_done) {
            copy_parser_status(md);
            break;
        }
        // otherwise, message incomplete: loop to read more.
    }

    if (md.state != state_done && !md.hm.error_) {
        hp_.error = md.hm.error_ = HPE_CLOSED_CONNECTION;
    }
    co_return std::move(md.hm);
}

task<http_parser::ticket_type> http_parser::send_request(http_message m) {
    std::string urlline = std::format("{} {} HTTP/{}.{}\r\n",
        m.method_name(), m.url(), m.http_major(), m.http_minor());
    if (!host_.empty()
        && !m.has_header("host")) {
        m.add_header("Host", host_);
    }
    if (m.has_body_
        && !m.has_header("content-length")
        && !m.has_header("transfer-encoding")) {
        m.add_header("Content-Length", m.body_.length());
    }
    m.headers_.append("\r\n", 2);

    iovec iov[3];
    iov[0] = iovec{ urlline.data(), urlline.length() };
    iov[1] = iovec{ m.headers_.data(), m.headers_.length() };
    size_t iovcnt = 2;
    if (m.body_.length()) {
        iov[2] = iovec{ m.body_.data(), m.body_.length() };
        ++iovcnt;
    }

    // lock for writing, then obtain read ticket
    unique_lock guard(co_await m_[1].lock());
    auto ticket = m_[0].lock();

    co_await sendv_all(f_, iov, iovcnt);
    co_return std::move(ticket);
}

task<> http_parser::send_response(http_message m) {
    // If the response is marked `Connection: close`, then ensure
    // should_keep_alive() returns 0
    http_message::header_iterator connhdr;
    const char* data;
    if (should_keep_alive()
        && (connhdr = m.find_header("connection")) != m.header_end()
        && (connhdr.value().length() == 5
            && (data = connhdr.value().data())
            && (data[0] == 'C' || data[0] == 'c')
            && (data[1] == 'L' || data[1] == 'l')
            && (data[2] == 'O' || data[2] == 'o')
            && (data[3] == 'S' || data[3] == 's')
            && (data[4] == 'E' || data[4] == 'e'))) {
        if (hp_.http_major > 0 && hp_.http_minor > 0) {
            hp_.flags |= F_CONNECTION_CLOSE;
        } else {
            hp_.flags &= ~F_CONNECTION_KEEP_ALIVE;
        }
    }

    std::string_view status_message = m.status_message_.empty()
        ? std::string_view(m.default_status_message(m.status_code()))
        : std::string_view(m.status_message());
    std::string codeline = std::format("HTTP/{}.{} {} {}\r\n",
        m.http_major(), m.http_minor(), m.status_code(), status_message);
    if (m.has_body_
        && !m.has_header("content-length")
        && !m.has_header("transfer-encoding")) {
        m.add_header("Content-Length", m.body_.length());
    }
    m.headers_.append("\r\n", 2);

    iovec iov[3];
    iov[0] = iovec{ codeline.data(), codeline.length() };
    iov[1] = iovec{ m.headers_.data(), m.headers_.length() };
    size_t iovcnt = 2;
    if (m.body_.length()) {
        iov[2] = iovec{ m.body_.data(), m.body_.length() };
        ++iovcnt;
    }

    unique_lock guard(co_await m_[1].lock());
    co_await sendv_all(f_, iov, iovcnt);
}

task<> http_parser::send(http_message m) {
    assert(hp_.type == (int) HTTP_RESPONSE || hp_.type == (int) HTTP_REQUEST);
    if (hp_.type == (int) HTTP_RESPONSE) {
        co_await send_request(std::move(m));
    } else {
        co_await send_response(std::move(m));
    }
}

task<> http_parser::send_response_chunk(std::string str) {
    std::string lenline = std::format("{}\r\n", str.length());
    iovec iov[3];
    iov[0] = iovec{ lenline.data(), lenline.length() };
    iov[1] = iovec{ str.data(), str.length() };
    iov[2] = iovec{ (void*) "\r\n", 2 };
    unique_lock guard(co_await m_[1].lock());
    co_await sendv_all(f_, iov, 3);
}

task<> http_parser::send_response_end_chunk() {
    unique_lock guard(co_await m_[1].lock());
    co_await send_all(f_, "0\r\n\r\n", 5);
}

} // namespace cotamer

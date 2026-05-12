#pragma once
#include "cotamer/cotamer.hh"
#include <functional>
#include <map>
#include <stdexcept>
#include <string>
#include <curl/curl.h>

// cotamer/curl.hh
//    Cotamer integration of libcurl.
//
//    `curl_fetch(url)` returns a `cot::task<curl_response>`; it fetches
//    the given URL and co_returns the response. By default, `curl_fetch`
//    follows redirects. Use `curl_fetch(url, func)` to override the
//    default configuration; `func` is a function object that takes a
//    `CURL*` easy handle pointer.

namespace cotamer {

struct curl_response {
    long status = 0;
    std::string body;
    std::multimap<std::string, std::string, std::less<>> headers;
};

// Fetch `url`. Throws `curl_error` on transport failure.
task<curl_response> curl_fetch(std::string url);

// Same, but call `configure` with the CURL* easy handle after default options
// are installed.
task<curl_response> curl_fetch(std::string url,
                               std::function<void(CURL*)> configure);

// Drop this thread's curl_driver and any connections in its pool.
void curl_reset();


// Thrown by curl_fetch on transport failure. `code()` is the CURLcode,
// `what()` a detailed error message.
class curl_error : public std::runtime_error {
public:
    curl_error(int code, const char* msg)
        : std::runtime_error(msg), code_(code) { }
    int code() const noexcept { return code_; }
private:
    int code_;
};

} // namespace cotamer

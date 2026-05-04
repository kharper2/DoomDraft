cmake_minimum_required(VERSION 3.16)
project(cs2620-pset4 CXX)

# Cotamer uses C++20 `std::format`, `std::formatter`, and chrono `operator<=>`.
# Apple Clang 14 + older Command Line Tools ship a libc++ that is incomplete for
# those features → confusing errors in circular_int.hh, cotamer.cc, timer_heap.hh.
# Fix: Xcode 15+ / CLT 15+, or `brew install llvm` and configure with
#   cmake -B build -DCMAKE_CXX_COMPILER=/opt/homebrew/opt/llvm/bin/clang++
if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"
   AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 15.0)
    message(FATAL_ERROR
        "Apple Clang ${CMAKE_CXX_COMPILER_VERSION} is too old for this tree "
        "(needs full C++20 <format> and chrono <=>). "
        "Install Xcode 15+ / Command Line Tools 15+, or use Homebrew LLVM as CMAKE_CXX_COMPILER.")
endif()

option(SAN "Enable AddressSanitizer+UBSanitizer" OFF)
option(ASAN "Enable AddressSanitizer" OFF)
option(UBSAN "Enable UBSanitizer" OFF)
option(TSAN "Enable ThreadSanitizer" OFF)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include_directories(${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/..)
add_compile_options(-g -O3 -W -Wall)
add_link_options(-g -O3 -W -Wall)

if((SAN AND NOT TSAN) OR UBSAN)
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
endif()
if((SAN AND NOT TSAN) OR ASAN)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
endif()
if(TSAN)
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
endif()

find_path(XXHASH_INCLUDE_DIR xxhash.h HINTS /opt/homebrew/include)
find_library(XXHASH_LIBRARY xxhash HINTS /opt/homebrew/lib)
if(NOT XXHASH_INCLUDE_DIR OR NOT XXHASH_LIBRARY)
    message(FATAL_ERROR "xxhash not found. Install with: brew install xxhash")
endif()

add_library(Cotamer OBJECT
    ../cotamer/cotamer.cc
    ../cotamer/io.cc
)

add_library(Pancy OBJECT
    pancydb.cc
    client_model.cc
    lockseq_model.cc
    utils.cc
)

# Standalone OT unit test — no Paxos, no Cotamer dependency
add_executable(test-doc-ops doc_ops.cc)
target_compile_definitions(test-doc-ops PRIVATE RUN_DOC_OPS_TESTS)

# Standalone doc_state test — pancydb + doc_ops only (no Cotamer when linking
# doc_ops without RUN_DOC_OPS_TESTS; pancydb.hh pulls circular_int)
add_executable(test-doc-state doc_state.cc doc_ops.cc)
target_compile_definitions(test-doc-state PRIVATE RUN_DOC_STATE_TESTS)

# Original Paxos test binary (unchanged from pset3 — sanity check)
add_executable(pt-paxos
    pt-paxos.cc
    $<TARGET_OBJECTS:Cotamer>
    $<TARGET_OBJECTS:Pancy>
)

# Simulation-based collaborative editor test (Person A)
# Becomes active once doc_ops.cc, doc_state.cc, collab_model.cc, pt-collab.cc exist
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/pt-collab.cc)
    add_executable(pt-collab
        pt-collab.cc
        doc_ops.cc
        doc_state.cc
        collab_model.cc
        $<TARGET_OBJECTS:Cotamer>
        $<TARGET_OBJECTS:Pancy>
    )
endif()

# HTTP server + collaborative editor (Person B)
# Becomes active once http_server.cc and pt-collab-server.cc exist
if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/pt-collab-server.cc)
    add_executable(pt-collab-server
        pt-collab-server.cc
        doc_ops.cc
        doc_state.cc
        http_server.cc
        $<TARGET_OBJECTS:Cotamer>
        $<TARGET_OBJECTS:Pancy>
    )
endif()

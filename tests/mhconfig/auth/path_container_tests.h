#ifndef MHCONFIG__AUTH__PATH_CONTAINER_TESTS_H
#define MHCONFIG__AUTH__PATH_CONTAINER_TESTS_H

#include <catch2/catch.hpp>

#include "mhconfig/auth/path_container.h"

namespace mhconfig {
namespace auth {

TEST_CASE("Path container", "[path-container]") {
  SECTION("Empty container") {
    PathContainer<uint64_t> path_container;

    uint64_t r;

    REQUIRE(path_container.find("hello", r) == false);
    REQUIRE(path_container.find("/world", r) == false);
    REQUIRE(path_container.find("/i/am/a/path", r) == false);
  }

  SECTION("Exact path find") {
    PathContainer<uint64_t> path_container;
    REQUIRE(path_container.init({{"/exact/path", 1234}, {"/another", 2343}}) == true);

    uint64_t r;

    REQUIRE(path_container.find("hello", r) == false);

    REQUIRE(path_container.find("/exact/path", r) == true);
    REQUIRE(r == 1234);

    REQUIRE(path_container.find("another", r) == true);
    REQUIRE(r == 2343);
  }

  SECTION("Prefix path find") {
    PathContainer<uint64_t> path_container;
    REQUIRE(path_container.init({{"/prefix/path/*", 82743}, {"/prefix/path/with/*", 11}, {"/prefix/path/with/more/priority", 22}}) == true);

    uint64_t r;

    REQUIRE(path_container.find("/prefix/path/me/or/not", r) == true);
    REQUIRE(r == 82743);

    REQUIRE(path_container.find("/prefix/path/with/priority", r) == true);
    REQUIRE(r == 11);

    REQUIRE(path_container.find("/prefix/path/with/more/priority", r) == true);
    REQUIRE(r == 22);
  }

  SECTION("Any path find") {
    PathContainer<uint64_t> path_container;
    REQUIRE(path_container.init({{"/any/+", 0}, {"/any/special/+", 1}, {"/any/+/path", 2}}) == true);

    uint64_t r;

    REQUIRE(path_container.find("/any/value", r) == true);
    REQUIRE(r == 0);

    REQUIRE(path_container.find("/any/special/path", r) == true);
    REQUIRE(r == 1);

    REQUIRE(path_container.find("/any/another/path", r) == true);
    REQUIRE(r == 2);

    REQUIRE(path_container.find("/any/another/thing", r) == false);
  }

}

}
}

#endif

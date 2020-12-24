#ifndef MHCONFIG__AUTH__PATH_ACL_TESTS_H
#define MHCONFIG__AUTH__PATH_ACL_TESTS_H

#include <catch2/catch.hpp>

#include "mhconfig/auth/path_acl.h"

namespace mhconfig {
namespace auth {

TEST_CASE("Path acl", "[path-acl]") {
  SECTION("Empty acl") {
    PathAcl<uint64_t> path_acl;

    uint64_t r;

    REQUIRE(path_acl.find("hello", r) == false);
    REQUIRE(path_acl.find("/world", r) == false);
    REQUIRE(path_acl.find("/i/am/a/path", r) == false);
  }

  SECTION("Exact path find") {
    PathAcl<uint64_t> path_acl;
    REQUIRE(path_acl.init({{"/exact/path", 1234}, {"/another", 2343}}) == true);

    uint64_t r;

    REQUIRE(path_acl.find("hello", r) == false);

    REQUIRE(path_acl.find("/exact/path", r) == true);
    REQUIRE(r == 1234);

    REQUIRE(path_acl.find("another", r) == true);
    REQUIRE(r == 2343);
  }

  SECTION("Prefix path find") {
    PathAcl<uint64_t> path_acl;
    REQUIRE(path_acl.init({{"/prefix/path/*", 82743}, {"/prefix/path/with/*", 11}, {"/prefix/path/with/more/priority", 22}}) == true);

    uint64_t r;

    REQUIRE(path_acl.find("/prefix/path/me/or/not", r) == true);
    REQUIRE(r == 82743);

    REQUIRE(path_acl.find("/prefix/path/with/priority", r) == true);
    REQUIRE(r == 11);

    REQUIRE(path_acl.find("/prefix/path/with/more/priority", r) == true);
    REQUIRE(r == 22);
  }

  SECTION("Any path find") {
    PathAcl<uint64_t> path_acl;
    REQUIRE(path_acl.init({{"/any/+", 0}, {"/any/special/+", 1}, {"/any/+/path", 2}}) == true);

    uint64_t r;

    REQUIRE(path_acl.find("/any/value", r) == true);
    REQUIRE(r == 0);

    REQUIRE(path_acl.find("/any/special/path", r) == true);
    REQUIRE(r == 1);

    REQUIRE(path_acl.find("/any/another/path", r) == true);
    REQUIRE(r == 2);

    REQUIRE(path_acl.find("/any/another/thing", r) == false);
  }

}

}
}

#endif

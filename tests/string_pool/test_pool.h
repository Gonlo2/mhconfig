#ifndef STRING_POOL__TEST_POOL_H
#define STRING_POOL__TEST_POOL_H

#include <catch2/catch.hpp>

#include "string_pool/pool.h"

namespace string_pool {

TEST_CASE("String", "[string]") {
  SECTION("Small string from std::string") {
    String hello("hello");
    REQUIRE(hello == "hello");
    REQUIRE(hello.str() == "hello");
    REQUIRE(hello.hash() == 122511465736203ul);
    REQUIRE(hello != "test");
    REQUIRE(hello.size() == 5);
  }

  SECTION("Small string from uint64_t") {
    String hello(122511465736203ul);
    REQUIRE(hello == "hello");
    REQUIRE(hello.str() == "hello");
    REQUIRE(hello.hash() == 122511465736203ul);
    REQUIRE(hello != "test");
    REQUIRE(hello.size() == 5);
  }

  SECTION("Large string from std::string") {
    std::string lorem("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.");
    String s(lorem);
    REQUIRE(s == lorem);
    REQUIRE(s.str() == lorem);
    REQUIRE(s.hash() == 10109368953727484612ul);
    REQUIRE(s != "..................................................");
    REQUIRE(s.size() == 295);
  }
}

TEST_CASE("String pool", "[string-pool]") {
  SECTION("Add small string") {
    Pool pool;
    auto s = pool.add("world");
    REQUIRE(s == "world");
    REQUIRE(pool.stats().num_strings == 0);
  }

  SECTION("Add large string") {
    Pool pool;
    std::string lorem = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.";
    auto s = pool.add(lorem);
    REQUIRE(s == lorem);
    REQUIRE(pool.stats().num_strings == 1);
  }

  SECTION("Add multiple times the same string") {
    Pool pool;
    std::string lorem = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.";
    std::vector<String> strings;
    for (size_t i = 0; i < 10000; ++i) {
      strings.push_back(pool.add(lorem));
    }
    REQUIRE(pool.stats().num_strings == 1);
  }

  SECTION("Automatic chunk cleanup after string removal") {
    Pool pool;
    std::vector<String> strings;
    for (size_t i = 0; i < 500; ++i) {
      std::string s(10000, '\0');
      s[i] = 'x';
      strings.push_back(pool.add(s));
    }
    REQUIRE(pool.stats().num_strings == 500);
    REQUIRE(pool.stats().num_chunks == 2);

    strings.clear();

    REQUIRE(pool.stats().num_strings < 500);
    REQUIRE(pool.stats().num_chunks == 2);
  }

  SECTION("Force pool compaction") {
    Pool pool;
    std::vector<String> strings;
    for (size_t i = 0; i < 500; ++i) {
      std::string s(10000, '\0');
      s[i] = 'x';
      strings.push_back(pool.add(s));
    }
    REQUIRE(pool.stats().num_strings == 500);
    REQUIRE(pool.stats().num_chunks == 2);

    strings.clear();
    pool.compact();

    REQUIRE(pool.stats().num_strings == 0);
    REQUIRE(pool.stats().num_chunks == 2);

    for (size_t i = 0; i < 500; ++i) {
      std::string s(10000, '\0');
      s[i] = 'x';
      strings.push_back(pool.add(s));
    }
    REQUIRE(pool.stats().num_strings == 500);
    REQUIRE(pool.stats().num_chunks == 2);
  }

}

}

#endif

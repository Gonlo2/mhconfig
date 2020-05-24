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
    REQUIRE(hello.hash() == 122511465736213ul);
    REQUIRE(hello != "test");
    REQUIRE(hello.size() == 5);
    REQUIRE(hello.is_small() == true);
  }

  SECTION("Small string from uint64_t") {
    String hello(122511465736213ul);
    REQUIRE(hello == "hello");
    REQUIRE(hello.str() == "hello");
    REQUIRE(hello.hash() == 122511465736213ul);
    REQUIRE(hello != "test");
    REQUIRE(hello.size() == 5);
    REQUIRE(hello.is_small() == true);
  }

  SECTION("Small+ string from std::string") {
    String remembered("remembered");
    REQUIRE(remembered == "remembered");
    REQUIRE(remembered.str() == "remembered");
    REQUIRE(remembered.hash() == 883906214080811291ull);
    REQUIRE(remembered != "¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬");
    REQUIRE(remembered.size() == 10);
    REQUIRE(remembered.is_small() == true);
  }

  SECTION("Small+ string from uint64_t") {
    String remembered(883906214080811291ull);
    REQUIRE(remembered == "remembered");
    REQUIRE(remembered.str() == "remembered");
    REQUIRE(remembered.hash() == 883906214080811291ull);
    REQUIRE(remembered != "¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬");
    REQUIRE(remembered.size() == 10);
    REQUIRE(remembered.is_small() == true);
  }

  SECTION("Potential small+ string from std::string") {
    String remembered("127.0.0.1");
    REQUIRE(remembered == "127.0.0.1");
    REQUIRE(remembered.size() == 9);
    REQUIRE(remembered.is_small() == false);
  }

  SECTION("Large string from std::string") {
    std::string lorem("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.");
    String s(lorem);
    REQUIRE(s == lorem);
    REQUIRE(s.str() == lorem);
    REQUIRE(s.hash() == 10109368953727484612ul);
    REQUIRE(s != "..................................................");
    REQUIRE(s.size() == 295);
    REQUIRE(s.is_small() == false);
  }

  SECTION("Small string from std::string in utf8") {
    String hello("¬¬");
    REQUIRE(hello == "¬¬");
    REQUIRE(hello.str() == "¬¬");
    REQUIRE(hello.hash() == 742000476689ul);
    REQUIRE(hello != "test");
    REQUIRE(hello.size() == 4);
    REQUIRE(hello.is_small() == true);
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

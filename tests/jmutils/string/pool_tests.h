#ifndef JMUTILS__STRING__POOL_TESTS_H
#define JMUTILS__STRING__POOL_TESTS_H

#include <catch2/catch.hpp>

#include "jmutils/string/pool.h"

namespace jmutils {
namespace string {

TEST_CASE("String", "[string]") {
  SECTION("Small string from std::string") {
    InternalString internal_string;
    String hello = make_string("hello", &internal_string);
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
    InternalString internal_string;
    String remembered = make_string("remembered", &internal_string);
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
    InternalString internal_string;
    String s = make_string("127.0.0.1", &internal_string);
    REQUIRE(s == "127.0.0.1");
    REQUIRE(s.size() == 9);
    REQUIRE(s.is_small() == false);
  }

  SECTION("Large string from std::string") {
    InternalString internal_string;
    std::string lorem("Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.");
    String s = make_string(lorem, &internal_string);
    REQUIRE(s == lorem);
    REQUIRE(s.str() == lorem);
    REQUIRE(s.hash() == 10109368953727484612ul);
    REQUIRE(s != "..................................................");
    REQUIRE(s.size() == 295);
    REQUIRE(s.is_small() == false);
  }

  SECTION("Small string from std::string in utf8") {
    InternalString internal_string;
    String s = make_string("¬¬", &internal_string);
    REQUIRE(s == "¬¬");
    REQUIRE(s.str() == "¬¬");
    REQUIRE(s.hash() == 742000476689ul);
    REQUIRE(s != "test");
    REQUIRE(s.size() == 4);
    REQUIRE(s.is_small() == true);
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
    REQUIRE(pool.stats().num_chunks == 1);
  }

  SECTION("Automatic chunk cleanup after string removal") {
    Pool pool;
    std::vector<String> strings;
    for (size_t i = 0; i < 150; ++i) {
      std::string s(10000, '\0');
      s[i] = 'x';
      strings.push_back(pool.add(s));
    }
    REQUIRE(pool.stats().num_strings == 150);
    REQUIRE(pool.stats().num_chunks == 2);

    strings.clear();

    REQUIRE(pool.stats().num_strings < 150);
    REQUIRE(pool.stats().num_chunks == 2);
  }

  SECTION("Force pool compaction") {
    Pool pool;
    std::vector<String> strings;
    for (size_t i = 0; i < 150; ++i) {
      std::string s(10000, '\0');
      s[i] = 'x';
      strings.push_back(pool.add(s));
    }
    REQUIRE(pool.stats().num_strings == 150);
    REQUIRE(pool.stats().num_chunks == 2);

    strings.clear();
    pool.compact();

    REQUIRE(pool.stats().num_strings == 0);
    REQUIRE(pool.stats().num_chunks == 2);

    for (size_t i = 0; i < 150; ++i) {
      std::string s(10000, '\0');
      s[i] = 'x';
      strings.push_back(pool.add(s));
    }
    REQUIRE(pool.stats().num_strings == 150);
    REQUIRE(pool.stats().num_chunks == 2);
  }

}

}
}

#endif

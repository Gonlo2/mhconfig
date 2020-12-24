#ifndef JMUTILS__CONTAINER__LABEL_SET_TESTS_H
#define JMUTILS__CONTAINER__LABEL_SET_TESTS_H

#include <catch2/catch.hpp>

#include "jmutils/container/label_set.h"

namespace jmutils {
namespace container {

void test_labels(
  LabelSet<uint64_t>& lbl_set,
  const Labels& labels,
  uint64_t value
) {
  *lbl_set.get_or_build(labels) = value;
  REQUIRE(!lbl_set.empty());
  REQUIRE(*lbl_set.get(labels) == value);
  {
    size_t count = 0;
    bool ok = lbl_set.for_each([&labels, &count, value](const auto& l, auto* v) {
      REQUIRE(l == labels);
      REQUIRE(*v == value);
      ++count;
      return true;
    });
    REQUIRE(ok);
    REQUIRE(count == 1);
  }
  {
    size_t count = 0;
    bool ok = lbl_set.for_each_subset(
      labels,
      [&labels, &count, value](const auto& l, auto* v) {
        REQUIRE(l == labels);
        REQUIRE(*v == value);
        ++count;
        return true;
      }
    );
    REQUIRE(ok);
    REQUIRE(count == 1);
  }

  REQUIRE(lbl_set.remove(labels));
  REQUIRE(lbl_set.empty());
  REQUIRE(lbl_set.get(labels) == nullptr);

  {
    bool ok = lbl_set.for_each([](const auto&, auto*) {
      REQUIRE(false);
      return true;
    });
    REQUIRE(ok);
  }
  {
    bool ok = lbl_set.for_each_subset(
      labels,
      [](const auto&, auto*) {
        REQUIRE(false);
        return true;
      }
    );
    REQUIRE(ok);
  }
}

TEST_CASE("Label set", "[label-set]") {
  SECTION("Initial set") {
    LabelSet<uint64_t> lbl_set;
    Labels labels;
    REQUIRE(lbl_set.empty());
    REQUIRE(lbl_set.get(labels) == nullptr);
    {
      bool ok = lbl_set.for_each([](const auto&, auto*) {
        REQUIRE(false);
        return true;
      });
      REQUIRE(ok);
    }
    {
      bool ok = lbl_set.for_each_subset(
        labels,
        [](const auto&, auto*) {
          REQUIRE(false);
          return true;
        }
      );
      REQUIRE(ok);
    }
  }

  SECTION("Empty label") {
    LabelSet<uint64_t> lbl_set;
    Labels labels;
    test_labels(lbl_set, labels, 123456789ull);
  }

  SECTION("Some label") {
    LabelSet<uint64_t> lbl_set;
    Labels labels = make_labels(
      {{"key1", "value_1"}, {"key2", "value_2"}, {"xxx", "xxx"}}
    );
    test_labels(lbl_set, labels, 82347234823ull);
  }

  SECTION("Test subset") {
    LabelSet<uint64_t> lbl_set;

    auto labels_1 = make_labels({{"key1", "value_1"}, {"key2", "value_2"}});
    auto labels_2 = make_labels({{"key2", "xxx"}});
    auto labels_3 = make_labels({{"key1", "value_1"}, {"key2", "value_2"}, {"a", "b"}});
    auto labels_4 = make_labels({{"key1", "value_1"}, {"key3", "xxx"}});

    lbl_set.insert(labels_1, 123456789ull);
    lbl_set.insert(labels_2, 82347234823ull);
    lbl_set.insert(labels_3, 1);
    lbl_set.insert(labels_4, 33);

    {
      auto labels = make_labels(
        {{"key1", "value_1"}, {"key2", "value_2"}, {"a", "b"}, {"key3", "xxx"}}
      );
      absl::flat_hash_map<Labels, uint64_t> seen;
      bool ok = lbl_set.for_each_subset(
        labels,
        [&seen](const auto& l, auto* v) {
          seen[l] = *v;
          return true;
        }
      );
      REQUIRE(ok);
      REQUIRE(seen.size() == 3);
      REQUIRE(seen[labels_1] == 123456789ull);
      REQUIRE(seen[labels_3] == 1);
      REQUIRE(seen[labels_4] == 33);
    }

    {
      auto labels = make_labels(
        {{"key1", "value_1"}, {"key2", "xxx"}, {"a", "b"}, {"key3", "xxx"}}
      );
      absl::flat_hash_map<Labels, uint64_t> seen;
      bool ok = lbl_set.for_each_subset(
        labels,
        [&seen](const auto& l, auto* v) {
          seen[l] = *v;
          return true;
        }
      );
      REQUIRE(ok);
      REQUIRE(seen.size() == 2);
      REQUIRE(seen[labels_2] == 82347234823ull);
      REQUIRE(seen[labels_4] == 33);
    }
  }

}

}
}

#endif

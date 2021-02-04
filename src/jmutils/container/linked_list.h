#ifndef JMUTILS__STRUCTURES__LINKED_LIST_H
#define JMUTILS__STRUCTURES__LINKED_LIST_H

#include <utility>
#include <iterator>

namespace jmutils
{
namespace container
{

template <typename T>
class LinkedList final
{
private:
  struct node_t {
    T value;
    node_t* next{nullptr};
  };

  template <typename ref, typename ptr>
  class Iterator final
  {
  public:
    typedef T value_type;
    typedef std::forward_iterator_tag iterator_category;
    typedef int difference_type;

    Iterator operator++() {
      Iterator o = *this;
      if (node_ != nullptr) {
        node_ = node_->next;
      }
      return o;
    }

    Iterator operator++(int junk) {
      if (node_ != nullptr) {
        node_ = node_->next;
      }
      return *this;
    }

    ref operator*() { return node_->value; }
    ptr operator->() { return &node_->value; }

    bool operator==(const Iterator& rhs) { return node_ == rhs.node_; }
    bool operator!=(const Iterator& rhs) { return node_ != rhs.node_; }

  private:
    friend class LinkedList;

    node_t* node_;

    Iterator(node_t* node) : node_(node) { }
  };

  node_t* first_;
  node_t* last_;

  void push_back(node_t* node) {
    if (first_ == nullptr) {
      first_ = node;
      last_ = node;
    } else {
      last_->next = node;
      last_ = node;
    }
  }

public:
  using iterator = Iterator<T&, T*>;
  using const_iterator = Iterator<const T&, const T*>;

  LinkedList() : first_(nullptr), last_(nullptr) {
  }

  ~LinkedList() {
    node_t* x = first_;
    while (x != nullptr) {
      node_t* next = x->next;
      delete x;
      x = next;
    }
  }

  LinkedList(const LinkedList& o) = delete;
  LinkedList& operator=(const LinkedList& o) = delete;

  LinkedList(
    LinkedList&& o
  ) : first_(o.first_),
    last_(o.last_)
  {
    o.first_ = nullptr;
    o.last_ = nullptr;
  }

  LinkedList& operator=(
    LinkedList&& o
  ) {
    std::swap(first_, o.first_);
    std::swap(last_, o.last_);
    return *this;
  }

  iterator begin() const {
    return iterator(first_);
  }

  iterator end() const {
    return iterator(nullptr);
  }

  const_iterator cbegin() const {
    return const_iterator(first_);
  }

  const_iterator cend() const {
    return const_iterator(nullptr);
  }

  bool empty() const {
    return first_ == nullptr;
  }

  T& front() const {
    assert (first_ != nullptr);
    return first_->value;
  }

  void pop_front() {
    assert (first_ != nullptr);
    node_t* node = first_;
    if (first_ == last_) {
      first_ = nullptr;
      last_ = nullptr;
    } else {
      first_ = first_->next;
    }
    delete node;
  }

  void push_back(const T& v) {
    node_t* node = new node_t;
    assert (node != nullptr);
    node->value = v;
    push_back(node);
  }

  void push_back(T&& v) {
    node_t* node = new node_t;
    assert (node != nullptr);
    node->value = std::move(v);
    push_back(node);
  }

  template <typename L>
  void remove_if(L lambda) {
    while ((first_ != nullptr) && lambda(first_->value)) {
      node_t* next = first_->next;
      delete first_;
      first_ = next;
    }

    last_ = first_;
    node_t* node = first_ != nullptr ? first_->next : nullptr;
    while (node != nullptr) {
      if (lambda(node->value)) {
        node_t* next = node->next;
        delete node;
        node = next;
      } else {
        last_->next = node;
        last_ = node;
        node = node->next;
      }
    }

    if (last_ != nullptr) {
      last_->next = nullptr;
    }
  }
};

} /* container */
} /* jmutils */

#endif

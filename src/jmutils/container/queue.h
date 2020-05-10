#ifndef JMUTILS__STRUCTURES__THREAD_SAFE_QUEUE_H
#define JMUTILS__STRUCTURES__THREAD_SAFE_QUEUE_H

#include <stdlib.h>

#include "blockingconcurrentqueue.h"

namespace jmutils
{
namespace container
{

template <typename T>
class Queue
{
public:
  Queue() {
  }
  virtual ~Queue() {
  }

  Queue(const Queue&) = delete;
  Queue(Queue&&) = delete;

  void pop(T& result) {
    queue_.wait_dequeue(result);
  }

  // TODO check if it was possible to add the item
  void push(T item) {
    queue_.enqueue(item);
  }

  // TODO check if it was possible to add the item
  template <typename Container>
  void push_all(Container items) {
    queue_.enqueue_bulk(items.data(), items.size());
  }

private:
  moodycamel::BlockingConcurrentQueue<T> queue_;
};

} /* container */
} /* jmutils */

#endif

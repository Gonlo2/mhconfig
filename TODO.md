# Pending taks
- [ ] Send a error if the path don't exists.
- [ ] Ignore hiden files/dirs.
- [ ] Add sampling to the metrics, preferably by request.
- [ ] Replace the queue with a channel.
- [ ] Add doc.
- [ ] Add tests.
- [ ] Allow increase the number of waiting requests.
- [ ] Reuse the requests.
- [ ] Change the do_topological_sort_over_ref_graph_rec to iterative version.
- [ ] Use two kind of workers: low priority for optimization and high priority for setup/get/update.
- [ ] Change the namespace_id and another values that return a u64 with the grpc API to i64 to avoid problems with languajes
  without u64 support like java.
- [ ] Check if the service handle correctly empty overrides.
- [ ] Create a proxy for the metrics to avoid preprocess the summary data online.
- [ ] Allow remove the watchers.
- [ ] Change proto definition to have at leat a key of one element.
- [ ] Review the cmake configuration (too slow).

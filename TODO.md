# TODO
- Add sampling to the metrics, preferably by request
- Use a minimal perfect hashing for the optimized merge config
- Allow subscribe to config updates
- Optimize in a worker the config if they are in use (after the first garbage cycle?)
- Replace the queue with a channel
- Add doc
- Add tests
- Optimize the make_overrides_key
- Allow increase the number of waiting requests
- Reuse the requests
- Change the do_topological_sort_over_ref_graph_rec to iterative version
- Use two kind of workers: low priority for optimization and high priority for setup/get/update
- Check the string_pool
- Change the namespace_id and another values that return a u64 with the grpc API to i64 to avoid problems with languajes
  without u64 support like java.

# Doing

# Done
- Enable again the garbage collector

# Pending taks
- Add a configuration file
- Allow only the config folders with a prefix
- Stop gracefully the service.
- Add tests.
- Allow increase the number of waiting requests.
- Reuse the requests.
- Use two kind of workers: low priority for optimization and high priority for setup/get/update.
- Change the namespace_id and another values that return a u64 with the grpc API to i64 to avoid problems with languajes
  out u64 support like java.
- Check if the service handle correctly empty overrides.
- Review the cmake configuration (too slow).
- Create a time worker for the garbage collector and the usage stats.

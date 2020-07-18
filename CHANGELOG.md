## Changelog

### Next
- Added a time worker with a better control of the trigger times
- Add trace function to the api
- Fix memory leak if some request/stream finish without any event in the completion queue
- Fix non unique override key if the document use references
- Fix don't watch a override if the config not already created
- Fix wrong logic for the topological sort
- Add support for config flavors
- Fix deadlocks and memory leaks

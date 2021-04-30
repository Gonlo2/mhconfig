## Changelog

### Next
- Fix that a sequence could discard elements if some element changed
- Reduce the size of the logs, remove some logs and improve the map logging
- Refactor of the elements class to store the position of the config elements in the source files
- Send logs about the contruction/merge operations
- Changed sorted paths with labels
- Added a time worker with a better control of the trigger times
- Add trace function to the api
- Fix memory leak if some request/stream finish without any event in the completion queue
- Fix non unique override key if the document use references
- Fix don't watch a override if the config not already created
- Fix wrong logic for the topological sort
- Add support for config flavors
- Fix deadlocks and memory leaks

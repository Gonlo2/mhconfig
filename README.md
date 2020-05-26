![](/assets/logo.readme.png?raw=true "MHConfig logo")

# Introduction

MHConfig is a Multi Hierarchy Configuration server to obtain reproducible and online configuration in a easy way.

The idea of this service is to solve a series of problems:

- The coupling of the code with the configuration, which prevents an agile deployment either to test experimental functionalities, to modify the existing functionality or to deprecate a functionality.
- Configuration management in the case of multiple environments (e.g. development, pre-production and production), data centers or services
- Allow to detect configuration changes and to reload them hot without losing the possibility to have a consistent view (two requests have to return the same configuration even if it has been changed if necessary).
- Avoid the high computational cost of having this logic in a separate library for each of the services.

## Getting Started

### Prerequisites

This service need cmake and a c++ compiler, to facilitate the build process a docker image with all the requirements is
provided, in this case you only need to have installed docker, docker-compose and [an-ci](https://github.com/Gonlo2/an-ci).

First of all you need to build the docker-image

```bash
cd dockerfiles/cpp-builder/
docker build -t mhconfig-builder:0.1  .
```

After that go to the root path, prepare the thirdparties and build the server

```bash
cd ../..
git submodule update --init
an-ci third_party
an-ci build
```

You could find the executable in `./build/mhconfig`

### Execution

To run the program execute

```bash
./mhconfig <gRPC listen address> <prometheus listen address> <num grpc threads> <num workers>
```

For example

```bash
SPDLOG_LEVEL=debug ./mhconfig 0.0.0.0:2222 0.0.0.0:1111 13 13
```

To test it you could implement a client using the protobuf file `./src/mhconfig/proto/mhconfig.proto` or using some tool
like [grpcurl](https://github.com/fullstorydev/grpcurl) or [ghz](https://ghz.sh/).

PS: the logger configuration format could be obtained from https://github.com/gabime/spdlog/releases/tag/v1.6.0


## Config

The configuration use YAML files and the mapping between files and configuration is simple, the file name is the first level of the configuration and the file content is
appended to this first level.

### Tags

Some custom tags are allowed to facilitate the configuration administration.

* `!format` allow format a string with some parameters
* `!ref` insert the configuration of another file if don't exists a circular dependency
* `!sref` insert a scalar or a null value from the same configuration file
* `!delete` remove the previous element with that path
* `!override` force override one value instead merge it.

#### Format

The `!format` tag allow create a string from some named variables, it use [fmt](https://fmt.dev/latest/syntax.html) under
the hood so it's possible use all the flexibility that it provide. The syntax is the next:

```yaml
!format
- "{host}:{port} (database: {database}, table: {table})"
- host: 127.0.0.1
  port: 2332
  database: test_me
  table: !sref [another, table, path]
```

Take in mind that it's possible use references in the variable part, so it's possible write something like:

```yaml
!format
- "{host}:{port} (database: {database}, table: {table})"
- !ref [databaseParameters, default]
```

PS: fmt don't allow the usage of variable named arguments so it's supported only up to eight named arguments.

#### Reference and scalar reference

The tag `!ref` allow use some configuration provided in another configuration file if don't exists circular
dependencies, also it's possible override that configuration in a next override. Like in some cases it's necessary reuse
some value provided in the same configuration the `!sref` allow do that but only if the referenced element is a scalar or null.

#### Delete

If it's necessary remove the content of a previous override it's possible do that with the `!delete` tag.

#### Override

By default only it's possible merge values of the same kind with the overrides, to force the override it's necessary use
the tag `!override`.

### Restrictions

The overrides have some restrictions:

* A scalar or null value can only be overwritten with another scalar or null value, if you want to overwrite it you have to use the `!override` tag to force it.
* A map or a list can only be merged with another element of the same type, if you want to overwrite it you must use the `!override` tag.
* The `!ref` tag can only be used if the resulting configuration does not have cycles, that is, you cannot refer to a configuration directly or indirectly uses the self configuration.
* You can only use the sref tag for scalars in the document.

## API

### Common

Most methods that make use of a configuration path return a namespace identifier, this value is a 64bits value generated randomly and its purpose is to tell the client if the version number is still valid, if the instance has been restarted, etc. From now on we will call the set of configurations that are in a particular path namespace.

Also this kind of responses return a version number that allow reproducible results for some time in the case that the
configuration has been updated between two calls. Take in mind that returns a different value does not mean that the required configuration has been changed, but rather that some configuration has been updated.

### Get

This method allow retrieve a configuration and the protobuf definition is:

```protobuf
message GetRequest {
  // the root path of the namespace to obtain the configuration.
  string root_path = 1;
  // the list of overrides to apply from lower to higher priority.
  repeated string overrides = 2;
  // the version of the configuration to retrieve, it retrieve the latest
  // version with a zero value and the proper version otherwise.
  uint32 version = 3;
  // the key to retrieve and it must have at least one element.
  repeated string key = 4;
}

message GetResponse {
  enum Status {
    // the operation take place successfully.
    OK = 0;
    //some error take place.
    ERROR = 1;
    // has been requested a removed or inexistent version.
    INVALID_VERSION = 2;
    // in the case of use the `ref` tag, the dependency graph have a cycle.
    REF_GRAPH_IS_NOT_DAG = 3;
  }
  Status status = 1;
  uint64 namespace_id = 2;
  // the returned version, it's the last version if the asked version was the zero.
  uint32 version = 3;
  repeated Element elements = 4;
}
```

### Update

This method allow notify that a configuration file has been changed. It's necessary to specify that the updates comply with three properties: atomicity because all the configuration changes are visible at the same time, consistency because if some configuration file is bad formed one of them will be updated and isolation because it's possible retrieve the configurations like in some snapshot (at least for some time).

```protobuf
message UpdateRequest {
  // the root path of the namespace that has been changed.
  string root_path = 1;
  // the list of paths to check for changes, this apply to creations, modifications and deletions.
  repeated string relative_paths = 2;
}

message UpdateResponse {
  uint64 namespace_id = 1;
  enum Status {
    OK = 0;
    ERROR = 1;
  }
  Status status = 2;
  // the new version.
  uint32 version = 3;
}
```

### Watch

To avoid sporadic requests to the service to detect configuration changes it's possible watch some configuration
documents, it this case a stream is created and the service will send the new configuration when some change take place.

```protobuf
message WatchRequest {
  // the id to assign to the watcher (this is assigned by the client).
  uint32 uid = 1;
  // if the purpose of the call is remove the watcher with the provided uid.
  bool remove = 2;
  // the root path of the namespace to obtain the configuration.
  string root_path = 3;
  // the list of overrides to apply from lower to higher priority.
  repeated string overrides = 4;
  // the last know version of the configuration, in the case of zero the server
  // will reply the latest one, in other case the server will reply when exists
  // a newer version that change the configuration. Please note that this means
  // that the server can skip several versions if the server don't change the
  // requested document directly or indirectly
  uint32 version = 5;
  // document to watch.
  string document = 6;
}

message WatchResponse {
  // the id assigned to the watcher.
  uint32 uid = 1;
  enum Status {
    OK = 0;
    ERROR = 1;
    // has been requested a removed or inexistent version
    INVALID_VERSION = 2;
    // in the case of use the `ref` tag, the dependency graph have a cycle.
    REF_GRAPH_IS_NOT_DAG = 3;
    // the provided uid is already in use.
    UID_IN_USE = 4;
    // the provided uid don't exists in the system.
    UNKNOWN_UID = 5;
    // the provided uid has been removed, keep in mind that it's possible
    // to have career conditions in this method if:
    // - a file has been changed and processing of the watcher has begun
    // - an watcher has been requested to be removed
    // - it's returned that the watcher has been removed
    // - a configuration with the removed watcher identifier is returned
    // PS: the server could remove the watcher if the namespace has been
    // deleted and it's the client responsibility to re-register if it
    // wish to do so
    REMOVED = 6;
  }
  Status status = 2;
  uint64 namespace_id = 3;
  uint32 version = 4;
  repeated Element elements = 5;
}
```

### Run garbage collector

This is a administration method which allows for finer control of the garbage collector although it is not necessary to use it for normal use.

```protobuf
message RunGCRequest {
  enum Type {
    CACHE_GENERATION_0 = 0;
    CACHE_GENERATION_1 = 1;
    CACHE_GENERATION_2 = 2;
    DEAD_POINTERS = 3;
    NAMESPACES = 4;
    VERSIONS = 5;
  }
  Type type = 1;
  uint32 max_live_in_seconds = 2;
}

message RunGCResponse {
}
```


## License

This project is licensed under the AGPL-3.0 - see the [LICENSE.md](LICENSE.md) file for details

## Third party libraries

* CMPH: http://cmph.sourceforge.net/index.html
* fmt: https://github.com/fmtlib/fmt
* grpc: https://grpc.io/
* prometheus-cpp: https://github.com/jupp0r/prometheus-cpp
* spdlog: https://github.com/gabime/spdlog
* yaml-cpp: https://github.com/jbeder/yaml-cpp

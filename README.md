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
docker build -t mhconfig-builder:0.2  .
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
./mhconfig <daemon config path> <gRPC listen address> <prometheus listen address> <num grpc threads> <num workers>
```

For example

```bash
SPDLOG_LEVEL=debug ./mhconfig daemon_config 0.0.0.0:2222 0.0.0.0:1111 13 13
```

To test it you could implement a client using the protobuf file `./src/mhconfig/proto/mhconfig.proto` or using some tool
like [grpcurl](https://github.com/fullstorydev/grpcurl) or [ghz](https://ghz.sh/).

PS: the logger configuration format could be obtained from https://github.com/gabime/spdlog/releases/tag/v1.6.0
PS2: for the moment it isn't possible to reload the configuration of the daemon.


## Config

The configuration use YAML files and the mapping between files and configuration is simple, the file name is the first level of the configuration and the file content is
appended to this first level.

### Tags

Some custom tags are allowed to facilitate the configuration administration.

* `!format` allow format a string with some parameters.
* `!ref` insert the configuration of another file if don't exists a circular dependency.
* `!sref` insert a scalar or a null value from the same configuration file.
* `!delete` remove the previous element with that path.
* `!override` force override one value instead merge it.
* `!!str`, `!!binary`, `!!int`, `!!float` and `!!bool` defines the type of a scalar, being a string type by default.

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
some value provided in the same configuration document the `!sref` allow do that but only if the referenced element is a scalar or null.

#### Delete

If it's necessary remove the content of a previous override it's possible do that with the `!delete` tag.

#### Override

By default only it's possible merge values of the same kind with the overrides, to force the override it's necessary use
the tag `!override`.

### Flavors

Sometimes you want to change the configuration according to multiple logical dimensions, for these cases you have to use the flavors.

The way to put the settings together is to repeat the overrides for each flavor, so if we have the overrides `override-1` and `override-2` and the flavors `flavor-1` and `flavor-2` would be the same as if we put the settings together in the following order

```
<no-flavor> / <override-1>
<no-flavor> / <override-2>
<flavor-1> / <override-1>
<flavor-1> / <override-2>
<flavor-2> / <override-1>
<flavor-2> / <override-2>
```

The way to define a flavor is by the name of the file itself, up to the first point is the document while from this point is the flavor. For example, the file `service-routes.virtual-environment.mr-universe.yaml` is interpreted as the `virtual-environment.mr-universe` flavor of the `service-routes` document.

### Restrictions

The overrides have some restrictions:

* A scalar or null value can only be overwritten with another scalar or null value, if you want to overwrite it you have to use the `!override` tag to force it.
* A map or a list can only be merged with another element of the same type, if you want to overwrite it you must use the `!override` tag.
* The `!ref` tag can only be used if the resulting configuration does not have cycles, that is, you cannot refer to a configuration directly or indirectly uses the self configuration.
* You can only use the sref tag for scalars in the document.

## Access Control Lists

It is possible to configure the server to control access to sensitive resources or procedures, that said, the permission logic can be divided into the following parts: policies, entities and tokens.

### Policies

A policy allows you to restrict access to resources based on root path and overrides. Each policy is
defined in the `policy` subfolder of the configuration folder as a YAML file, whose name is the policy name, this file
has the following structure:

```yaml
root_path:
- path: /mnt/data/mhconfig/+
  capabilities: [GET, WATCH, TRACE]
overrides:
- path: '*'
  capabilities: [GET, WATCH]
- path: /dev/*
  capabilities: [GET, WATCH, TRACE]
```

Where a path could have two special characters:
- The character `+` to denote any value within a single path segment.
- The character `*` to denote any number of path segments, this can only be used at the end of the path.

And the possible capabilities are: `GET`, `WATCH`, `TRACE`, `UPDATE` (it only check the `root_path`) and `RUN_GC` (it only check the entity capability).

### Entities

A entity encapsulates the capabilities and policies that an being can have. Each entity is
defined in the `entity` subfolder of the configuration folder as a YAML file, whose name is the entity name, this file
has the following structure:

```yaml
capabilities: [GET]
policies:
- test
```

Policies are ordered from lowest to highest priority, so if two or more policies have the same path, the one with the highest priority will be used.

It's also necessary to take into account that the capabilities of the entity have priority over those of the paths and, therefore, even if an operation is allowed on a path if it is not also defined in the entity, the permission will be denied.

### Tokens

A token allows to authenticate an entity and configure some authentication parameters, although at the moment it only allows you to define the expiration date of the token through a unix timestamp. Each token is
defined in the `token` subfolder of the configuration folder as a YAML file, whose name is the token itself, this file has the following structure:

```yaml
expires_on: 1595800000  # A zero value indicate that the token don't expire
entity: test
```

This token is specified in the gRPC call through the metadata `mhconfig-auth-token`.

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
  // the list of flavors to apply from lower to higher priority.
  repeated string flavors = 6;
  // the version of the configuration to retrieve, it retrieve the latest
  // version with a zero value and the proper version otherwise.
  uint32 version = 3;
  // the document to retrieve
  string document = 4;
}

message GetResponse {
  enum Status {
    OK = 0;
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
  // the list of paths to check for changes, this apply to creations,
  // modifications and deletions.
  repeated string relative_paths = 2;
  // reload all the config files of the namespace, if this flag is
  // on the relative_paths are ignored
  bool reload = 3;
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
  // the list of flavors to apply from lower to higher priority.
  repeated string flavors = 8;
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
  enum Status {
    OK = 0;
    // if some error take place the watcher will be removed
    ERROR = 1;
    // has been requested a removed or inexistent version
    INVALID_VERSION = 2;
    // in the case of use the `ref` tag, the dependency graph have a cycle.
    REF_GRAPH_IS_NOT_DAG = 3;
    // the provided uid is already in use.
    UID_IN_USE = 20;
    // the provided uid don't exists in the system.
    UNKNOWN_UID = 21;
    // the provided uid has been removed, keep in mind that it's possible
    // to have career conditions in this method if:
    // - a file has been changed and processing of the watcher has begun
    // - an watcher has been requested to be removed
    // - it's returned that the watcher has been removed
    // - a configuration with the removed watcher identifier is returned
    // PS: the server could remove the watcher if the namespace has been
    // deleted and it's the client responsibility to re-register if it
    // wish to do so
    REMOVED = 22;
  }
  Status status = 1;
  uint64 namespace_id = 2;
  uint32 version = 3;
  // The uid of the elements need be the same of the request
  // to allow reuse the serialization cache
  repeated Element elements = 4;

  // the id assigned to the watcher.
  uint32 uid = 10;
}
```

### Trace

To know if some service is using some document it's possible to add traces that will be activated
when certain conditions are met.

```protobuf
// The trace request parameters can be seen as a conditional like
//   (overrides is empty or overrides is a subset of A)
//   and (flavors is empty or flavors is a subset of B)
//   and (document is empty or document == C)
// where
//   A is a override got/watched
//   B is a flavor got/watched
//   C is a document got/watched
message TraceRequest {
  // the root path of the namespace where trace the configuration.
  string root_path = 1;
  // the list of overrides to trace, if some document with one of them
  // is asked it will be returned
  repeated string overrides = 2;
  // the list of flavors to trace, if some document with one of them
  // is asked it will be returned
  repeated string flavors = 3;
  // the document to trace, in case it is not defined it will not be used
  // in the query
  string document = 4;
}

message TraceResponse {
  enum Status {
    RETURNED_ELEMENTS = 0;
    ERROR = 1;
    ADDED_WATCHER = 2;
    EXISTING_WATCHER = 3;
    REMOVED_WATCHER = 4;
  }
  Status status = 1;
  uint64 namespace_id = 2;
  uint32 version = 3;
  repeated string overrides = 4;
  repeated string flavors = 5;
  string document = 6;
  // The client ip/socket/etc used to connect to the server
  string peer = 8;
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

## Metrics

The server expose some metrics through a prometheus client, some of this metrics are:

* The quantile 0.5, 0.9 and 0.99 of the requests duration (the stats are sampled).
* The quantile 0.5, 0.9 and 0.99 of the internal threads (the stats are sampled).
* Some internal stats.

## Credits

Created and maintained by [@Gonlo2](https://github.com/Gonlo2/).

## Third party libraries

* abseil: https://abseil.io/
* fmt: https://github.com/fmtlib/fmt
* grpc: https://grpc.io/
* inja: https://github.com/pantor/inja
* json: https://github.com/nlohmann/json
* prometheus-cpp: https://github.com/jupp0r/prometheus-cpp
* spdlog: https://github.com/gabime/spdlog
* yaml-cpp: https://github.com/jbeder/yaml-cpp

## FAQ

### What differentiates the overrides from the flavors?

What differentiates these two concepts are the owners of them, the owners of the overrides are the infrastructure teams while the owners of the flavors are the developers.

## License

This project is licensed under the AGPL-3.0 - see the [LICENSE.md](LICENSE.md) file for details

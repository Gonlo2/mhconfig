![](/assets/logo.readme.png?raw=true "MHConfig logo")

# Introduction

MHConfig is a Multi Hierarchy Configuration server to obtain reproducible and dynamic configuration in a easy way.

The idea of this service is to solve a series of problems:

- The coupling of the code with the configuration, which prevents an agile deployment either to test experimental functionalities, to modify the existing functionality or to deprecate a functionality.
- Configuration management in the case of multiple environments (e.g. development, pre-production and production), data centers or services
- Allow to detect configuration changes and to reload them hot without losing the possibility to have a consistent view (two requests have to return the same configuration even if it has been changed if necessary).
- Avoid the high computational cost of having this logic in a separate library for each of the services.

## Documentation

Documentation can be found on the [website](https://gonlo2.github.io/mhconfig/en/doc/).

## Credits

Created and maintained by [@Gonlo2](https://github.com/Gonlo2/).

## Third party libraries

* abseil: https://abseil.io/
* fmt: https://github.com/fmtlib/fmt
* grpc: https://grpc.io/
* prometheus-cpp: https://github.com/jupp0r/prometheus-cpp
* spdlog: https://github.com/gabime/spdlog
* yaml-cpp: https://github.com/jbeder/yaml-cpp

## License

This project is licensed under the AGPL-3.0 - see the [LICENSE.md](LICENSE.md) file for details

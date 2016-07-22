# dmclock

This repository contains C++ 11 code that implements the dmclock
distributed quality of service algorithm. See __mClock: Hanling
Throughput Variability for Hypervisor IO Scheduling__ by Gulati,
Merchant, and Varman for a description of the algorithm.

## Running cmake

When running cmake, set the build type with either:

    -DCMAKE_BUILD_TYPE=Debug
    -DCMAKE_BUILD_TYPE=Release

To turn on profiling, run cmake with an additional:

    -DPROFILE=yes
    
To compare current heap implementation with a simple heap (that uses linear search),
run cmake with an additional:

    -DUSE_SIMPLE_HEAP=yes

## Running make

### Building the dmclock library

The `make` command builds a library libdmclock.a. That plus the header
files in the src directory allow one to include the algorithm in their
code.

### Building unit tests

The `make dmclock-tests` command builds unit tests.

### Building simulations

The `make dmclock-sims` command builds two simulations -- *dmc_sim*
and *ssched_sim* -- which incorporate, respectively, the dmclock
priority queue or a very simple scheduler for comparison. Other
priority queue implementations could be added in the future.

## Using dmclock

Use `./dmc_sim -c config_file` to load parameters from 'config_file' while
running dmclock simulator. 
# TPCC-Benchmark For Blitzcrank

## Clone Instruction
To clone this project successfully, please install [git-lfs](https://git-lfs.com/). We use it to manage large dataset files.

## How to Compile:

We use CMake to build the project. To build the project, run the following commands:

```shell
cd ./build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```

## How to Run:

To run the benchmark, run the following commands:

` ./tpcc [number of warehouse] [allowed memory size (GB)] [running time (min)] [data generation mode]`

    [number warehouse]: The number of warehouses to load.

    [allowed memory size (GB)]: The maximum memory size allowed for the database.

    [running time (min)]: The running time of the benchmark.

    [data generation mode]: The mode of data generation. 0 for benchmark, 1 for data generation.

### Benchmark Example:

```shell
./tpcc 10 1 1 0 # Run the benchmark with 10 warehouses, 1GB memory, 1 minute running time.
```

Output:
```
Memory size: 1 GB
Random file id: 863194263
Loading 10 warehouses...        Loading Data Time: 3515
Transforming 10 warehouses... Learning Data Time: 4452 ms
Running...
[Executed Txns] [Throughput]    [Table Mem Size]        [Table Disk Size]       [Model Size]    [B+Tree Size]
10000   37898.167624    114632789       0       1364149 179511312
20000   37379.684142    115329584       0       1364149 181213000
30000   36082.976413    116030637       0       1364149 182915200
40000   39079.597324    116726852       0       1364149 184614128
50000   34898.167148    117434604       0       1364149 186361792
60000   35789.571635    118149065       0       1364149 188130640
70000   37316.217628    118853019       0       1364149 189884456
80000   38956.131500    119567036       0       1364149 191647728
90000   34920.834468    120269037       0       1364149 193372856
100000  35010.695768    120964427       0       1364149 195063984
110000  38626.891269    121678419       0       1364149 196800880
```


### Data Generation Example:

```shell
./tpcc 10 1 1 1 # Generate data with 10 warehouses.
```
The generated data will be stored in the same directory.


## Overview of the code:

tpccgenerate.cc: Code for loading the database.

tpccclient.cc: Code for generating client requests.

tpccdb.h: Definitions of the tuple types and constant

tpcctables.cc: Actual implementation of the in-memory data structures and TPC-C stored procedures.

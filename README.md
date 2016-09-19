# Spark-IO

Spark-IO contains various I/O accleration plugins for Spark tailored to high-performance network and storage hardware (RDMA, NVMef, etc.). Spark-IO is based on Crail, a fast multi-tiered distributed storage system. Spark-IO currently contains two IO plugins: a shuffle engine and a broadcast module. Both plugins inherit all the benefits of Crail such as very high performance (throughput and latency) and multi-tiering (e.g., DRAM and flash).

## Requirements

* Spark 2.0.0
* Java 8
* RDMA-based network, e.g., Infiniband, iWARP, RoCE. There are two options to run Spark-IO without RDMA networking hardware: (a) use SoftiWARP, (b) us the TCP/DRAM storage tier

## Building 

Building the source requires [Apache Maven](http://maven.apache.org/) and Java version 8 or higher.
To build Crail execute the following steps:

1. Obtain a copy of [Spark-IO](https://github.com/zrlio/spark-io) from Github
2. Make sure your local maven repo contains [Crail](https://github.com/zrlio/crail), if not build Crail from Github
4. Run: mvn -DskipTests install
5. Copy spark-io-1.0.jar as well as its dependencies to the Spark jars folder

```
    cd spark-io
    cp target/spark-io-1.0-dist/jars $SPARK_HOME/jars/
```
## Configuration

To configure the crail shuffle plugin included in spark-io add the following line to spark-defaults.conf
```
    spark.shuffle.manager		org.apache.spark.shuffle.crail.CrailShuffleManager
```
Since spark version 2.0.0, broadcast is no longer an exchangeable plugin, unfortunately. To use the crail broadcast plugin in Spark it has to be manually added to Spark's BroadcastManager.scala.

## Running

For the Crail shuffler to perform best, applications are encouraged to provide an implementation of the `CrailShuffleSerializer` interface, as well as an implementation of the `CrailShuffleSorter` interface. Defining its own custom serializer and sorter for the shuffle phase not only allows the application to serialize and sort faster, but allows applications to directly leverage the functionality provided by the Crail input/output streams such as zero-copy or asynchronous operations. Custom serializer and sorter can be specified in spark-defaults.xml. For instance, [crail-terasort](https://github.com/zrlio/crail-terasort) defines the shuffle serializer and sorter as follows:

```
    spark.crail.shuffle.sorter     com.ibm.crail.terasort.sorter.CrailShuffleNativeRadixSorter
    spark.crail.shuffle.serializer com.ibm.crail.terasort.serializer.F22Serializer
```

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com



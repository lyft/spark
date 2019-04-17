# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, Python, and R, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and DataFrames,
MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the [project web page](http://spark.apache.org/documentation.html).
This README file only contains basic setup instructions.

## Building Spark

Spark is built using [Apache Maven](http://maven.apache.org/).
To build Spark and its example programs, run:

    build/mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)

You can build Spark using more than one thread by using the -T option with Maven, see ["Parallel builds in Maven 3"](https://cwiki.apache.org/confluence/display/MAVEN/Parallel+builds+in+Maven+3).
More detailed documentation is available from the project site, at
["Building Spark"](http://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](http://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

The easiest way to start using Spark is through the Scala shell:

    ./bin/spark-shell

Try the following command, which should return 1000:

    scala> sc.parallelize(1 to 1000).count()

## Interactive Python Shell

Alternatively, if you prefer Python, you can use the Python shell:

    ./bin/pyspark

And run the following command, which should also return 1000:

    >>> sc.parallelize(range(1000)).count()

## Example Programs

Spark also comes with several sample programs in the `examples` directory.
To run one of them, use `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

You can set the MASTER environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" to run locally with N threads. You
can also use an abbreviated class name if the class is in the `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

Many of the example programs print usage help if no params are given.

## Running Tests

Testing first requires [building Spark](#building-spark). Once Spark is built, tests
can be run using:

    ./dev/run-tests

Please see the guidance on how to
[run tests for a module, or individual tests](http://spark.apache.org/developer-tools.html#individual-tests).

There is also a Kubernetes integration test, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the protocols have changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.

Please refer to the build documentation at
["Specifying the Hadoop Version and Enabling YARN"](http://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
for detailed guidance on building for a particular distribution of Hadoop, including
building for particular Hive and Hive Thriftserver distributions.

## Configuration

Please refer to the [Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)
in the online documentation for an overview on how to configure Spark.

## Contributing

Please review the [Contribution to Spark guide](http://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.

# Spark SQL Adaptive Execution
There are three main features in Adaptive Execution, including auto setting the shuffle partition number, optimizing join strategy at runtime and handling skewed join. These features can be enabled separately. To start with Adaptive Exection on Spark 2.3, please build branch `ae-2.3-08` and at least set `spark.sql.adaptive.enabled` to true. For users who enabled external shuffle service, please also upgrade external shuffle service to use adaptive execution feature.
An Engilish version design doc is available on [google doc](https://docs.google.com/document/d/1mpVjvQZRAkD-Ggy6
## Auto Setting The Shuffle Partition Number
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.enabled</code></td>
  <td>false</td>
  <td>
    When true, enable adaptive query execution.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.minNumPostShufflePartitions</code></td>
  <td>1</td>
  <td>
    The minimum number of post-shuffle partitions used in adaptive execution. This can be used to control the mi
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.maxNumPostShufflePartitions</code></td>
  <td>500</td>
  <td>
    The maximum number of post-shuffle partitions used in adaptive execution. This is also used as the initial s
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.shuffle.targetPostShuffleInputSize</code></td>
  <td>67108864</td>
  <td>
    The target post-shuffle input size in bytes of a task. By default is 64 MB.
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.shuffle.targetPostShuffleRowCount</code></td>
  <td>20000000</td>
  <td>
    The target post-shuffle row count of a task. This only takes effect if row count information is collected.
  </td>
</tr>
</table>

## Optimizing Join Strategy at Runtime
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.join.enabled</code></td>
  <td>true</td>
  <td>
    When true and <code>spark.sql.adaptive.enabled</code> is enabled, a better join strategy is determined at ru
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptiveBroadcastJoinThreshold</code></td>
  <td>equals to <code>spark.sql.autoBroadcastJoinThreshold</code></td>
  <td>
    Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing 
  </td>
</tr>
</table>

## Handling Skewed Join
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.sql.adaptive.skewedJoin.enabled</code></td>
  <td>false</td>
  <td>
    When true and <code>spark.sql.adaptive.enabled</code> is enabled, a skewed join is automatically handled at 
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionFactor</code></td>
  <td>10</code></td>
  <td>
    A partition is considered as a skewed partition if its size is larger than this factor multiple the median p
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionSizeThreshold</code></td>
  <td>67108864</td>
  <td>
    Configures the minimum size in bytes for a partition that is considered as a skewed partition in adaptive sk
  </td>
</tr>
<tr>
  <td><code>spark.sql.adaptive.skewedPartitionRowCountThreshold</code></td>
  <td>10000000</td>
  <td>
    Configures the minimum row count for a partition that is considered as a skewed partition in adaptive skewed
  </td>
</tr>
<tr>
  <td><code>spark.shuffle.statistics.verbose</code></td>
  <td>false</td>
  <td>
    Collect shuffle statistics in verbose mode, including row counts etc. This is required for handling skewed j
  </td>
</tr>
</table>
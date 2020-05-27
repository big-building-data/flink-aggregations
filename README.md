# Flink Aggregations Application

This repository implements the aggregations processor for BBData measures. It implements a custom windowing system for aggregations with a granularity of several minutes to several hours.

## Quick start

### Requirements

This application requires Flink 1.2+. It is intended to run in a BBData environment. The source kafka topic should
contain augmented measures and the cassandra database should have a table `bbdata2.aggregations` with the following
structure (see `aggregations.cql`):

    CREATE TABLE bbdata2.aggregations (
        minutes int,
        object_id int,
        date text,   -- form: MM-dd
        timestamp timestamp,

        last float,
        last_ts bigint,
        min float,
        max float,
        sum float,
        mean float,
        count int,
        -- std dev: keep intermediary data for late arrivals
        k float,
        k_sum float,
        k_sum_2 float,
        std float,

        PRIMARY KEY ((minutes, object_id, date), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);


### Build and configuration

clone this repository and create the jar:

    git git@github.com:big-building-data/flink-aggregations.git
    cd flink-aggregations
    mvn package

Create a `config.properties` file based on the template available in `src/main/resources/config.properties-template` and
modify the values accordingly. If you intend to run this application with multiple granularites, ensure you create
different properties files for each, changing AT LEAST the `window.granularity` and the `kafka.consumer.group` properties.


### Running the application (yarn)

**IMPORTANT**: you need to build the project with the **prod Maven profile** enabled:

    mvn package -P prod

To run the application on yarn, you have two options:

1. launch a new flink session and run the application(s) in the session
2. launch the application(s) in standalone mode


#### Running inside a flink session

Launch a new flink session:

    flink/bin/yarn-session.sh -d -n 4

Launch the application:

    flink run flink-aggregations-*-full.jar config.properties


#### Running in standalone mode

    flink run -d --yarnstreaming --jobmanager yarn-cluster -yn 1 -j flink-aggregations-*-full.jar \
        -c ch.derlin.bbdata.flink.Main config.properties


# Why a custom windowing system ?

In BBData, many sources/objects produce measures. Each source might have its own timeline and rhythm. For example,
object 1 might produce data every 5 seconds, while object 2 might produce data every 1 hour. Object clocks
are not synchronized, so their timestamps might be different even though the measures are produced exactly at the same
time. One requirement is that timestamps in measures for the same source are monotically increasing. But in case of a
failure, a source might stop sending values, then resume and send two hours worth of measures at the same time. In this
case, timestamps are still increasing monotically for that source, but timestamps will be "late" compared to the
other source's timelines.


With the current Flink windowing implementation, there is only one time advance, shared by all the streams. It is possible
to configure an "allowed lateness", but this is not enough to deal with the case of a source failure. We really need
 each source to have its own independent time advance !

 Another possibility would be to use Kafka-streams instead of Flink.

 Kafka-streams does not use watermarks to keep track of time (and detect when a window should be closed). Instead, it
 stores aggregation results in an ever-updating `KTable`. A `KTable` is table represented as a stream of row updates;
 in other words, a changelog stream. Kafka will fire a new event every time a window is updated.

 The each-element triggering can be a problem if the window function is expensive to compute and
 doesn’t have a straightforward "accumulative" nature. In our case, we need to save the aggregation results, so this
 means we will do a database update on each new record, which is rather heavy...

Kafka-streams is aware of this limitation and under [KIP-63](https://cwiki.apache.org/confluence/display/KAFKA/KIP-63%3A+Unify+store+and+downstream+caching+in+streams),
window output will be cached hence triggering not on each element, but only when the cache is full.
This should improve performance of window processing. But the cache might grow quickly, so we might be able to limit
database update to every five minutes or so, which is still often for windows of one hour or more.

# How we proceed in our custom implementation

This repository uses Flink states and rich map functions to implement its own sliding window mecanism.
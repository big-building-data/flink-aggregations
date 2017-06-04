# Flink Aggregations Application

This repository implements the aggregations processor for BBData measures. It implements a custom windowing system for aggregations with a granularity of several minutes to several hours.

## Quick start

### requirements

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


### build and configuration

clone this repository and create the jar:

    git clone git@gitlab.forge.hefr.ch:bbdata/flink-aggregations.git
    cd flink-aggregations
    mvn package

Copy the jar to daplab-app-1 (if not already on the daplab) :

    scp target/flink-aggregations-*-full.jar daplab-app-1.fri.lan:/opt/bbdata/flink

Create a `config.properties` file based on the template available in `src/main/resources/config.properties-template` and
modify the values accordingly. If you intend to run this application with multiple granularites, ensure you create
different properties files for each, changing the `window.granularity` and the `kafka.consumer.group` properties.


### running the application (yarn)

To run the application, you have two options:

1. launch a new flink session and run the application(s) in the session
2. launch the application(s) in standalone mode


#### Running inside a flink session

Launch a new flink session:

    flink/bin/yarn-session.sh -d -n 4 -jm 1024 -tm 4096

Launch the application:

    flink run flink-aggregations-*-full.jar config.properties


#### Running in standalone mode

    flink run -d --yarnstreaming --jobmanager yarn-cluster -yn 1 -j flink-aggregations-*-full.jar \
        -c ch.derlin.bbdata.flink.Main  config.properties


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
 each source to have its own independent time advance.




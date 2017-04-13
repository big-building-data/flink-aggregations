TODO:

- ConfigOptions
- doublons dans le flux kafka...



1. launch flink-session:

    flink-1.2.0/bin/yarn-session.sh -d -n 4 -jm 1024 -tm 4096

2. use the flink run utility as always:

    flink-1.2.p/bin/flink run -d --yarnstreaming --jobmanager yarn-cluster -yn 1 -j flink-aggregations-0.0.1-full.jar -c ch.derlin.bbdata.flink.Main  local.properties

    flink-1.2.p/bin/flink list

    flink-1.2.p/bin/flink cancel <app id>
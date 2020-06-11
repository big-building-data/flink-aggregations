#!/bin/bash

#####
# ----------------------------------------
# DAPLAB flink-aggregation launcher script
# ----------------------------------------
#
# Helper script to launch a yarn-session and the flink-aggregations application.
# This script assumes the following:
#  - flink-1.10.1 located in /opt/flink
#  - config-quarters.properties and config-hours.properties present in the same folder
#  - jar built for hadoop located in target/
#
# How to use:
#  - run with arguments "start session" to spawn a new flink session on yarn,
#  - run with arguments "start hours|quarters [savepoint]" to launch the aggregations,
#  - run with arguments "session-id" to get the current application_id of the session.
#
# Yarn and flink tips:
#
# To kill a yarn-session:
#    yarn application -kill <application_id>
#
# To cancel a running flink job:
#     flink list
#     flink cancel [-s <savepoint directoy>] <app_id>
#
# To resume the job from a savepoint:
#     flink run -s <savepoint directory>
####

CURRENT_DIR="$(dirname "$(readlink -f "$0/..")")"

## flink location
flink_base="/opt/flink/flink-1.10.1" # TODO: update if needed
export PATH="$flink_base/bin:$PATH"

## common environment variables needed by flink
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
export HADOOP_CLASSPATH=$(hadoop classpath)
export LD_LIBRARY_PATH=/usr/hdp/current/hadoop-client/lib/native:$LD_LIBRARY_PATH # TODO: change if not on Confluent

## current directory and usual jar location
JAR="$CURRENT_DIR/target/flink-aggregations-*-hadoop.jar" # TODO: update if needed

## session and job parameters
# session arguments: detached mode (-d), 1 taskmanager (-n), 4 slots (-s),
#   1024M for the jobmanager memory (-jm), 2048M for the taskmanager memory (-tm)
#OLD YARN_SESSION_ARGUMENTS="-d -n 1 -s 4 -jm 1024 -tm 2048 --name flink-agg-session"
YARN_SESSION_ARGUMENTS="-d -n 1 -s 2 -nm flink-agg-session"
# flink job arguments
FLINK_RUN_ARGUMENTS="-d"

# display the application_id of the yarn-session, if any
function get_session_id() {
    yarn application -list 2>&1 | grep "Flink session" | awk '{print $1}'
}

# list currently running flink jobs
function list_jobs() {
    flink list 2>&1 | grep -E '[0-9a-f]{32}'
}

# if a yarn-session is already running, display its application_id.
# if no session is running, launch one and print its application_id and web interface.
function start_yarn_session() {
    echo "checking for a running yarn session..."

    session_id=$(get_session_id)
    if [ -z "$session_id" ]; then
        # launch new yarn session
        echo "No yarn session found. Starting a new one..."
        out="$(yarn-session.sh $YARN_SESSION_ARGUMENTS 2>&1)"
        [ $? -ne 0 ] && echo -e "failed to start yarn session:\n $out" && exit 1
        # print information
        echo "yarn session started:"
        echo "   $(echo "$out" | grep -o "JobManager Web Interface.*")"
        echo "   $(echo "$out" | grep -o "yarn application -kil.*" | uniq)"
    else
        echo "yarn-session already running: $session_id"
    fi
}

# launch a flink job.
# this function assumes a yarn-session is already running.
# arguments: <jar> <app-config.properties> savepoint-directory to resume from (optional)
function launch_job() {
    # get args
    jar="$1"
    config="$2"
    savepoint="$3"

    # check args
    [ -z "$jar" ] && echo "missing <jar> to launch_job. Usage: launch_job <jar> <config> [<savepoint>]" && exit 1
    [ -z "$config" ] && echo "missing <config> to launch_job. Usage: launch_job <jar> <config> [<savepoint>]" && exit 1

    # append hdfs to savepoint, if any
    if [ -n "$savepoint" ]; then
        savepoint="-s hdfs://$savepoint"
        echo "Using savepoint: '$savepoint'"
    fi

    # launch
    args="$(eval echo $FLINK_RUN_ARGUMENTS -j $jar $savepoint $config)" # expand all variables such as "~" and "*"
    echo "launching job..."
    out="$(flink run $args 2>&1)"

    # check launch status
    [ $? -ne 0 ] && echo -e "failed to launch flink job:\n$out" && exit 1
    echo "flink job started"
    echo "   ApplicationId: $(echo $out | grep -o "JobID [^ ]*")"
}

case "$1" in
start)
    case "$2" in
    s | session)
        start_yarn_session
        ;;
    hours)
        launch_job "$JAR" "$CURRENT_DIR/config-hours.properties" "$3"
        ;;
    quarters)
        launch_job "$JAR" "$CURRENT_DIR/config-quarters.properties" "$3"
        ;;
    j | job)
        launch_job "$3" "$4" "$5"
        ;;
    *)
        echo "wrong parameter '$3'"
        echo "Usage: start <session|hours|quarters|job jar config [savepoint]>"
        exit 1
        ;;
    esac
    ;;

session-id)
    get_session_id
    ;;
list)
    list_jobs
    ;;
*)
    echo "usage:"
    echo " start <session|hours|quarters>"
    echo " start job <jar> <config> [<savepoint>]"
    echo " session-id"
    echo " list"
    ;;
esac

#!/usr/bin/env bash 

##
# DAPLAB flink-aggregation launcher script
# ----------------------------------------
#
# Helper script to launch a yarn-session and the flink-aggregations application.
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
##

# Parameters: detached mode (-d), 1 taskmanager (-n), 4 slots (-s),
#   1024M for the jobmanager memory (-jm), 2048M for the taskmanager memory (-tm)
YARN_SESSION_ARGUMENTS="-d -n 1 -s 4 -jm 1024 -tm 2048"
# TODO: update the path to the jar and properties file if needed
FLINK_RUN_ARGUMENTS="-d"

CURRENT_DIR="$(dirname "$(readlink -f "$0/..")")"

# job jars
FLINK_BASIC="$CURRENT_DIR/flink-basic-processing/target/flink-*.jar $CURRENT_DIR/properties/flink-basic.properties"

JOB_AUGMENT="-c ch.derlin.bbdata.augmentation.Augmentation $FLINK_BASIC"
JOB_CASSANDRA="-c ch.derlin.bbdata.cassandra.CassandraSaver $FLINK_BASIC"
JOB_AGGR="$CURRENT_DIR/flink-aggregations/target/flink-aggregations-*-full.jar $CURRENT_DIR/properties/flink-aggregations.properties"


# flink needs access to the hadoop configuration
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
# use a custom log4j.properties file (currently in /home/daplabadm/flink-agg)
export FLINK_CONF_DIR=$CURRENT_DIR/conf


# display the application_id of the yarn-session, if any
function get_session_id(){
    yarn application -list 2>&1 | grep "Flink session" | awk '{print $1}'
}

# if a yarn-session is already running, display its application_id.
# if no session is running, launch one and print its application_id and web interface.
function start_yarn_session(){
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

# launch the flink-aggregation program.
# this function assumes a yarn-session is already running.
# argument: savepoint-directory to resume from (optional)
function launch_job(){
    [ -z "$1" ] && echo "missing argument to launch_job. Usage: launch_job <jar> [<savepoint>]" && exit 1

    args="$(eval echo $FLINK_RUN_ARGUMENTS $1)" # expand all variables such as "~" and "*"

    if [ -n "$2" ]; then
        echo "resuming from savepoint $2..."
        out="$(flink run -s "$2" $args 2>&1)"
        #echo "flink run -s "$2" $args 2>&1"
    else
        echo "launching job..."
        out="$(flink run $args 2>&1)"
        #echo "flink run $args 2>&1"
    fi
    [ $? -ne 0 ] && echo -e "failed to launch flink job:\n$out" && exit 1
    echo "flink job started"
    echo "   ApplicationId: $(echo $out | grep -o "JobID [^ ]*")"
}

case "$1" in
    start-all) 
        start_yarn_session
        launch_job "$JOB_AUGMENT" "$2"
        launch_job "$JOB_CASSANDRA" "$2"
        launch_job "$JOB_AGGR" "$2"
        ;;

    start)
        case "$2" in 
            s|session)
                start_yarn_session
                ;;
            au|aug*)
                launch_job "$JOB_AUGMENT" "$3"
                ;;
            c|cass*)
                launch_job "$JOB_CASSANDRA" "$3"
                ;;
            ag|aggr*)
                launch_job "$JOB_AGGR" "$3"
                ;;
            *) 
                echo "wrong parameter '$3'"
                echo "Usage: start session|augmentation|cassandra|aggregation"
                exit 1
        esac
        ;;

    session-id)
        get_session_id 
        ;;
    *) 
        echo "usage: start-job|start-session|start-all|session-id"
esac


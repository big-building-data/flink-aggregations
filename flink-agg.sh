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
# Currently in /home/daplabadm/flink-agg
FLINK_RUN_ARGUMENTS="-d ~/flink-agg/flink-aggregations-*-full.jar ~/flink-agg/local.properties"


CURRENT_DIR="$(dirname "$(readlink -f "$0")")"

# use flink 1.2
export PATH=/home/daplabadm/flink-1.2.0/bin:$PATH
# flink needs access to the hadoop configuration
export HADOOP_CONF_DIR=/etc/hadoop/conf
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
    args="$(eval echo $FLINK_RUN_ARGUMENTS)"  # expand all variables such as "~" and "*"
    if [ -n "$1" ]; then
        echo "resuming from savepoint $1..."
        out="$(flink run -s "$1" $args 2>&1)"
    else
        echo "launching job..."
        out="$(flink run $args 2>&1)"
    fi
    [ $? -ne 0 ] && echo -e "failed to launch flink job:\n$out" && exit 1
    echo "flink job started"
    echo "   ApplicationId: $(echo $out | grep -o "JobID [^ ]*")"
}


case "$1" in
    start-all) 
        start_yarn_session
        launch_job "$2"
        ;;
    start-job)
        launch_job "$2"
        ;;
    start-session)
        start_yarn_session
        ;;
    session-id)
        get_session_id 
        ;;
    *) 
        echo "usage: start-job|start-session|start-all|session-id"
esac

#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

USAGE="Usage: aloha-daemon.sh [--config <conf-dir>] (start|stop|status) (master|worker) <args...>"

if [[ $# -le 1 ]]; then
    echo ${USAGE}
    exit 1
fi

if [[ -z "${ALOHA_HOME}" ]]; then
  export ALOHA_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. ${ALOHA_HOME}/sbin/aloha-config.sh


if [[ "$1" == "--config" ]]; then
    shift
    conf_dir="$1"
    if [[ ! -d ${conf_dir} ]]; then
        echo "Error: $conf_dir is not a directory"
    else
        export ALOHA_CONF_DIR="$conf_dir"
    fi
    shift
fi

OPTION=$1
shift
DAEMON=$1
shift
ARGS=("${@:1}")
shift

case ${DAEMON}  in
  (master)
    CLASS_TO_RUN="me.jrwang.aloha.scheduler.master.Master"
  ;;
  (worker)
    CLASS_TO_RUN="me.jrwang.aloha.scheduler.worker.Worker"
  ;;
  (*)
    echo "Unknown daemon '${DAEMON}'. $USAGE"
    exit 1
  ;;
esac

aloha_rotate_log ()
{
    log=$1;
    num=5;
    if [[ -n "$2" ]]; then
        num=$2
    fi
    if [[ -f "$log" ]]; then # rotate logs
        while [[ ${num} -gt 1 ]]; do
            prev=`expr ${num} - 1`
            [[ -f "$log.$prev" ]] && mv "$log.$prev" "$log.$num"
            num=${prev}
        done
        mv "$log" "$log.$num";
    fi
}

. "$ALOHA_HOME/bin/load-aloha-env.sh"

if [[ "$ALOHA_IDENT_STRING" = "" ]]; then
    export ALOHA_IDENT_STRING="$USER"
fi

#get log directory
if [[ "$ALOHA_LOG_DIR" = "" ]]; then
    export ALOHA_LOG_DIR="$ALOHA_HOME/logs"
fi
mkdir -p "$ALOHA_LOG_DIR"
touch "$ALOHA_LOG_DIR"/.aloha_test > /dev/null 2>&1
TEST_LOG_DIR=$?
if [[ ${TEST_LOG_DIR} -eq 0 ]]; then
    rm -f "$ALOHA_LOG_DIR"/.aloha_test
else
    echo "privilege error! $ALOHA_LOG_DIR"
fi

if [[ "$ALOHA_PID_DIR" = "" ]]; then
    ALOHA_PID_DIR=/tmp
fi

pid="$ALOHA_PID_DIR/aloha-$ALOHA_IDENT_STRING-$DAEMON.pid"

#log settings
ALOHA_LOG_PREFIX="$ALOHA_LOG_DIR/aloha-$ALOHA_IDENT_STRING-$DAEMON-$HOSTNAME"
log="${ALOHA_LOG_PREFIX}.log"
out="${ALOHA_LOG_PREFIX}.out"

log_settings=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${ALOHA_CONF_DIR}/log4j.properties")

#Set default scheduling priority
if [[ "$ALOHA_NICENESS"="" ]]; then
    export ALOHA_NICENESS=0
fi

run_command() {
  mode="$1"
  shift

  mkdir -p "$ALOHA_PID_DIR"

  if [[ -f "$pid" ]]; then
    TARGET_ID="$(cat "$pid")"
    if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
      echo "$DAEMON running as process $TARGET_ID. Stop it first."
      exit 1
    fi
  fi

  if [[ "$ALOHA_MASTER" != "" ]]; then
    echo rsync from "$ALOHA_MASTER"
    rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' "$ALOHA_MASTER/" "$ALOHA_HOME"
  fi

  aloha_rotate_log "$out"
  echo "starting $DAEMON, logging to $out"

  #nice -n :set the priority of the process,default is zero
  case "$mode" in
    (class)
      nohup nice -n "$ALOHA_NICENESS" "$ALOHA_HOME"/bin/aloha-class "${log_settings}" "$@" >> "$out" 2>&1 < /dev/null &
      newpid="$!"
      ;;

    (*)
      echo "unknown mode: $mode"
      exit 1
      ;;
  esac

   sleep 2
  # Check if the process has died; in that case we'll tail the log so the user can see
  if [[ ! $(ps -p "$newpid" -o comm=) =~ "java" ]]; then
    echo "failed to launch $DAEMON:"
    tail -2 "$log" | sed 's/^/  /'
    echo "full log in $out"
  else
    echo "$newpid" > "$pid"
  fi
}

case ${OPTION} in
  (start)
    run_command class ${CLASS_TO_RUN} "${ARGS[@]}"
    ;;

  (stop)
    if [[ -f ${pid} ]]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo "stopping $DAEMON"
        kill "$TARGET_ID" && rm -f "$pid"
      else
        echo "no $DAEMON to stop"
      fi
    else
      echo "no $DAEMON to stop"
    fi
    ;;

  (status)

    if [[ -f ${pid} ]]; then
      TARGET_ID="$(cat "$pid")"
      if [[ $(ps -p "$TARGET_ID" -o comm=) =~ "java" ]]; then
        echo ${DAEMON} is running.
        exit 0
      else
        echo ${pid} file is present but ${DAEMON} not running
        exit 1
      fi
    else
      echo ${DAEMON} not running.
      exit 2
    fi

    ;;

  (*)
    echo ${usage}
    exit 1
    ;;

esac


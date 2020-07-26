#!/usr/bin/env bash

base=$(
  cd $(dirname $0)
  pwd
)

function my_include() {
  local f=${1:-"$HOME/.bash_profile"}
  test -f $f && . $f
}

function get_abs_path() {
  local f=$1
  local dir=$(
    cd $(dirname $f)
    pwd
  )
  echo "$dir/$(basename $f)"
}

function get_param() {
  local var=$1
  local n=$2
  local d=$3

  test ! -f "$user_proper_file" && {
    echo "Properties $user_proper_file file not set" >&2
    exit 2
  }
  local v=$(grep "^$n=" $user_proper_file | head -1 | awk -F '=' '{s="";for(i=2;i<=NF;i++){if(s){s=s"="$i}else{s=$i}};print s}')
  test "x$v" == "x" && test "x$d" != "x" && v="$d"
  test "x$v" == "x" && {
    echo "$n not set" >&2
    exit 2
  }
  eval "$var=\"$v\""
}

function set_jars() {
  local jar=""
  test ! -d "$lib_path" && {
    echo "lib_path $lib_path not found." >&2
    exit 2
  }
  for jar in $(ls $lib_path); do
    if [[ "x$jar" != "x$(basename $main_jar)" && "x$jar" =~ x.*\.jar$ ]]; then
      jars=$lib_path/$jar,$jars
    fi
  done
  test "x$jars" != "x" && jars="--jars ${jars%,}"
}
function check_command() {
  local c=${1:-"yarn"}
  if ! which $c >/dev/null 2>&1; then
    echo "command $c not found." >&2
    exit 2
  fi
}

function check_run() {
  local flag=$1
  check_command yarn
  local appids=($(yarn application -list | awk -v app=$appname '{if($2==app){print $1}}'))
  test ${#appids[@]} -eq 0 && test "x$flag" == "xstop" && {
    echo "Spark app $appname already stop." >&2
    exit 2
  }
  if test ${#appids[@]} -ne 0; then
    local appid=${appids[0]}
    local id=""
    unset appids[0]
    for id in ${appids[@]}; do
      echo "yarn application -kill $id" >&2
      yarn application -kill $id
    done
    if [ "x$flag" == "xstop" ]; then
      echo "yarn application -kill $appid" >&2
      yarn application -kill $appid
      exit 0
    fi
    echo "Spark app $appname already running. $appid" >&2
    exit 2
  fi
}
function set_abs_lib() {
  local p=$1
  local lp=$2
  test $(echo $p | grep -E '\.properties$' | wc -l) -eq 0 && {
    echo "$p the file needs to be of type properties" >&2
    exit 2
  }
  local cap=$(
    cd $(dirname $p)
    pwd
  )
  test "x${!lp:0:1}" != "x/" && eval "$lp=$(get_abs_path $cap/${!lp})"
}

function set_conf_dir() {
  if [ -f "conf/$1" ]; then
    echo "conf/$1"
  else
    echo "$1"
  fi
}

function check_once() {
  local app_name=$1
  local pid=$$
  if test -f $app_name && kill -0 $(cat $app_name) >/dev/null 2>&1; then
    echo "run.sh already running. pid is $(cat $app_name)" >&2
    exit 2
  fi
  echo $pid >$app_name
}

function clear_once() {
  local app_name=$1
  local pid=$$
  while :; do
    if ! kill -0 $pid >/dev/null 2>&1; then
      rm -rf $app_name
    fi
    sleep 1
  done
}

function main() {

  local proper=${1:-"$(basename $base).properties"}
  shift
  proper=$(set_conf_dir $proper)
  test ! -f "$proper" && {
    echo "file $proper not found" >&2
    exit 2
  }
  proper=$(get_abs_path $proper)
  user_proper_file=$proper

  my_include
  my_include /etc/profile
  my_include /etc/bashrc
  my_include $HOME/.bashrc

  get_param "main" "spark.run.main"
  get_param "main_jar" "spark.run.main.jar"
  get_param "appname" "spark.app.name" "${main}.App"
  get_param "lib_path" "spark.run.lib.path" "lib/"

  set_abs_lib "$proper" "lib_path"
  main_jar=$lib_path/$main_jar
  test ! -f "$main_jar" && {
    echo "$main_jar file does not exist" >&2
    exit 2
  }

  local is_stop=""
  test "x$2" == "xstop" && is_stop="stop"
  check_run $is_stop
  set_jars

  local spark_submit=spark-submit
  local main_parameter="--name $appname --properties-file $proper $jars --class $main $main_jar"
  echo "spark-submit $main_parameter"

  test $(check_cmd "spark2-submit") -eq 1 && spark_submit=spark2-submit
  check_command spark-submit
  spark-submit $main_parameter "$@"
}

test $# -eq 0 && {
  echo -e "Usage Ex:\n\tbash $base/$0 kafka_2_hdfs.properties" >&2
  exit 2
}
main "$@"

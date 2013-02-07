#!/bin/bash

CLASSPATH="$HOME/Labo/lcm-over-hadoop/target/classes"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/net/sf/trove4j/trove4j/3.0.3/trove4j-3.0.3.jar"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"

PLCM="/Users/martin/Labo/plcm/algorithms/plcm/plcm"
jLCM="java -Xmx1024m -cp $CLASSPATH fr.liglab.lcm.Main"


function runner() {
  start=`date +%s`
  peak=0
  
  $* > /dev/null &
  
  while rss=`ps -o rss= -p$!`
  do
    if [[ $rss -gt $peak ]]; then
      peak=$rss
    fi
    sleep 0.1
  done
  
  end=`date +%s`
  duration=$(($end-$start))
  
  echo "$* : ${duration}s on ${peak}kb"
}

function bench() {
  echo "== $2 @ $1"
  runner $PLCM $1 -i $2
  runner $jLCM $2 $1
}

function hard() {
  echo "===== Starting hard benchmark :"
  bench 80 retail.dat
  bench 200000 accidents.dat
  bench 9000 kosarak.dat
  bench 2000 T40I10D100K.dat
  bench 8000 digg.dat
  bench 120000 lastfm.dat
  echo "===== HARD BENCHMARK DONE"
}

function easy() {
  echo "===== Starting easy benchmark :"
  bench 800 retail.dat
  bench 300000 accidents.dat
  bench 100000 kosarak.dat
  bench 20000 T40I10D100K.dat
  bench 80000 digg.dat
  bench 400000 lastfm.dat
  echo "===== EASY BENCHMARK DONE"
}

if [[ $# -eq 0 ]]; then
  echo "USAGE : bench.sh {easy|hard}" >&2
  exit 1
fi

while [[ $# -gt 0 ]]; do
  $1
  shift
done

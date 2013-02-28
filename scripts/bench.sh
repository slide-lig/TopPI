#!/bin/bash

###### TODO:
## inclure le LCM d'Uno et al dans le bench
## fix les supports partout
## passer le nom a afficher dans runner()


CLASSPATH="$HOME/Labo/lcm-over-hadoop/target/classes"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/net/sf/trove4j/trove4j/3.0.3/trove4j-3.0.3.jar"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"

PLCM="/Users/martin/Labo/plcm/algorithms/plcm/plcm"
jLCM="java -Xmx1024m -cp $CLASSPATH fr.liglab.lcm.Main"

function algoName() {
  commandline=`echo $1 | cut -f 1 -d ' '`
  basename $commandline
}

function runner() {
  start=`date +%s`
  peak=0
  
  $* &
  
  while rss=`ps -o rss= -p$!`
  do
    if [[ $rss -gt $peak ]]; then
      peak=$rss
    fi
    sleep 0.1
  done
  
  end=`date +%s`
  duration=$(($end-$start))
  name=`algoName $1`
  
  echo "$name : ${duration}s on ${peak}kb"
}

function bench() {
  echo "== $2 @ $1"
  
  runner $PLCM $1 -i $2 -o tmp_plcm_patterns_
  plcmCount=`wc -l tmp_plcm_patterns_* | tail -n 1 | grep -o -E '[0-9]+ '`
  rm tmp_plcm_patterns_*
  
  runner $jLCM $2 $1 tmp_jlcm_patterns.dat
  jlcmCount=`wc -l tmp_jlcm_patterns.dat | grep -o -E [0-9]+`
  rm tmp_jlcm_patterns.dat
  
  if [[ $plcmCount -ne $jlcmCount ]]; then
    echo "STAHP : PLCM found $plcmCount patterns and our LCM $jlcmCount"
    exit 1
  else
    echo "-- $jlcmCount patterns"
  fi
}

function hard() {
  echo "===== Starting hard benchmark :"
  bench 80 retail.dat
  bench 3000 kosarak.dat
  bench 10000 T40I10D100K.dat
  bench 8000 digg.dat
  bench 120000 lastfm.dat
  #bench 200000 accidents.dat
  echo "===== HARD BENCHMARK DONE"
}

function easy() {
  echo "===== Starting easy benchmark :"
  bench 800 retail.dat
  bench 300000 accidents.dat
  bench 100000 kosarak.dat
  bench 20000 T40I10D100K.dat
  #bench 80000 digg.dat
  #bench 400000 lastfm.dat
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

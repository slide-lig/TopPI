#!/bin/bash

TROVE="$HOME/.m2/repository/net/sf/trove4j/trove4j/3.0.3/trove4j-3.0.3.jar"
export HADOOP_CLASSPATH="$HOME/Labo/lcm-over-hadoop/target/classes:$TROVE"

hadoop fr.liglab.lcm.Main -libjars $TROVE $* 

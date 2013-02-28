#!/bin/bash

CLASSPATH="$HOME/Labo/lcm-over-hadoop/target/classes"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/net/sf/trove4j/trove4j/3.0.3/trove4j-3.0.3.jar"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar"

java -Xmx1024m -cp "$CLASSPATH" fr.liglab.lcm.Main $*

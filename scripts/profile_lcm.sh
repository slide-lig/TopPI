#!/bin/bash

JIP="$HOME/Labo/jip"
CLASSPATH="$HOME/Labo/lcm-over-hadoop/target/classes"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/net/sf/trove4j/trove4j/3.0.3/trove4j-3.0.3.jar"
CLASSPATH="$CLASSPATH:$HOME/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar"

java -javaagent:$JIP/profile/profile.jar -Dprofile.properties=profile_lcm.properties -Xmx1024m -cp "$CLASSPATH" fr.liglab.lcm.Main $*

java -jar "$JIP/tools/jipViewer.jar" profile_lcm.xml

#!/bin/bash

CWD=`pwd`

mkdir -p "out"

# Go to experiments folder
cd ..

EXP_DIR=`pwd`

# Go to project root
cd ../..

# Build whole project
mvn clean install -DskipTests

# Set path do data
export EXPERIMENTS_BASE_PATH="`pwd`/data"

# Go back to experiments directory
cd $EXP_DIR

# Build classpath
echo "Building classpath"
mvn -Dmdep.outputFile=load_classpath clean compile dependency:build-classpath > /dev/null 2>&1

echo "Using base-path: $EXPERIMENTS_BASE_PATH"

classes=(
"sigmod2021.pattern.experiments.CostModelCalibration"
"sigmod2021.pattern.experiments.SynthUniform"
"sigmod2021.pattern.experiments.SynthUniformSingleQuery"
"sigmod2021.pattern.experiments.SynthSelectionAlgorithmPerformance"
"sigmod2021.pattern.experiments.SynthSkewed"
"sigmod2021.pattern.experiments.ChicagoCrime"
"sigmod2021.pattern.experiments.LandingPattern"
)


# Run Experiments

for i in "${classes[@]}"; do
  echo "=================================================="
  echo "Running experiments: $i"
  JAVA_CMD="java -cp `cat load_classpath`./:target/classes"
  JAVA_CMD="$JAVA_CMD $i"
  $JAVA_CMD | tee -a $CWD/out/$i.out 2>> $CWD/out/$i.err
done

# Go back to scripts folder
cd $CWD

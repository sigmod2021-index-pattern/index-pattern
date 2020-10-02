# Index Accelerated Pattern Matching on Persistent Event Streams
Code and scripts for reproducing the results of the paper "Index Accelerated Pattern Matching on Persistent Event Streams"

## Building & Running the Experiments

We set up a script that compiles the whole project, creates the indexes, and executes all experiments found in the paper. The script is located at:
```pattern/pattern-experiments/scripts/sigmod_all.sh```
After completion, the results of the experiments are found in the `pattern/pattern-experiments/scripts/out` sub-folder.

## Prequisites
In order to run the experiments please make sure you have sufficient disk space for managing the indexes (~50 GiB). Moreover, make sure you are running Oracle JDK 14.0.1 or above and Maven (>= 3.0) is installed properly (i.e., the `mvn` command is in your path).

## Data sets
The data sets used during evaluation are found in `data/src`. To check out the files, you need to install the git-lfs extension (https://git-lfs.github.com/). Furthermore, before running the experiments, you need to unzip the checked out data files.




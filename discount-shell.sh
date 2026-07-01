#!/bin/bash

#Reliably obtain the directory where this script is located.
#Source: https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script/246128
get_script_dir()
{
    local SOURCE_PATH="${BASH_SOURCE[0]}"
    local SYMLINK_DIR
    local SCRIPT_DIR
    # Resolve symlinks recursively
    while [ -L "$SOURCE_PATH" ]; do
        # Get symlink directory
        SYMLINK_DIR="$( cd -P "$( dirname "$SOURCE_PATH" )" >/dev/null 2>&1 && pwd )"
        # Resolve symlink target (relative or absolute)
        SOURCE_PATH="$(readlink "$SOURCE_PATH")"
        # Check if candidate path is relative or absolute
        if [[ $SOURCE_PATH != /* ]]; then
            # Candidate path is relative, resolve to full path
            SOURCE_PATH=$SYMLINK_DIR/$SOURCE_PATH
        fi
    done
    # Get final script directory path from fully resolved source path
    SCRIPT_DIR="$(cd -P "$( dirname "$SOURCE_PATH" )" >/dev/null 2>&1 && pwd)"
    echo "$SCRIPT_DIR"
}


#SPARK_MASTER=spark://localhost:7077
SPARK_MASTER=${SPARK_MASTER:-local[*]}

# Find spark-submit script
if [ -z "$SPARK_HOME" ]; then
  SPARK_SHELL=$(which spark-shell || echo)
else
  SPARK_SHELL="$SPARK_HOME"/bin/spark-shell
fi
if [ -z "$SPARK_SHELL" ]; then
  echo "SPARK_HOME not set and spark-shell not on PATH; Aborting."
  exit 1
fi

TMPDIR=${TMPDIR:-/tmp}
DISCOUNT_TMP=${DISCOUNT_TMP:-$TMPDIR}
echo "Using ${DISCOUNT_TMP} for scratch data (set DISCOUNT_TMP to override)."

#Try to find the directory that this file is located in
DISCOUNT_HOME="$(get_script_dir)"

#For standalone mode (one process), it is helpful to provide as much memory as possible.
#This sets the default value to 16g if the variable is unassigned.
DISCOUNT_MEMORY=${DISCOUNT_MEMORY:-16g}
echo "Using ${DISCOUNT_MEMORY} as the memory setting (set DISCOUNT_MEMORY to override)."

MEMORY="spark.driver.memory=$DISCOUNT_MEMORY"

#Scratch space location. This has a big effect on performance; should ideally be a fast SSD or similar.
LOCAL_DIR="spark.local.dir=$DISCOUNT_TMP"

#On Windows: Change bin/spark-submit to bin/spark-submit.cmd.

exec $SPARK_SHELL \
  -I $DISCOUNT_HOME/shell/spark-shell.scala \
  --conf spark.driver.maxResultSize=2g \
  --driver-java-options -Dlog4j.configuration="file:$DISCOUNT_HOME/log4j.properties" \
  --conf $MEMORY \
  --conf $LOCAL_DIR \
  --jars "$DISCOUNT_HOME/target/scala-2.12/Discount-assembly-4.0.0.jar"

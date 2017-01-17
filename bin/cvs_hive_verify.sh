#!/bin/bash

usage() {
  echo 'Usage : sh cvs_hive_verify.sh table, jobId, ingestedCnt, timestamp'
}

if [ "$#" -ne 4 ]
then
  usage
  exit 1
fi

table=$1
jobId=$2
inputCnt=$3
ts=$4

#echo "select count(*) from ${table}_error where iw_job_id='${jobId}'"
err=$(hive -e "select count(*) from ${table}_error where iw_job_id='${jobId}'")
#echo "hive err" $err
if [ $err -ne 0 ]; then
  echo "error count is not zero"
  exit -1
fi 

#echo "select count(*) from ${table} where timestamp='${ts}'"
ins=$(hive -e "select count(*) from ${table} where timestamp='${ts}'")
if [ $inputCnt -ne $ins ]; then
  echo "Ingested hive counts are not equal to the input counts"
  exit -1
fi
echo "Hive ingested counts verification succeeded"

#!/bin/bash
set -e
echo "Making directories"
hadoop fs -mkdir -p /user/cloudera/FP/pfiles/precip/
hadoop fs -mkdir -p /user/cloudera/FP/pfiles/msa_loc/
hadoop fs -mkdir -p /user/cloudera/FP/pfiles/pop/
hadoop fs -mkdir -p /user/cloudera/FP/pfiles/stat/
hadoop fs -mkdir -p /user/cloudera/FP/pfiles/config/
echo "Making directories completed"

echo "Copying files from Edge node to HDFS now"
sh /home/cloudera/Downloads/FinalProject/copy_file.sh /home/cloudera/Downloads/FinalProject/srcPrep/*.txt /user/cloudera/FP/pfiles/precip/ 
sh /home/cloudera/Downloads/FinalProject/copy_file.sh /home/cloudera/Downloads/FinalProject/srcMsa/*.txt /user/cloudera/FP/pfiles/msa_loc/ 
sh /home/cloudera/Downloads/FinalProject/copy_file.sh /home/cloudera/Downloads/FinalProject/srcPop/*.csv /user/cloudera/FP/pfiles/pop/
sh /home/cloudera/Downloads/FinalProject/copy_file.sh /home/cloudera/Downloads/FinalProject/srcStat/*.txt /user/cloudera/FP/pfiles/stat/ 
sh /home/cloudera/Downloads/FinalProject/copy_file.sh /home/cloudera/Downloads/FinalProject/srcConfig/*.sh /user/cloudera/FP/pfiles/config/ 
echo "Copying files completed"

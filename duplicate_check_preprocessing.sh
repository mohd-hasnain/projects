#!/bin/bash

if [[ $# -ne 1 ]]
then
   echo "please pass parameter file"
   exit 1
else
   echo "Number of Parameters Passed : $#"
   echo "Valid Number of Parameters Passed"
fi

#parsing json and getting the variable values
output_directory_path=$(cat ${1} | perl -MJSON -nE 'say decode_json($_)->{output_directory_path}')
log_dir=$(cat ${1} | perl -MJSON -nE 'say decode_json($_)->{log_path}')
hadoop_queue=$(cat ${1} | perl -MJSON -nE 'say decode_json($_)->{hadoop_queue}')
bin_path=$(cat ${1} | perl -MJSON -nE 'say decode_json($_)->{bin_path}')
input_file_path=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{input_file_path}')
input_file_name=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{input_file_name}')
feed_id=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{feed_id}')
reg_id=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{reg_id}')
TRGDATPOS=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{trig_dat_pos}')
trgdlm=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{trig_delim}')
TRGDLIM=`printf "${trgdlm}"`

file_format_type=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{file_format_type}')
abort_ingestion_if_duplicate=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{abort_ingestion_if_duplicate}')

primaryKey_Tag=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{primaryKey_Tag}')
start_end_tag_name=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{start_end_tag_name}')
primaryKey_positions=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{primaryKey_positions}')
delimiter=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{delimiter}')

now=`date +"%d-%m-%Y-%H-%M-%S"`
log_file="GCAP_BATCH_INTERNATIONAL_wrapper_script_-$now.log"
std_log=${log_dir}/${log_file}
touch -f ${std_log}
echo "Feed ID -> ${feed_id}" >> ${std_log}
#register_id=$(cat $1 | perl -MJSON -nE 'say decode_json($_)->{REGISTER_ID}')
register_id="${reg_id}"
echo "Register ID -> ${register_id}" >> ${std_log}
echo ""
echo "#####GCAP BATCH INTERNATIONAL wrapper Script started at `date` #####" >> ${std_log} 
echo "#####GCAP BATCH INTERNATIONAL wrapper script log path ${log_dir}/${log_file}" >> ${std_log} 
echo "" >> ${std_log} 
echo "" >> ${std_log} 
TRIGFILE=`dirname ${input_file_path}`"/trigger/${input_file_name}/${input_file_name}"
echo "output_directory_path -> ${output_directory_path}" >> ${std_log}
echo "TRIGFILE -> ${TRIGFILE}" >> ${std_log}
echo "input_file_path -> ${input_file_path}" >> ${std_log}
echo "input_file_name -> ${input_file_name}" >> ${std_log}
echo "feed_id -> ${feed_id}" >> ${std_log}
echo "Register ID -> ${reg_id}" >> ${std_log}
echo "" >> ${std_log} 
echo "" >> ${std_log} 

logMessage() {

echo $*
        if [[ $log_file != "" ]] ; then
            echo $* >> $std_log
        fi

}
  datafile=$(cat ${TRIGFILE} | cut -d"${TRGDLIM}" -f${TRGDATPOS})
  DATFILE="${input_file_path}""${datafile}/${datafile}"
  echo "input_file_path -> ${input_file_path}" >> ${std_log}
  echo "TRIGFILE -> ${TRIGFILE}" >> ${std_log}
  echo "DATFILE -> ${DATFILE}" >> ${std_log}

output02_directory_path_tmp="${output_directory_path}/${register_id}_${feed_id}/tmp/duplicate_temp_file_${now}"

if [[ $file_format_type = "xml" ]]
then
        className="com.aexp.cs.bulletproofing.XmlParserDriver"
        primKey=$primaryKey_Tag
        delim_val=$start_end_tag_name
else
        className="com.aexp.cs.bulletproofing.DelimitedParserDriver"
        primKey=$primaryKey_positions
        delim_val=$delimiter
fi

hadoop jar ${bin_path}/DuplicateRecord-2.7.0-mapr-1607-SNAPSHOT.jar $className ${DATFILE} ${output02_directory_path_tmp} ${hadoop_queue} ${delim_val} ${primKey} >> ${std_log} 2>&1
#
       STATUS=$?
       echo $STATUS

       if [[ "$STATUS" = "1" ]]
        then

         echo "Hadoop job failed. " >> ${std_log} 2>&1
         exit 1 >> ${std_log} 2>&1
       else
         echo "Hadoop job Completed successfully" >> ${std_log} 2>&1

       fi

echo "[INFO] Listing the output directory of mapreduce:"  >> ${std_log} 2>&1
hadoop fs -ls -R $output02_directory_path_tmp >> ${std_log} 2>&1

#Checking if duplicate record are available
hadoop fs -ls -R ${output02_directory_path_tmp}/DuplicateRecords/duplicate* 
ST1=$?

if [[ "$ST1" -ne "0" ]]
then

   echo "No Duplicate records found" >> ${std_log} 2>&1
else
   echo "Duplicate records are found" >> ${std_log} 2>&1
hadoop fs -cat ${output02_directory_path_tmp}/DuplicateRecords/duplicate* >> ${output02_directory_path_tmp}/final_duplicate_output
ST2=$?
if [[ "$S2" -ne "0" ]]
then

   echo "Process failed. while concating the final duplicate records" >> ${std_log} 2>&1
   exit 1 >> ${std_log} 2>&1
else
   echo " Process  Completed successfully" >> ${std_log} 2>&1
fi
	
	if [[ $abort_ingestion_if_duplicate = "yes" ]]
	then           
	   exit 201
    fi
fi

echo "[INFO] Merging the unique records from mapreduce output" >> $log_file_name 2>&1
hadoop fs -cat ${output02_directory_path_tmp}/part* >> ${output02_directory_path_tmp}/final_unique_output
ST3=$?

if [[ "$ST3" -ne "0" ]]
then

   echo "Process failed. while concating the final unique output" >> ${std_log} 2>&1
   exit 1 >> ${std_log} 2>&1
else
   echo " Process  Completed successfully" >> ${std_log} 2>&1
  #Now need to decide if we need overwrite the orginal data file with this 
fi

echo "Preprocessing Script is successful " >> ${std_log} 2>&1

exit 0

#/home/HadoopUser/Desktop/ques/pig-poc/test-input.log
#/home/HadoopUser/Desktop/output2
echo "PIG POC demo"
PS3="Select an option (1-2): "
select i in initialize_POC_setup exit
do
  case $i in
    initialize_POC_setup) echo "processing"	
    			  echo "Enter input file location"
    			  read input
    			  echo "Enter output file location"
    			  read output
    			  pig -x local -param input=$input -param output=$output './weblog2.pig'
			  echo "executing pig script"
			  hive -e "CREATE table IF NOT EXISTS weblog2(url String,
					error_code String,
					browser String,
					os String,
					date String, 
					time String,
					keyword String	)
				ROW FORMAT DELIMITED
				FIELDS TERMINATED BY '\t';"
			echo "output:" output
   			  hive -hiveconf output=$output -e "LOAD DATA LOCAL INPATH '${hiveconf:output}' OVERWRITE INTO TABLE weblog2;"
    			  echo "completed initialization"
    			  	PS3="Select a hive query or 8 to exit (1-8): "
    			  	select j in TOP_10_URLS LEAST_10_URLS TOP_10_URLS_INPUT_DAY TOP_10_KEYWORDS_INPUT_DAY TOP_3_OS_BY_HOUR TOP_3_BROWSER_BY_HOUR 							TOP_10_ERROR_URLS_BY_HOUR exit
				do
				  case $j in
				  	TOP_10_URLS) hive -S -e 'select url,count(*) as count from weblog2 GROUP BY url ORDER BY count DESC;' ;;
				  	LEAST_10_URLS) hive -S -e 'select url,count(*) as count from weblog2 GROUP BY url ORDER BY count ASC LIMIT 10;' ;;
				  	TOP_10_URLS_INPUT_DAY) 
				  		echo "Enter input day"
				  		read inday
					  	hive -hiveconf input_day=$inday -e "select url,count(*) as count from weblog2 WHERE SUBSTR(date,10,2) = 								'${hiveconf:input_day}' GROUP BY url ORDER BY count ASC LIMIT 10;" ;; 
				  	TOP_10_KEYWORDS_INPUT_DAY) 
						echo "Enter input day for keyword"
					  	read inday
					  	echo inday
					  	echo $inday
					  	hive -hiveconf inday='05' -e "select keyword,count(*) as count from weblog2 WHERE SUBSTR(date,10,2) = '${hiveconf:inday}'  AND keyword <> '' GROUP BY keyword ORDER BY count DESC LIMIT 10;"  ;;
				  	TOP_3_OS_BY_HOUR) 
				  		echo "Enter input day"
					  	read inday
					  	echo "Enter input hour"
					  	read inhour
					  	hive -S  -hiveconf input_day=$inday input_hour=$inhour -e "select os,count(*) as count from weblog2 WHERE 							SUBSTR(date,10,2) = '${hiveconf:input_day}' AND  SUBSTR(time,1,2) = '${hiveconf:input_hour}' GROUP BY os ORDER BY 							count DESC LIMIT 10;" ;;
				  	TOP_3_BROWSER_BY_HOUR) 
				  		echo "Enter input day"
					  	read inday
					  	echo "Enter input hour"
					  	read inhour
					  	hive -S  -hiveconf input_day=$inday input_hour=$inhour -e "select browser,count(*) as count from weblog2 WHERE 							SUBSTR(date,10,2) = '${hiveconf:input_day}'  AND  SUBSTR(time,1,2) = '${hiveconf:input_hour}' GROUP BY browser 							ORDER BY count DESC LIMIT 10;" ;;
				  	TOP_10_ERROR_URLS_BY_HOUR) 
				  		echo "Enter input day"
					  	read inday
					  	echo "Enter input hour"
					  	read inhour
				  		hive -hiveconf input_day=$inday -hiveconf input_hour=$inhour -e "select url,count(*) as count from weblog2 WHERE 							SUBSTR(date,10,2) = '${hiveconf:input_day}' AND SUBSTR(error_code,1,1) IN ('4','5') AND  SUBSTR(time,1,2) = 							'${hiveconf:input_hour}'  GROUP BY url ORDER BY count DESC LIMIT 10;" ;;
					exit) break;;
				  esac
    			      	done
    			      			;;
    exit) exit;;
  esac
done


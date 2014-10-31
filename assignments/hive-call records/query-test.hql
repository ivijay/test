--DELETE JAR file:///home/HadoopUser/Desktop/DateTimeDiff.jar;
--DELETE JAR file:///home/HadoopUser/Desktop/sumtime.jar;
list jars;

dfs -rm  /home/HadoopUser/Desktop/sumtime.jar;
dfs -copyFromLocal /home/HadoopUser/Desktop/sumtime.jar  hdfs://localhost/home/HadoopUser/Desktop/sumtime.jar;

ADD JAR file:///home/HadoopUser/Desktop/DateTimeDiff.jar;
create temporary function datetimediff as 'com.hive.udf.DateTimeDiff';

ADD JAR file:///home/HadoopUser/Desktop/sumtime.jar;
create temporary function sumtime as 'com.hive.udaf.Sumtime';

--dfs -copyFromLocal /home/HadoopUser/Desktop/TimeDiffUDF.jar  hdfs://localhost/home/HadoopUser/Desktop/TimeDiffUDF.jar;
--dfs -copyFromLocal /home/HadoopUser/Desktop/SumOfTimesUDAF.jar  hdfs://localhost/home/HadoopUser/Desktop/SumOfTimesUDAF.jar;

ADD JAR file:///home/HadoopUser/Desktop/TimeDiffUDF.jar;
create temporary function TimeDiffUDF as 'TimeDiffUDF.TimeDiffUDF';

ADD JAR file:///home/HadoopUser/Desktop/SumOfTimesUDAF.jar;
create temporary function SumOfTimesUDAF as 'SumOfTimesUDAF.SumOfTimesUDAF';

--list jars;
set hive.cli.print.header=true;
set hive.root.logger=INFO,console;


--select substring(callingPartyNumber,1,5) as service_provider, TimeDiffUDF(Start_time,End_time) as call_time from log;

--select substring(callingPartyNumber,1,5) as service_provider, count(*) as total_calls, SumOfTimesUDAF(TimeDiffUDF(Start_time,End_time)) as call_time from log
--GROUP BY substring(callingPartyNumber,1,5);

--select substring(callingPartyNumber,1,5) as service_provider, count(*) as total_calls, sumtime(datetimediff(Start_time,End_time)) as call_time from log
--GROUP BY substring(callingPartyNumber,1,5);
--WHERE substring(Called_date,1,4) = "2012";



Select substring(a.callingPartyNumber,1,5), count(*), sumtime(datetimediff(Start_time,End_time))  from log a 
where a.originalCalledPartyNumber <> a.finalCalledPartyNumber
GROUP BY substring(a.callingPartyNumber,1,5);
--AND  substring(Called_date,1,4) = "2012";


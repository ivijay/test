-- show tables;

--DROP TABLE log;

Create table IF NOT EXISTS log (globalCallID_callId INT
,origLegCallIdentifier INT
,destLegCallIdentifier INT
,callingPartyNumber STRING
,originalCalledPartyNumber STRING
	,finalCalledPartyNumber STRING
	,Called_date STRING
	,Start_time STRING
	,End_time STRING
	,lastRedirectDn STRING
	,origCause_Value INT
	,dest_CauseValue INT
	,origMediaCap_payloadCapability INT
	,origMediaCap_Bandwidth INT
	,destMediaCap_payloadCapability INT
	,destMediaCap_Bandwidth INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|' ;

--LOAD DATA LOCAL INPATH '/home/HadoopUser/Desktop/hive-input' OVERWRITE INTO table log;

--select * from log;

--select callingPartyNumber,Called_date,Start_time,End_time from log;

--DROP TEMPORARY FUNCTION IF EXISTS datediff;
--ADD JAR file:///home/HadoopUser/Desktop/hivedatediff.jar;
--create temporary FUNCTION datediff as 'com.hive.log.datediff.DatediffUDF';
--select datediff(Start_time,End_time) from log;
--select callingPartyNumber,Start_time,End_time,datediff(Start_time,End_time) from log;
--select callingPartyNumber,Start_time,End_time,datediff("2011-07-07","2011-07-07") from log;
--select * from log;

--------------------Display the Call Summary of various service providers based on Date, Month and Year------------------
select substring(callingPartyNumber,1,5), count(*),sumtime(datetimediff(Start_time,End_time)) from log
GROUP BY substring(callingPartyNumber,1,5)
--WHERE substring(Called_date,1,4) = "2012";

--------------------Display the Consumers Call Summary for a Service Provider based on Date, Month and Year-----------------
SELECT callingPartyNumber, sumtime(datetimediff(Start_time,End_time)) from log
where substring(callingPartyNumber,1,5) = "99446"
group by callingPartyNumber
--AND substring(Called_date,1,4) = "2012";

-----------------Display the Forwarded Call Summary of various service providers based on Date, Month and Year-----------
Select substring(a.callingPartyNumber,1,5), count(*), sumtime(datetimediff(Start_time,End_time))  from log a 
where a.originalCalledPartyNumber <> a.finalCalledPartyNumber
GROUP BY substring(a.callingPartyNumber,1,5);
--AND substring(Called_date,1,4) = "2012";

------------Display the Top 'n' of Customers for Regular calls of a given service provider based on Date, Month and Year---------
select callingPartyNumber, count(*) as call_count from log
where substring(callingPartyNumber,1,5) = "99446"
group by callingPartyNumber
order by call_count desc
limit 10;

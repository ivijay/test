package test;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class StackOverFlow {  
    public static void main(String args[])  
    {  
        if(args.length != 3)  
        {  
            System.out.println("SparkStream <zookeeper_ip> <group_nm> <topic1,topic2,...>");  
            System.exit(1);  
        }  
        Map<String,Integer> topicMap = new HashMap<String,Integer>();  
        
        String[] topic = args[2].split(",");  
        for(String t: topic)  
            topicMap.put(t, new Integer(1));  
//
        JavaStreamingContext jssc = new JavaStreamingContext("spark://localhost.localdomain:7077", "SparkStream", new Duration(2000));  
//        JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1], topicMap );
//        messages.foreachRDD(messagesstream -> {messagesstream.println});
//        messages.print();  

        jssc.start();  
        jssc.awaitTermination();  

    }  
}  
package com.example.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;


public class Consumer implements Serializable
{

	  public static void main(String args[]) throws InterruptedException
	  {
		final String topic1 = "data";

		 SparkConf sparkConf = new SparkConf()
		    		.setAppName("KafkaConnector")
		    		.setMaster("local[4]")
		    		.set("spark.driver.allowMultipleContexts", "true");
		    		//.set("spark.executor.memory", "4g");
		 JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	
	    Set<String> topics = Collections.singleton("data");			    
	 Map<String, Object> kafkaParams = new HashMap<String, Object>();
	   kafkaParams.put("bootstrap.servers", "localhost:9092");
	   kafkaParams.put("key.deserializer", StringDeserializer.class);
	    kafkaParams.put("value.deserializer", StringDeserializer.class);
	    kafkaParams.put("auto.offset.reset", "latest");
	    kafkaParams.put("group.id", "java-test-consumer-"+"-" + System.currentTimeMillis());
	 JavaInputDStream<ConsumerRecord<String, String>> istream1 = KafkaUtils.createDirectStream(
		        ssc,
		        LocationStrategies.PreferConsistent(),
		        ConsumerStrategies.<String, String>Subscribe(Arrays.asList(topic1), kafkaParams));
	 

	  istream1.foreachRDD(rdd ->{
	    	
	    	rdd.foreach(message ->  {
	    		//System.out.println(message);
     
  	 
    	String ids=message.toString();
    	System.out.println(ids);
       
	    	});
	  });
	
	    ssc.start();
		ssc.awaitTermination();
			}
	
    
	 }
		      			    

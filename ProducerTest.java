package com.example.kafka;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;



public class ProducerTest {
			
	
	private static final String topic = "data";
	
	public static void main(String args[]) throws InterruptedException, IOException, ClassNotFoundException, SQLException {

		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("client.id", "camus");
		
		KafkaProducer<String, String> producer = new KafkaProducer <String , String>(properties);
		
		
			Class.forName("com.mysql.jdbc.Driver");
			Connection con=(Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/kiran", "root","root");
			Statement stmt=(Statement) con.createStatement();
			ResultSet rs=stmt.executeQuery("select * from stockdata");
					while(rs.next())
					{
						String msg=null;
						String company=rs.getString("company"); 
						msg=company;
						ProducerRecord<String, String> message = null;
						
							message = new ProducerRecord<String, String>(topic,msg);
						 
						producer.send(message);
					
				
			}
					con.close();
	
	}
	
}
		

		
		
			
		
			


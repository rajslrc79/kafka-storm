/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package kafka.storm.hdfs;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

public class MyKafkaProducer {
	
    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;
    

    public static void main(String[] args) throws Exception {
    	
    	//turn off sl4j warnings
    	ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    	rootLogger.setLevel(Level.toLevel("info"));
    	    	
        if (args.length < 8) {
            System.err.println("USAGE: java -cp "+ MyKafkaProducer.class.getPackage() +" "+ MyKafkaProducer.class.getName() +
                               " broker_list buffer_memory topic_name #_of_acks batch_size throughput(-1/1) source_file has_headers(true/false)");
            System.exit(1);
        }

        long startTime = System.currentTimeMillis();
               
        /* parse args */
        String brokerlist = args[0];
	    String bufferMemory = args[1];
	    String topicName = args[2];
	    String acks = args[3];
	    String batchSize = args[4];
	    int throughput = Integer.parseInt(args[5]);
	    String file = args[6];
	    boolean hasHeaders = Boolean.parseBoolean(args[7]);
	    
	    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	    
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerlist);
		props.put("buffer.memory", bufferMemory);
	    props.put("acks", acks);
	    props.put("batch.size", batchSize);
	    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        FileInputStream fstream = new FileInputStream(file);
		
		System.out.println("\nPRODUCER CONFIGURATION:"
	    		+"\n----------------------------------------------------------"
	    		+"\n----------------------------------------------------------"
	    		+"\n   Start Time: "+sdfDate.format(startTime)
	    		+"\n   Broker List: "+brokerlist
	    		+"\n   Topic: "+topicName
	    		+"\n   Number of Acks: "+acks
	    		+"\n   Messages Batch Size: "+batchSize+" bytes"
	    		+"\n   Buffer Max Memory Size: "+bufferMemory+" bytes"
	    		+"\n   Input file Name: "+file
	    		+"\n----------------------------------------------------------"
	    		+"\n----------------------------------------------------------");
	    
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		
		//read headers
		if(hasHeaders)
			br.readLine();      
		
		KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
		  
	    long sleepTime = NS_PER_SEC / throughput;
        long sleepDeficitNs = 0;
        String strLine = null;
        int count = 0;
        long bytes = 0;
        
      
        
        while((strLine= br.readLine()) != null){

        		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, strLine);
                
                Callback cb = new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) { 
                    	if (e != null)
                        e.printStackTrace();
                    		} };
                    		
                bytes+=strLine.length();
                producer.send(record, cb);
                count++;

                
//                 * Maybe sleep a little to control throughput. Sleep time can be a bit inaccurate for times < 1 ms so
//                 * instead of sleeping each time instead wait until a minimum sleep time accumulates (the "sleep deficit")
//                 * and then make up the whole deficit in one longer sleep.
                 
                if (throughput > 0) {
                    sleepDeficitNs += sleepTime;
                    if (sleepDeficitNs >= MIN_SLEEP_NS) {
                        long sleepMs = sleepDeficitNs / 1000000;
                        long sleepNs = sleepDeficitNs - sleepMs * 1000000;
                        Thread.sleep(sleepMs, (int) sleepNs);
                        sleepDeficitNs = 0;
                    }
                }
        }

	    producer.close();
	    br.close();
		in.close();
		fstream.close();
		long ellapsed = System.currentTimeMillis() - startTime;
		double recsPerSec = 1000.0 * count / (double) ellapsed;
		double mbPerSec = 1000.0 * bytes / (double) ellapsed / (1024.0 * 1024.0);
      
		System.out.printf("Time elapsed %d milliseconds, %d records sent, %f records/sec (%.2f MB/sec)",
                        ellapsed,
                        count,
                        recsPerSec,
                        mbPerSec);
		System.out.println();

    }
}
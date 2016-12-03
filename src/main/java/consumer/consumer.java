package consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;


public class consumer{
    private static final String TOPIC = "b";
    private static final int NUM_THREADS = 20;
   
       
    public static void main(String[] args) throws Exception {
    	
        Properties props = new Properties();
        props.put("group.id", "test-group");
        props.put("zookeeper.connect", "163.152.47.94:2182");
        props.put("auto.commit.interval.ms", "100");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, NUM_THREADS);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TOPIC);
        ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
        long startTime = System.currentTimeMillis();
        
        for (final KafkaStream<byte[], byte[]> stream : streams) {
        	        	    
           executor.execute(new Runnable() {
          int count=0;
          long startTime = System.currentTimeMillis();
                public void run() {
                    for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
                    	                  	
                    	byte[] test;
                    	
                    	test=messageAndMetadata.message();
                    	count++;
                    	System.out.println("one_complete!"+" "+count);
               //     	String str ="";
                                     	
               //     	str = new String(messageAndMetadata.message());
              //      	str = str + "    Test";
                 //       System.out.println(str);
                    }
                }
     
          
            });
           
         
        }
        long endTime = System.currentTimeMillis();
        long Result_Time = endTime - startTime;
        
        System.out.println("Result : " + Result_Time + "(ms)");
        
        
        Thread.sleep(60000);
        System.out.println("empty topic!!");
        consumer.shutdown();
        executor.shutdown();
    }
}
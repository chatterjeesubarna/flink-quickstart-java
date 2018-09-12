package org.apache.flink;


import java.util.Properties;
import java.util.Random;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010.FlinkKafkaProducer010Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
//import org.apache.flink.streaming.connectors.kafka.KafkaSink;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;



public class WriteIntoKafka {

	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		ExecutionConfig ec = env.getConfig();
		
		ec.setLatencyTrackingInterval(2000L);
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		//properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092,slave3:9092");
		properties.setProperty("group.id", "test");

		DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());
		messageStream.getExecutionConfig().setLatencyTrackingInterval(2000L);

		FlinkKafkaProducer010Configuration<String> myProducerConfig = FlinkKafkaProducer010.writeToKafkaWithTimestamps(
				messageStream,                     // input stream
				"topic",                 // target topic
				new SimpleStringSchema(),   // serialization schema
				properties);                // custom configuration for KafkaProducer (including broker list)

		try {
			env.execute();
			myProducerConfig.setLogFailuresOnly(false);   // "false" by default
			myProducerConfig.setFlushOnCheckpoint(true);  // "false" by default
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static class SimpleStringGenerator implements SourceFunction<String> {
		
		private static final long serialVersionUID = -6867736771747690202L;
		Random _rand;
		//Thread.sleep(10);
	    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
	        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
	    	    
		boolean running = true;
		long i = 0;
		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while(running) {
				_rand = new Random();
				String sentence = sentences[_rand.nextInt(sentences.length)];
				//System.out.println(sentence);
				ctx.collect(sentence);
				Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

}

package AIS;

import java.util.Properties;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.flink.ReadFromKafka.Splitter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import dk.tbsalling.aismessages.AISInputStreamReader;


public class FirstKafkaConsumer {

	/*public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = -6867736771747690202L;
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
			
			for (String word: sentence.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}*/

	public static void main(String[] args) throws Exception {


		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		//properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092,slave3:9092");
		properties.setProperty("group.id", "test");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>("static_topic", new SimpleStringSchema(), properties));
		messageStream.getExecutionConfig().setLatencyTrackingInterval(2000L);
		messageStream.rebalance()
		.map(new MapFunction<String, String>() {

			private static final long serialVersionUID = -6867736771747690202L;
			@Override
			public String map(String value) throws Exception {
				System.out.println(value);
				return value;
			}
		})
		/*.flatMap(new Splitter())
		.keyBy(0)
		.timeWindow(Time.milliseconds(10))
		.sum(1)
		.print()*/;

		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();


		}

	}
}

package AIS;

//./bin/flink run -p1 ~/workspace/flink-quickstart-java/target/ReadFromStaticTopic.jar

import java.util.Properties;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.flink.ReadFromKafka.Splitter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

public class ReadFromStaticTopic {

	public static void main(String[] args) {
		
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>("static_topic", new SimpleStringSchema(), properties));
		messageStream.getExecutionConfig().setLatencyTrackingInterval(2000L);
		messageStream.rebalance()
		.map(new MapFunction<String, String>() {

			private static final long serialVersionUID = -6867736771747690202L;
			@Override
			public String map(String value) throws Exception {
				System.out.println("************ Static topic ***************" + value);
				return value;
			}
		});
		
		String opPath = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/static";
		messageStream.writeAsText(opPath, WriteMode.OVERWRITE).name("The Sink");
		
		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();


		}

	}

}

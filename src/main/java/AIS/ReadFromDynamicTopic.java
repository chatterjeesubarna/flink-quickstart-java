package AIS;

//Gk5
// ./bin/flink run -p1 /root/flink-quickstart-java/target/ReadFromDynamicTopic.jar 0 100 100 0 &

// LOcal mc
// ./bin/flink run -p1 ~/workspace/flink-quickstart-java/target/ReadFromDynamicTopic.jar 0 100 100 0

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.flink.ReadFromKafka.Splitter;
import org.apache.flink.api.common.functions.FilterFunction;
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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.sling.commons.json.JSONObject;

public class ReadFromDynamicTopic {

	public static float x_1, y_1, x_2, y_2;


	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Boolean>> {
		private static final long serialVersionUID = -6867736771747690202L;
		@Override
		public void flatMap(String record, Collector<Tuple2<String, Boolean>> out) throws Exception {
			//System.out.println(record);
			ArrayList<String> recordList = new ArrayList<String>();
			for (String field: record.split(", ")) {
				recordList.add(field);
			}
			float latitude = Float.parseFloat(recordList.get(2));
			float longitude = Float.parseFloat(recordList.get(3));
			String result = "";

			if(latitude >= x_1 && latitude <= x_2 && longitude >= y_2 && longitude <= y_1)
			{
				//System.out.println("************metadata ***********" + recordList.get(4));
				result="Boat ".concat(recordList.get(0)).concat(" entered the zone with position ").concat(String.valueOf(latitude)).concat(", ")
						.concat(String.valueOf(longitude)).concat(" at time ").concat(String.valueOf(recordList.get(4)))
						.concat(" with speed over ground ").concat(String.valueOf(recordList.get(1)));
				/*result = "The point ".concat(String.valueOf(latitude)).concat(", ").concat(String.valueOf(longitude))
						.concat(" is within the rectangle with upper left as ").concat(String.valueOf(x_1)).concat(", ")
						.concat(String.valueOf(y_1)).concat(" and lower right as ").concat(String.valueOf(x_2)).concat(", ")
						.concat(String.valueOf(y_2)).concat(" \nOriginal record: ").concat(record);*/
				out.collect(new Tuple2<String, Boolean>(result, true));
			} 

		}
	}

	public static class SurveillanceMonitor implements MapFunction<String, String> {
		@Override
		public String map(String record) throws Exception {
			if(!record.equals(""))
			{
				JSONObject jsonObject = new JSONObject(record);
				float latitude = Float.parseFloat(jsonObject.getString("x"));
				float longitude = Float.parseFloat(jsonObject.getString("y"));
				String boat = jsonObject.getString("mmsi");
				String time = "";
				String sog = "";
				if (jsonObject.has("tagblock_timestamp"))
					time = jsonObject.getString("tagblock_timestamp");
				if (jsonObject.has("sog"))
					sog = jsonObject.getString("sog");
				String result = "";

				if(latitude >= x_1 && latitude <= x_2 && longitude >= y_2 && longitude <= y_1)
				{
					result="Boat ".concat(boat).concat(" entered the zone with position ").concat(String.valueOf(latitude)).concat(", ")
							.concat(String.valueOf(longitude)).concat(" at time ").concat(time)
							.concat(" with speed over ground ").concat(sog);
					System.out.println(result);
					return result;
				}
				else
					System.out.println("Not crossed " + x_1 + " " + y_1 + " " + x_2 + " " + y_2);
			}
			return ""; 

		}
	}

	public static class Analyzer implements FlatMapFunction<String, Tuple2<Boolean, String>> {
		private static final long serialVersionUID = -6867736771747690202L;
		@Override
		public void flatMap(String record, Collector<Tuple2<Boolean, String>> out) throws Exception {

		}
	}

	public static class SplitterDataDynamic implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			String type="", value = "";
			int c = 0;
			JSONObject jsonObject = new JSONObject(sentence);
			if(jsonObject.has("_ais_type") && jsonObject.getString("_ais_type").equals("dynamic"))
			{
				//System.out.println(sentence + " .................. ");
				out.collect(new Tuple2<String, String>(sentence, ""));
			}

		}
	}

	public static class SplitterDataDynamic1 implements MapFunction<String, String> {
		@Override
		public String map(String sentence) throws Exception {
			JSONObject jsonObject = new JSONObject(sentence);
			if(jsonObject.has("_ais_type") && jsonObject.getString("_ais_type").equals("dynamic"))
			{
				//System.out.println(sentence + " .................. ");
				return sentence;
			}
			else
				return "";
		}
	}

	public static class SplitterDataStatic implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			String type="", value = "";
			int c = 0;
			JSONObject jsonObject = new JSONObject(sentence);
			if(jsonObject.has("_ais_type") && jsonObject.getString("_ais_type").equals("static"))
			{
				//System.out.println(sentence + " .................. ");
				out.collect(new Tuple2<String, String>(sentence, ""));
			}

		}
	}

	public static void main(String[] args) {

		Properties properties = new Properties();
		//properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("bootstrap.servers", "192.168.143.245:29092");
		//properties.setProperty("group.id", "test");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		properties.put("auto.offset.reset", "earliest");
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>("ais", new SimpleStringSchema(), properties)).name("Kafka source");
		messageStream.getExecutionConfig().setLatencyTrackingInterval(2000L);

		/*DataStream<Tuple2<String, String>> dataStreamAIS =
				messageStream.rebalance().flatMap(new SplitterData());

		String opPath1 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/dynamic";
		dataStreamAIS.writeAsText(opPath1, WriteMode.OVERWRITE).name("The Sink");*/


		messageStream.rebalance()
		.map(new MapFunction<String, String>() {

			private static final long serialVersionUID = -6867736771747690202L;
			@Override
			public String map(String value) throws Exception {
				return value;
			}
		});

		String opPath1 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/all";
		messageStream.writeAsText(opPath1, WriteMode.OVERWRITE).name("The Sink");

		DataStream<String> dynamicStream =
				messageStream.rebalance().map(new SplitterDataDynamic1());
		String opPath2 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/dynamic";
		dynamicStream.writeAsText(opPath2, WriteMode.OVERWRITE).name("Dynamic Stream");

		/*DataStream<Tuple2<String, String>> dynamicStream =
				messageStream.rebalance().flatMap(new SplitterDataDynamic());
		String opPath2 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/dynamic";
		dynamicStream.writeAsText(opPath2, WriteMode.OVERWRITE).name("Dynamic Stream");*/

		DataStream<Tuple2<String, String>> staticStream =
				messageStream.rebalance().flatMap(new SplitterDataStatic());
		String opPath3 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/static";
		staticStream.writeAsText(opPath3, WriteMode.OVERWRITE).name("Static Stream");

		x_1 = 0; //new Random().nextInt(100-0) + 0; //Float.parseFloat(args[0]); // Abscissa of upper left
		y_1 = 100; //new Random().nextInt(100-0) + 0 ;//Float.parseFloat(args[1]); // Ordinate of upper left
		x_2 = 100; //new Random().nextInt(100-0) + 0; //Float.parseFloat(args[2]); // Abscissa of lower right
		y_2 = 0; //new Random().nextInt(100-0) + 0; //Float.parseFloat(args[3]); // Ordinate of lower right

		System.out.println(x_1 + " " + y_1 + " " + x_2 + " " + y_2);


		DataStream<String> surveilleanceRecord = dynamicStream.rebalance().map(new SurveillanceMonitor());
		String opPath4 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/intrusion";
		surveilleanceRecord.writeAsText(opPath4, WriteMode.OVERWRITE).name("Intrusion Analysis");
		//messageStream.rebalance().flatMap(new Splitter()).setParallelism(2);

		//String opPath2 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/intrusion";
		//dynamicStream.writeAsText(opPath2, WriteMode.OVERWRITE).name("Intrusion Analysis");
		//recordList.print();




		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();


		}


	}

}

package Subarna;

import java.util.HashMap;

import org.apache.flink.WordCountStreamAPI.Splitter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WriteCOValues {

	public static HashMap<String, Double> hmap = new HashMap<String, Double>();
	public static class SplitterCO implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			String type = "", value = "";
			int c = 0;
			for (String word: sentence.split(",")) {
				if(++c == 3)
				{
					type = word;
				}
				if(++c == 10)
				{
					value = word;
					out.collect(new Tuple2<String, String>(type, value));
				}
			}
		}
	}

	public static class SplitterNO2 implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			String type = "NO2", value = "";
			int c = 0;
			//System.out.println(sentence + " " + (c++));;
			for (String word: sentence.split(",")) {
				if(++c == 5)
				{
					value = word;
					out.collect(new Tuple2<String, String>(type, value));
				}
			}
		}
	}
	
	public static class SplitterO3 implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			String type = "", value = "";
			int c = 0;
			for (String word: sentence.split(",")) {
				if(++c == 3)
				{
					type = word;
				}
				if(++c == 10)
				{
					value = word;
					out.collect(new Tuple2<String, String>(type, value));
				}
			}
		}
	}
	
	public static class SplitterPM10 implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			String type = "", value = "";
			int c = 0;
			for (String word: sentence.split(",")) {
				if(++c == 3)
				{
					type = word;
				}
				if(++c == 10)
				{
					value = word;
					out.collect(new Tuple2<String, String>(type, value));
				}
			}
		}
	}
	
	public static class assessQuality implements ReduceFunction<Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> arg0, Tuple2<String, String> arg1)
				throws Exception {
			if(Double.parseDouble(arg0.getField(1).toString()) <= Double.parseDouble(hmap.get(arg0.getField(0)).toString()))
			{
				System.out.println("Good: " + arg0.getField(1).toString());;
			}
			else
			{
				System.out.println("Bad: " + arg0.getField(1).toString());;
			}
			return null;
		}
		
	}

	public static void main(String[] args) {
		
		hmap.put("CO", 0.4);
		hmap.put("NO2", 0.4);
		hmap.put("O3", 0.4);
		hmap.put("PM10", 0.4);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> COstream = env.readTextFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/CO");
		DataStream<String> NO2stream = env.readTextFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/NO2");
		DataStream<String> O3stream = env.readTextFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/O3");
		DataStream<String> PM10stream = env.readTextFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/PM10");

		DataStream<Tuple2<String, String>> dataStreamCO =
				COstream.flatMap(new SplitterCO());

		DataStream<Tuple2<String, String>> dataStreamNO2 =
				NO2stream.flatMap(new SplitterNO2());
		
		DataStream<Tuple2<String, String>> dataStreamO3 =
				O3stream.flatMap(new SplitterO3());
		
		DataStream<Tuple2<String, String>> dataStreamPM10 =
				PM10stream.flatMap(new SplitterPM10());

		//        .keyBy(0)
		//      .timeWindow(Time.seconds(10))
		//    .sum(1);

		//dataStreamCO.print();
		//dataStreamNO2.print();
		//dataStreamO3.print();
		//dataStreamPM10.print();
		DataStream<Tuple2<String, String>> allStream = dataStreamCO.union(dataStreamNO2).union(dataStreamO3).union(dataStreamPM10);
		
		allStream.keyBy(0).reduce(new assessQuality());
		
		try {
			env.execute("Air Quality Monitoring");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}

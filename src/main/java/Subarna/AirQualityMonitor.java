package Subarna;

import java.util.*;
import java.io.*;

import org.apache.flink.WriteIntoKafka.SimpleStringGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

// G5k
// ./bin/flink run -p 10 /root/flink-quickstart-java/target/AirQualityMonitor.jar

//Local mc
// ./bin/flink run -p 2 ~/workspace/flink-quickstart-java/target/AirQualityMonitor.jar 

public class AirQualityMonitor {

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
			//System.out.println(arg0);
			if(hmap.containsKey(arg0.getField(0)))
			{
				if(Double.parseDouble(arg0.getField(1).toString()) <= Double.parseDouble(hmap.get(arg0.getField(0)).toString()))
				{
					//System.out.println("Good: " + arg0.getField(1).toString());;
				}
				else
				{
					//System.out.println("Bad: " + arg0.getField(1).toString());;
				}
			}
			return null;
		}

	}

	public static class assessAirQuality implements ReduceFunction<Tuple2<String, String>> {

		ArrayList<Double> COarr = new  ArrayList<Double>();
		ArrayList<Double> NO2arr = new  ArrayList<Double>();
		ArrayList<Double> O3arr = new  ArrayList<Double>();
		ArrayList<Double> PM10arr = new  ArrayList<Double>();
		ArrayList<Double> temp = new  ArrayList<Double>();
		int winSize=10000;

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> arg0, Tuple2<String, String> arg1)
				throws Exception {
			if(arg0.getField(0).equals("CO"))
			{
				temp = COarr;
			}
			else if(arg0.getField(0).equals("NO2"))
			{
				temp = COarr;
			}
			else if(arg0.getField(0).equals("O3"))
			{
				temp = COarr;
			}
			else if(arg0.getField(0).equals("PM10"))
			{
				temp = COarr;
			}
			temp.add(Double.parseDouble(arg0.getField(1).toString()));

			if(temp.size() >= winSize)
			{
				double sum = 0, avg;
				Double threshold = 0.0;
				if (hmap.containsKey(arg0.getField(0)))
					threshold = Double.parseDouble(hmap.get(arg0.getField(0)).toString());
				for(Double d: temp)
				{
					sum = sum + d;
				}
				avg = sum/temp.size();
				if(avg >= threshold)
					System.out.println("Bad air quality!");
				else
					System.out.println("Good air quality!");
			}
			if(hmap.containsKey(arg0.getField(0)))
			{
				if(Double.parseDouble(arg0.getField(1).toString()) <= Double.parseDouble(hmap.get(arg0.getField(0)).toString()))
				{
					//System.out.println("Good: " + arg0.getField(1).toString());;
				}
				else
				{
					//System.out.println("Bad: " + arg0.getField(1).toString());;
				}
			}
			return null;
		}

	}

	public static ArrayList<String> readFile(String path)
	{
		ArrayList<String> lines = new ArrayList<String>();
		File file = new File(path);
		try {
			Scanner input = new Scanner(file);
			while(input.hasNextLine())
			{
				String line = input.nextLine();
				lines.add(line);
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lines;
	}


	public static void main(String[] args) throws Exception {

		hmap.put("CO", 0.4);
		hmap.put("NO2", 0.4);
		hmap.put("O3", 0.4);
		hmap.put("PM10", 0.4);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> COstream = env.addSource(new ParallelSourceFunction<String>() {
			Random _rand;
			//ArrayList<String> CO = readFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/CO");
			ArrayList<String> CO = readFile("/root/flink-quickstart-java/src/main/java/Subarna/CO");
			boolean running = true;
			int max = 500000;

			public void run(SourceContext<String> ctx) throws Exception {
				int count = -1; int total=0;
				while(running) {
					count = (count + 1)%CO.size();
					total++;
					//if(total <= max)
					ctx.collect(CO.get(count));
					//System.out.println("CO " + CO.get(count));;
					Thread.sleep(10);
				}
			}

			public void cancel() {
			}
		}).name("CO_source");

		DataStream<String> NO2stream = env.addSource(new ParallelSourceFunction<String>() {
			Random _rand;
			//ArrayList<String> NO2 = readFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/NO2");
			ArrayList<String> NO2 = readFile("/root/flink-quickstart-java/src/main/java/Subarna/NO2");
			boolean running = true;
			int max = 500000;

			public void run(SourceContext<String> ctx) throws Exception {
				int count = -1; int total=0;
				while(running) {
					count = (count + 1)%NO2.size();
					total++;
					//if(total <= max)
					ctx.collect(NO2.get(count));
					//System.out.println("NO2 " + NO2.get(count));;
					Thread.sleep(10);
				}
			}

			public void cancel() {
			}
		}).name("NO2_source");

		DataStream<String> O3stream = env.addSource(new ParallelSourceFunction<String>() {
			Random _rand;
			//ArrayList<String> O3 = readFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/O3");
			ArrayList<String> O3 = readFile("/root/flink-quickstart-java/src/main/java/Subarna/O3");
			boolean running = true;
			int max = 500000;

			public void run(SourceContext<String> ctx) throws Exception {
				int count = -1; int total=0;
				while(running) {
					count = (count + 1)%O3.size();
					total++;
					//if(total <= max)
					ctx.collect(O3.get(count));
					//System.out.println("O3 " + O3.get(count));;
					Thread.sleep(10);
				}
			}

			public void cancel() {
			}
		}).name("O3_source");

		DataStream<String> PM10stream = env.addSource(new ParallelSourceFunction<String>() {
			Random _rand;
			//ArrayList<String> PM10 = readFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/Subarna/PM10");
			ArrayList<String> PM10 = readFile("/root/flink-quickstart-java/src/main/java/Subarna/PM10");
			boolean running = true;
			int max = 500000;

			public void run(SourceContext<String> ctx) throws Exception {
				int count = -1; int total=0;
				while(running) {
					count = (count + 1)%PM10.size();
					total++;
					//if(total <= max)
					ctx.collect(PM10.get(count));
					//System.out.println("PM10 " + PM10.get(count));;
					Thread.sleep(10);
				}
			}

			public void cancel() {
			}
		}).name("PM10_source");

		DataStream<Tuple2<String, String>> dataStreamCO =
				COstream.flatMap(new SplitterCO());

		DataStream<Tuple2<String, String>> dataStreamNO2 =
				NO2stream.flatMap(new SplitterNO2());

		DataStream<Tuple2<String, String>> dataStreamO3 =
				O3stream.flatMap(new SplitterO3());

		DataStream<Tuple2<String, String>> dataStreamPM10 =
				PM10stream.flatMap(new SplitterPM10());

		DataStream<Tuple2<String, String>> allStream = dataStreamCO.union(dataStreamNO2).union(dataStreamO3).union(dataStreamPM10);

		allStream.keyBy(0).reduce(new assessQuality());
		allStream.keyBy(0).reduce(new assessAirQuality());

		//allStream.print();
		try {
			env.execute("Air Quality Monitoring");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

package FlightDelayPackage;

import java.util.*;
import java.io.*;

import org.apache.flink.WordCountStreamAPI.Splitter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;


// g5k
// ./bin/flink run -p 10 /root/flink-quickstart-java/target/FlightDelay.jar

public class FlightDelay {

	//public static File file = new File("/home/suchatte/workspace/flink-quickstart-java/src/main/java/FlightDelayPackage/2008.csv");
	//public static File file = new File("/root/flink-quickstart-java/src/main/java/FlightDelayPackage/2008.csv");
	public static HashMap<String, int[][]> hmap = new HashMap<String, int[][]>();
	public static HashMap<Integer, HashMap<String, int[][]>> hmap1 = new HashMap<Integer, HashMap<String, int[][]>>();


	public static class Splitter1 implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			//System.out.println(sentence);
			String[] oneLine = sentence.split(",");
			String reducedtuple = oneLine[8].concat(",").concat(oneLine[14]).concat(",").concat(oneLine[15]).concat(",").concat(oneLine[24])
					.concat(",").concat(oneLine[25]).concat(",").concat(oneLine[26]).concat(",").concat(oneLine[26]).concat(",").concat(oneLine[28]);
			out.collect(new Tuple2<String, String>(oneLine[8], reducedtuple));
		}
	}

	public static class Splitter2 implements FlatMapFunction<String, Tuple2<String, String>> {
		@Override
		public void flatMap(String sentence, Collector<Tuple2<String, String>> out) throws Exception {
			//System.out.println(sentence);
			String[] oneLine = sentence.split(",");
			String reducedtuple = oneLine[8].concat(",").concat(oneLine[14]).concat(",").concat(oneLine[15]).concat(",").concat(oneLine[24])
					.concat(",").concat(oneLine[25]).concat(",").concat(oneLine[26]).concat(",").concat(oneLine[26]).concat(",").concat(oneLine[28]);
			out.collect(new Tuple2<String, String>(oneLine[3], reducedtuple));
		}
	}

	public static class DayDelaySum implements ReduceFunction<Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> arg0, Tuple2<String, String> arg1)
				throws Exception {
			String[] oneLine = arg0.getField(1).toString().split(",");;
			int day = Integer.parseInt(arg0.getField(0).toString());
			String carrier = oneLine[0];
			//System.out.println(day);

			if(!hmap1.containsKey(day))
			{
				HashMap<String, int[][]> hmap = new HashMap<String, int[][]>();
				int temp[][] = new int[7][2];
				hmap.put(carrier, temp);
				hmap1.put(day,  hmap);
			}
			for(int i =1;i<oneLine.length;i++)
			{
				String delay = oneLine[i];
				if (delay.equals("NA"))
					delay = "0";
				int d = Integer.parseInt(delay);
				if (d > 0)
				{
					HashMap<String, int[][]> hmap = hmap1.get(day);
					int[][] array = hmap.get(carrier);
					array[i-1][0] = array[i-1][0] + 1;
					array[i-1][1] =  array[i-1][1] + d;
					hmap.put(carrier, array);
					hmap1.put(day,  hmap);
				}
			}
			return arg0;
		}

	}

	public static class DelaySum implements ReduceFunction<Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> arg0, Tuple2<String, String> arg1)
				throws Exception {
			String[] oneLine = arg0.getField(1).toString().split(",");;
			String carrier = arg0.getField(0);
			//System.out.println(arg0);
			if(!hmap.containsKey(carrier))
			{
				int temp[][] = new int[7][2];
				hmap.put(carrier, temp);
			}
			for(int i =1;i<oneLine.length;i++)
			{
				String delay = oneLine[i];
				if (delay.equals("NA"))
					delay = "0";
				int d = Integer.parseInt(delay);
				if (d > 0)
				{
					int[][] array = hmap.get(carrier);
					array[i-1][0] = array[i-1][0] + 1;
					array[i-1][1] =  array[i-1][1] + d;
					hmap.put(carrier, array);
				}
			}
			return arg0;
		}

	}

	public static class DayDelayRank implements ReduceFunction<Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> arg0, Tuple2<String, String> arg1)
				throws Exception {
			String[] oneLine = arg0.getField(1).toString().split(",");;

			HashMap <Integer, HashMap<String, Double>> arrDelay  = new HashMap <Integer, HashMap<String, Double>>();
			HashMap <Integer, HashMap<String, Double>> depDelay  = new HashMap <Integer, HashMap<String, Double>>();
			HashMap <Integer, HashMap<String, Double>> carrierDelay  = new HashMap <Integer, HashMap<String, Double>>();
			HashMap <Integer, HashMap<String, Double>> weatherDelay  = new HashMap <Integer, HashMap<String, Double>>();
			HashMap <Integer, HashMap<String, Double>> NASDelay  = new HashMap <Integer, HashMap<String, Double>>();
			HashMap <Integer, HashMap<String, Double>> securityDelay  = new HashMap <Integer, HashMap<String, Double>>();
			HashMap <Integer, HashMap<String, Double>> lateDelay  = new HashMap <Integer, HashMap<String, Double>>();

			for(int day: hmap1.keySet())
			{
				HashMap<String, int[][]> hmap = hmap1.get(day);
				for(String carrier: hmap.keySet())
				{
					int[][] array = hmap.get(carrier);

					if(!arrDelay.containsKey(day))
					{
						HashMap<String, Double> hmap2 = new HashMap<String, Double>();
						hmap2.put(carrier, 0.0);
						arrDelay.put(day, hmap2);
					}
					if(arrDelay.get(day).containsKey(carrier) && array[0][0]!=0)
					{
						arrDelay.get(day).put(carrier, (double)array[0][1]/array[0][0]);
					}

					if(!depDelay.containsKey(day))
					{
						HashMap<String, Double> hmap2 = new HashMap<String, Double>();
						hmap2.put(carrier, 0.0);
						depDelay.put(day, hmap2);
					}
					if(depDelay.get(day).containsKey(carrier) && array[0][0]!=0)
					{
						depDelay.get(day).put(carrier, (double)array[0][1]/array[0][0]);
					}

					if(!carrierDelay.containsKey(day))
					{
						HashMap<String, Double> hmap2 = new HashMap<String, Double>();
						hmap2.put(carrier, 0.0);
						carrierDelay.put(day, hmap2);
					}
					if(carrierDelay.get(day).containsKey(carrier) && array[0][0]!=0)
					{
						carrierDelay.get(day).put(carrier, (double)array[0][1]/array[0][0]);
					}

					if(!weatherDelay.containsKey(day))
					{
						HashMap<String, Double> hmap2 = new HashMap<String, Double>();
						hmap2.put(carrier, 0.0);
						weatherDelay.put(day, hmap2);
					}
					if(weatherDelay.get(day).containsKey(carrier) && array[0][0]!=0)
					{
						weatherDelay.get(day).put(carrier, (double)array[0][1]/array[0][0]);
					}

					if(!NASDelay.containsKey(day))
					{
						HashMap<String, Double> hmap2 = new HashMap<String, Double>();
						hmap2.put(carrier, 0.0);
						NASDelay.put(day, hmap2);
					}
					if(NASDelay.get(day).containsKey(carrier) && array[0][0]!=0)
					{
						NASDelay.get(day).put(carrier, (double)array[0][1]/array[0][0]);
					}

					if(!securityDelay.containsKey(day))
					{
						HashMap<String, Double> hmap2 = new HashMap<String, Double>();
						hmap2.put(carrier, 0.0);
						securityDelay.put(day, hmap2);
					}
					if(securityDelay.get(day).containsKey(carrier) && array[0][0]!=0)
					{
						securityDelay.get(day).put(carrier, (double)array[0][1]/array[0][0]);
					}

					if(!lateDelay.containsKey(day))
					{
						HashMap<String, Double> hmap2 = new HashMap<String, Double>();
						hmap2.put(carrier, 0.0);
						lateDelay.put(day, hmap2);
					}
					if(lateDelay.get(day).containsKey(carrier) && array[0][0]!=0)
					{
						lateDelay.get(day).put(carrier, (double)array[0][1]/array[0][0]);
					}
				}

			}
			System.out.println("arrDelay: " + arrDelay);	
			System.out.println("depDelay: " + depDelay);	
			System.out.println("carrierDelay: " + carrierDelay);	
			System.out.println("weatherDelay: " + weatherDelay);	
			System.out.println("NASDelay: " + NASDelay);	
			System.out.println("securityDelay: " + securityDelay);	
			System.out.println("lateDelay: " + lateDelay);	
			return null;
		}
	}

	public static class DelayRank implements ReduceFunction<Tuple2<String, String>> {

		@Override
		public Tuple2<String, String> reduce(Tuple2<String, String> arg0, Tuple2<String, String> arg1)
				throws Exception {
			String[] oneLine = arg0.getField(1).toString().split(",");;
			//String carrier = arg0.getField(0);

			HashMap<String, Double> arrDelay  = new HashMap<String, Double>();
			HashMap<String, Double> depDelay  = new HashMap<String, Double>();
			HashMap<String, Double> carrierDelay  = new HashMap<String, Double>();
			HashMap<String, Double> weatherDelay  = new HashMap<String, Double>();
			HashMap<String, Double> NASDelay  = new HashMap<String, Double>();
			HashMap<String, Double> securityDelay  = new HashMap<String, Double>();
			HashMap<String, Double> lateDelay  = new HashMap<String, Double>();

			for(String carrier: hmap.keySet())
			{
				int[][] array = hmap.get(carrier);
				if(!arrDelay.containsKey(carrier) && array[0][0]!=0)
				{
					arrDelay.put(carrier, (double)array[0][1]/array[0][0]);
				}
				if(!depDelay.containsKey(carrier) && array[1][0]!=0)
				{
					depDelay.put(carrier, (double)array[1][1]/array[1][0]);
				}
				if(!carrierDelay.containsKey(carrier) && array[2][0]!=0)
				{
					carrierDelay.put(carrier, (double)array[2][1]/array[2][0]);
				}
				if(!weatherDelay.containsKey(carrier) && array[3][0]!=0)
				{
					weatherDelay.put(carrier, (double)array[3][1]/array[3][0]);
				}
				if(!NASDelay.containsKey(carrier) && array[4][0]!=0)
				{
					NASDelay.put(carrier, (double)array[4][1]/array[4][0]);
				}
				if(!securityDelay.containsKey(carrier) && array[5][0]!=0)
				{
					securityDelay.put(carrier, (double)array[5][1]/array[5][0]);
				}
				if(!lateDelay.containsKey(carrier) && array[6][0]!=0)
				{
					lateDelay.put(carrier, (double)array[6][1]/array[6][0]);
				}

			}
			System.out.println("arrDelay: " + arrDelay);	
			System.out.println("depDelay: " + depDelay);	
			System.out.println("carrierDelay: " + carrierDelay);	
			System.out.println("weatherDelay: " + weatherDelay);	
			System.out.println("NASDelay: " + NASDelay);	
			System.out.println("securityDelay: " + securityDelay);	
			System.out.println("lateDelay: " + lateDelay);	
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

	public static void main(String[] args) throws Exception{

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = env.addSource(new ParallelSourceFunction<String>() {
			//ArrayList<String> arr = readFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/FlightDelayPackage/2008_small.csv");
			ArrayList<String> arr = readFile("/root/flink-quickstart-java/src/main/java/FlightDelayPackage/2008_small.csv");

			boolean running = true;

			public void run(SourceContext<String> ctx) throws Exception {
				int count = -1; int total=0;
				while(running) {
					count = (count + 1)%arr.size();
					total++;
					//if(total <= max)
					if(!arr.get(count).contains("ArrDelay"))
						ctx.collect(arr.get(count));
					Thread.sleep(10);
				}
			}

			public void cancel() {
			}
		}).name("test_source");

		/*DataStream<String> text = env.addSource(new ParallelSourceFunction<String>() {

			public void run(SourceContext<String> ctx) throws Exception {
				Scanner input = new Scanner(file);
				while(input.hasNextLine()) {
					String line = input.nextLine();
					if (!line.contains("ArrDelay"))
						ctx.collect(line);
					//System.out.println(line);
					Thread.sleep(10);
				}
				input.close();
			}

			public void cancel() {
			}
		}).name("test_source");*/

		DataStream<Tuple2<String, String>> dataStream =
				text.flatMap(new Splitter1());

		DataStream<Tuple2<String, String>> dataStream2 = dataStream.keyBy(0).reduce(new DelaySum());
		DataStream<Tuple2<String, String>> dataStream3 = dataStream2.keyBy(0).reduce(new DelayRank());

		DataStream<Tuple2<String, String>> dataStreamDay =
				text.flatMap(new Splitter2());

		DataStream<Tuple2<String, String>> dataStream2Day = dataStreamDay.keyBy(0).reduce(new DayDelaySum());
		DataStream<Tuple2<String, String>> dataStream3Day = dataStream2Day.keyBy(0).reduce(new DayDelayRank());
		//dataStream.print();

		try {
			env.execute("Window FlightDelayAnalysis");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

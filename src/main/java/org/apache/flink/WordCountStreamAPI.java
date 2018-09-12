package org.apache.flink;

import java.util.Random;

import org.apache.flink.WriteIntoKafka.SimpleStringGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class WordCountStreamAPI {

	public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
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
				ctx.collect(sentence);
				Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
	public static void main(String[] args) {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> text = env.addSource(new ParallelSourceFunction<String>() {
			
			Random _rand;
		    String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
		        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
		    	    
			boolean running = true;
			
	        public void run(SourceContext<String> ctx) throws Exception {
	        	while(running) {
					_rand = new Random();
					String sentence = sentences[_rand.nextInt(sentences.length)];
					ctx.collect(sentence);
					Thread.sleep(10);
				}
	        }

	        public void cancel() {
	        }
	    }).name("test_source");
		
		//DataStream<String> text = env.addSource(new SimpleStringGenerator());
		//DataStream<String> text = env.readTextFile("/home/suchatte/workspace/flink-quickstart-java/src/main/java/org/apache/flink/data.txt");
		//DataStream<String> text = env.readTextFile("hdfs://localhost:9000/input/data.txt");
		//DataStream<String> text = env.readTextFile("hdfs://172.16.192.5:8020/flink/data.txt");
		
		DataStream<Tuple2<String, Integer>> dataStream =
				text.flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(1))
                .sum(1);
		
		dataStream.print();

        try {
			env.execute("Window WordCount");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}

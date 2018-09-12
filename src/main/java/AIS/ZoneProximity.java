package AIS;

import java.io.File;
import java.io.IOException;

//Gk5
// ./bin/flink run -p1 /root/flink-quickstart-java/target/ReadFromDynamicTopic.jar 0 100 100 0 &

// LOcal mc
// ./bin/flink run -p1 ~/workspace/flink-quickstart-java/target/ReadFromDynamicTopic.jar 0 100 100 0
import java.util.*;

import org.opengts.util.GeoPoint;
import org.opengts.util.GeoPolygon;


import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.FileUtils;
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

public class ZoneProximity {

	public static float x_1, y_1, x_2, y_2;
	public static final double R = 6372.8; // In kilometers
	public static String zonePath = "/home/suchatte/eez_extractor/data/raw/Seychellois_Exclusive_Economic_Zone.dat";
	public static List<String> coordinatesOfZone;
	public static GeoPolygon zone;
	public static HashMap<Integer, List<Integer>> groupLimit = new HashMap<Integer, List<Integer>>();
	public static HashMap<Integer, Integer> representativePointList = new HashMap<Integer, Integer>(); // Contains a representative point from every group
	public static int noOfGroups = -1;

	public static void displayHashMap(HashMap<Integer, List<Integer>> hmap)
	{
		for (int key: hmap.keySet()){
			System.out.println(key + " " + hmap.get(key));
		}
	}

	public static double[] getCoordinates(String str)
	{
		double[] arr = new double[2];
		int separator = str.indexOf(",");
		arr[0] = Double.parseDouble(str.substring(0, separator));
		arr[1] = Double.parseDouble(str.substring(separator+1));	
		return arr;
	}

	public static void breakZoneInGroups(){
		double[] point1 = new double[2];
		double[] point2 = new double[2];
		ArrayList<Integer> limit;
		double distance = 0;
		int start = 0;
		point1 = getCoordinates(coordinatesOfZone.get(start));
		boolean flag = false;
		for(int i=1;i<coordinatesOfZone.size();i++){
			point2 = getCoordinates(coordinatesOfZone.get(i));
			if((distance + getDistanceUsingHaversine(point1[0], point1[1], point2[0], point2[1])) >= 1)
			{
				noOfGroups++;
				limit = new ArrayList<Integer>();
				limit.add(start);
				limit.add(i-1);
				groupLimit.put(noOfGroups, limit);
				representativePointList.put(noOfGroups, start + ((i -1 - start)/2));
				start = i;
				point1 = getCoordinates(coordinatesOfZone.get(start));
				distance = 0;
			}
			else
			{
				distance = distance + getDistanceUsingHaversine(point1[0], point1[1], point2[0], point2[1]);
				point1 = point2;
			}
		}
		if(groupLimit.get(noOfGroups).get(1) != (coordinatesOfZone.size()-1))
		{
			noOfGroups++;
			limit = new ArrayList<Integer>();
			limit.add(start);
			limit.add(coordinatesOfZone.size()-1);
			groupLimit.put(noOfGroups, limit);
			representativePointList.put(noOfGroups, start + ((limit.get(1) - start)/2));
		}

		/*for (int key: groupLimit.keySet()){
			System.out.println(key + " " + groupLimit.get(key));
		}*/
	}

	public static void initialSetUp()
	{
		// Create GeoPolygon
		List<GeoPoint> gplist = new ArrayList<GeoPoint>();
		for(int i=0;i<coordinatesOfZone.size();i++){
			int separator = coordinatesOfZone.get(i).indexOf(",");
			gplist.add(new GeoPoint(Double.parseDouble(coordinatesOfZone.get(i).substring(0, separator)), Double.parseDouble(coordinatesOfZone.get(i).substring(separator+1))));
		}
		zone = new GeoPolygon(gplist);

		// Break the zone into groups
		breakZoneInGroups();
	}

	public static double getDistanceUsingHaversine(double lat1, double lon1, double lat2, double lon2)
	{
		double dLat = Math.toRadians(lat2 - lat1);
		double dLon = Math.toRadians(lon2 - lon1);
		lat1 = Math.toRadians(lat1);
		lat2 = Math.toRadians(lat2);

		double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
		double c = 2 * Math.asin(Math.sqrt(a));
		return R * c;
	}

	public static int getGroupId(int i, int low, int high)
	{
		if (high > low)
		{
			int mid = low + ((high -low)/2);
			if (i > groupLimit.get(mid).get(0) && i < groupLimit.get(mid).get(1))
				return i;
			else if (i < groupLimit.get(mid).get(0))
			{
				return getGroupId(i, low, mid-1);
			}
			else if (i > groupLimit.get(mid).get(1))
			{
				return getGroupId(i, mid+1, high);
			}
		}
		return -1;
	}

	public static class ProximityMonitorOptimized implements MapFunction<String, String> {
		@Override
		public String map(String record) throws Exception {
			//System.out.println(record);
			String returnValue;
			double arr[] = new double[2];
			if(!record.equals(""))
			{
				JSONObject jsonObject = new JSONObject(record);
				double latitudeOfBoat = Double.parseDouble(jsonObject.getString("x"));
				double longitudeOfBoat = Double.parseDouble(jsonObject.getString("y"));
				String boat = jsonObject.getString("mmsi");
				String time = "";
				String sog = "";
				if (jsonObject.has("tagblock_timestamp"))
					time = jsonObject.getString("tagblock_timestamp");
				if (jsonObject.has("sog"))
					sog = jsonObject.getString("sog");

				if(!zone.isPointInside(new GeoPoint(latitudeOfBoat,longitudeOfBoat)))
				{ 
					for (int groupId = 0; groupId < groupLimit.size(); groupId++){
						arr = getCoordinates(coordinatesOfZone.get(representativePointList.get(groupId)));
						double distance = getDistanceUsingHaversine(latitudeOfBoat, longitudeOfBoat, arr[0], arr[1]);
						if (distance < 100 && (!sog.isEmpty() && Float.parseFloat(sog) <= 10))
						{
							returnValue = "Boat: ".concat(boat.concat(" ")).concat(" is close to zone ").concat(zonePath)
									.concat(" at ").concat(time).concat(" with sog ").concat(sog).concat(" at a proximity of ")
									.concat(String.valueOf(distance)).concat(" km.");
							//System.out.println(returnValue);
							return returnValue;

						}
						else if((distance > 100 && distance <= 102) && (!sog.isEmpty() && Float.parseFloat(sog) <= 10)){
							int start = groupLimit.get(groupId).get(0);
							for(int i=start;i<coordinatesOfZone.size();i++){
								arr = getCoordinates(coordinatesOfZone.get(i));
								distance = getDistanceUsingHaversine(latitudeOfBoat, longitudeOfBoat, arr[0], arr[1]);
								if(distance <= 100)
								{
									returnValue = "Boat: ".concat(boat.concat(" ")).concat(" is close to zone ").concat(zonePath)
											.concat(" at ").concat(time).concat(" with sog ").concat(sog).concat(" at a proximity of ")
											.concat(String.valueOf(distance)).concat(" km.");
									System.out.println(returnValue);
									return returnValue;
								}
								else if (distance > 102)
								{
									groupId = getGroupId(i, 0, groupLimit.size()-1);
									break;
								}
							}
						}
					}
				}
			}
			return ""; 
		}
	}

	public static class ProximityMonitor implements MapFunction<String, String> {
		@Override
		public String map(String record) throws Exception {
			//System.out.println(record);
			double zoneX, zoneY;
			String returnValue;
			if(!record.equals(""))
			{
				JSONObject jsonObject = new JSONObject(record);
				double latitudeOfBoat = Double.parseDouble(jsonObject.getString("x"));
				double longitudeOfBoat = Double.parseDouble(jsonObject.getString("y"));
				String boat = jsonObject.getString("mmsi");
				String time = "";
				String sog = "";
				if (jsonObject.has("tagblock_timestamp"))
					time = jsonObject.getString("tagblock_timestamp");
				if (jsonObject.has("sog"))
					sog = jsonObject.getString("sog");
				String result = "";

				if(!zone.isPointInside(new GeoPoint(latitudeOfBoat,longitudeOfBoat)))
				{
					for(int i=0;i<coordinatesOfZone.size();i++){
						int separator = coordinatesOfZone.get(i).indexOf(",");
						zoneX = Double.parseDouble(coordinatesOfZone.get(i).substring(0, separator));
						zoneY = Double.parseDouble(coordinatesOfZone.get(i).substring(separator+1));
						double distance = getDistanceUsingHaversine(latitudeOfBoat, longitudeOfBoat, zoneX, zoneY);
						//System.out.println("(" + latitudeOfBoat + ", " + longitudeOfBoat + ") -> " + "(" + zoneX + ", " + zoneY + ") is " + distance );
						if(distance < 100)
						{
							if (!sog.isEmpty() && Float.parseFloat(sog) <= 10)
							{
								returnValue = "Boat: ".concat(boat.concat(" ")).concat(" is close to zone ").concat(zonePath)
										.concat(" at ").concat(time).concat(" with sog ").concat(sog).concat(" at a proximity of ")
										.concat(String.valueOf(distance)).concat(" km.");
								System.out.println(returnValue);
								return returnValue;
							}
						}
					}
				}
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

	public static class SplitterDataDynamic implements MapFunction<String, String> {
		@Override
		public String map(String sentence) throws Exception {
			JSONObject jsonObject = new JSONObject(sentence);
			if(jsonObject.has("_ais_type") && jsonObject.getString("_ais_type").equals("dynamic"))
			{
				return sentence;
			}
			else
				return "";
		}
	}


	public static void main(String[] args) throws Exception {
		coordinatesOfZone = FileUtils.readLines(new File(zonePath), "utf-8");
		initialSetUp();

		Properties properties = new Properties();
		//properties.setProperty("bootstrap.servers", "192.168.143.245:29092");
		properties.setProperty("bootstrap.servers", "localhost:9092");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		properties.put("auto.offset.reset", "earliest");
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>("ais", new SimpleStringSchema(), properties)).name("Kafka source");
		messageStream.getExecutionConfig().setLatencyTrackingInterval(2000L);

		messageStream.rebalance()
		.map(new MapFunction<String, String>() {

			private static final long serialVersionUID = -6867736771747690202L;
			@Override
			public String map(String value) throws Exception {
				return value;
			}
		});

		String opPath1 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/all";
		messageStream.writeAsText(opPath1, WriteMode.OVERWRITE).name("All streams");

		DataStream<String> dynamicStream = messageStream.rebalance().map(new SplitterDataDynamic());
		String opPath2 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/dynamic";
		dynamicStream.writeAsText(opPath2, WriteMode.OVERWRITE).name("Dynamic streams");

		//DataStream<String> shipRecord = dynamicStream.rebalance().map(new ProximityMonitor());
		DataStream<String> shipRecord = dynamicStream.rebalance().map(new ProximityMonitorOptimized());
		String opPath4 = "file:///home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/proximity";
		shipRecord.writeAsText(opPath4, WriteMode.OVERWRITE).name("Zone Proximity");


		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();


		}


	}

}

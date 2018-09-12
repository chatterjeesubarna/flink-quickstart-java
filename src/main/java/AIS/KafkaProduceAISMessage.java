package AIS;

// G5k
// ./bin/flink run -p1 /root/flink-quickstart-java/target/KafkaProduceAISMessage.jar &

// Local mc
// ./bin/flink run -p1 ~/workspace/flink-quickstart-java/target/KafkaProduceAISMessage.jar

import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;

import dk.tbsalling.aismessages.AISInputStreamReader;
import java.io.InputStream;

import java.io.*;

public class KafkaProduceAISMessage {

	public static String mmsi= "",   msg = "";
	static Object sog="", destination = "", x = "", y = "", metadata ="", timestamp = "";


	public static void main(String[] args) throws Exception {

		//String path = "/root/flink-quickstart-java/src/main/java/AIS/ais_orbcomm_20170301_0000.nm4";
		String path = "/home/suchatte/workspace/flink-quickstart-java/src/main/java/AIS/ais_orbcomm_20170301_0000.nm4";
		Scanner s = new Scanner(new File(path));

		// Build the configuration required for connecting to Kafka
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("batch.size", 1);
		// Serializer used for sending data to kafka. Since we are sending string,
		// we are using StringSerializer.
		props.put("key.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer",
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("producer.type", "sync");

		// Create the producer instance
		Producer<String, String> producer = new KafkaProducer<>(props);
		boolean run = true;

		while(run)
		{
			while(s.hasNext())
			{
				Utils.sleep(3);
				String sCurrentLine = s.nextLine();
				if(sCurrentLine.isEmpty())
					continue;
				sCurrentLine = sCurrentLine.substring(sCurrentLine.indexOf("!"));
				//System.out.println("line : " + sCurrentLine);
				InputStream inputStream = new ByteArrayInputStream(sCurrentLine.getBytes());
				AISInputStreamReader streamReader
				= new AISInputStreamReader(
						inputStream,
						aisMessage -> {
							//System.out.println("Received AIS message from MMSI " + aisMessage.getSourceMmsi().getMMSI() + ": " + aisMessage);
							mmsi = aisMessage.getSourceMmsi().getMMSI().toString();
							sog = aisMessage.dataFields().get("speedOverGround");
							x = aisMessage.dataFields().get("latitude");
							y = aisMessage.dataFields().get("longitude");
							metadata = aisMessage.dataFields().get("metadata");
							if(sog == null)
							{
								sog = "0.0";
							}
							if(x == null)
							{
								x = "0.0";
							}
							if(y == null)
							{
								y = "0.0";
							}
							if(metadata == null)
							{
								metadata = "";
							}
							else
							{
								timestamp = metadata.toString().substring(metadata.toString().indexOf(","));
								timestamp = timestamp.toString().substring(timestamp.toString().indexOf("=")+1).replaceAll("}", "");
							}

							destination = aisMessage.dataFields().get("destination");
							//System.out.println("received: ************** " + timestamp);;

						}
						);
				streamReader.run();


				if(destination == null || destination.toString().replaceAll("\\s", "").length() == 0)
				{
					msg = mmsi.concat(", ").concat(sog.toString().concat(", ").concat(x.toString()).concat(", ").concat(y.toString())
							.concat(", ").concat(timestamp.toString()));
					ProducerRecord<String, String> data = new ProducerRecord<>(
							"dynamic_topic", msg, msg);
					//System.out.println(msg);
					producer.send(data);
				}
				else
				{
					msg = mmsi.concat(", ").concat(sog.toString()).concat(", ").concat(destination.toString().concat(", ")
							.concat(timestamp.toString()));
					ProducerRecord<String, String> data = new ProducerRecord<>(
							"static_topic", msg, msg);
					//System.out.println(msg);
					producer.send(data);
				}

			}
			s = new Scanner(new File(path));
			
		}
		producer.close();
	}

}
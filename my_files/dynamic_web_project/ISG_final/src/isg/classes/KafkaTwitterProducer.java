package isg.classes;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class KafkaTwitterProducer {
	
	public static void getKafkaTweets(String consumerKey,	// The consumer key to get the Twitter data
			String consumerSecret,					// The consumer secret to get the Twitter data
			String token,							// The token to get the Twitter data
			String secret,							// The secret to get the Twitter data
			String[] terms,							// All the keywords to search in the tweets
			String flumeTopicName,					// The name of the Kafka topic the user wants to use
			int amountOfTweets						// The amount of tweets the user wants to get
			) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> producer = null;
		try {
			producer = new KafkaProducer<>(props);
			BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
			StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
			endpoint.trackTerms(Lists.newArrayList(terms)); // keywords
			Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
			
			Client client = new ClientBuilder()
					.hosts(Constants.STREAM_HOST)
					.endpoint(endpoint)
					.authentication(auth)
					.processor(new StringDelimitedProcessor(queue))
					.build();
			client.connect();
			
			for(int i = 0; i < amountOfTweets; i++) {	// max number of tweets that we want
				String msg = queue.take();
				producer.send(new ProducerRecord<String, String>(flumeTopicName, msg));
			}
			producer.close();
			client.stop();
		}
		catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static String giveKafkaClass(String consumerKey,	// The consumer key to get the Twitter data
			String consumerSecret,					// The consumer secret to get the Twitter data
			String token,							// The token to get the Twitter data
			String secret,							// The secret to get the Twitter data
			String[] terms,							// All the keywords to search in the tweets
			String flumeTopicName,					// The name of the Kafka topic the user wants to use
			int amountOfTweets						// The amount of tweets the user wants to get
			) {
		String termsString = "\""+terms[0]+"\"";
		for(int i=1; i<terms.length; i++) {
			termsString+=", \""+terms[i]+"\"";
		}
		
		String result = "";
		result += ""
				+ "import java.util.Properties;\r\n"
				+ "import java.util.concurrent.BlockingQueue;\r\n"
				+ "import java.util.concurrent.LinkedBlockingQueue;\r\n"
				+ "\r\n"
				+ "import org.apache.kafka.clients.producer.KafkaProducer;\r\n"
				+ "import org.apache.kafka.clients.producer.Producer;\r\n"
				+ "import org.apache.kafka.clients.producer.ProducerRecord;\r\n"
				+ "\r\n"
				+ "import com.google.common.collect.Lists;\r\n"
				+ "import com.twitter.hbc.ClientBuilder;\r\n"
				+ "import com.twitter.hbc.core.Client;\r\n"
				+ "import com.twitter.hbc.core.Constants;\r\n"
				+ "import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;\r\n"
				+ "import com.twitter.hbc.core.processor.StringDelimitedProcessor;\r\n"
				+ "import com.twitter.hbc.httpclient.auth.Authentication;\r\n"
				+ "import com.twitter.hbc.httpclient.auth.OAuth1;\r\n"
				+ ""
				+ "public class KafkaTwitterProducer{\r\n"
				+ "\r\n"
				+ "     public static void main(String[] args) {\r\n"
				+ "		Properties props = new Properties();\r\n"
				+ "		props.put(\"bootstrap.servers\", \"localhost:9092\");\r\n"
				+ "		props.put(\"acks\", \"all\");\r\n"
				+ "		props.put(\"retries\", 0);\r\n"
				+ "		props.put(\"batch.size\", 16384);\r\n"
				+ "		props.put(\"linger.ms\", 1);\r\n"
				+ "		props.put(\"buffer.memory\", 33554432);\r\n"
				+ "		props.put(\"key.serializer\", \"org.apache.kafka.common.serialization.StringSerializer\");\r\n"
				+ "		props.put(\"value.serializer\", \"org.apache.kafka.common.serialization.StringSerializer\");\r\n"
				+ "		\r\n"
				+ "		Producer<String, String> producer = null;\r\n"
				+ "		try {\r\n"
				+ "			producer = new KafkaProducer<>(props);\r\n"
				+ "			String consumerKey = \""+consumerKey+"\";\r\n"
				+ "			String consumerSecret = \""+consumerSecret+"\";\r\n"
				+ "			String token = \""+token+"\";\r\n"
				+ "			String secret = \""+secret+"\";\r\n"
				+ "			BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);\r\n"
				+ "			StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();\r\n"
				+ "			endpoint.trackTerms(Lists.newArrayList("+termsString+")); // keywords\r\n"
				+ "			Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);\r\n"
				+ "			\r\n"
				+ "			Client client = new ClientBuilder()\r\n"
				+ "					.hosts(Constants.STREAM_HOST)\r\n"
				+ "					.endpoint(endpoint)\r\n"
				+ "					.authentication(auth)\r\n"
				+ "					.processor(new StringDelimitedProcessor(queue))\r\n"
				+ "					.build();\r\n"
				+ "			client.connect();\r\n"
				+ "			\r\n"
				+ "			for(int i = 0; i < "+amountOfTweets+"; i++) {	// max number of tweets that we want\r\n"
				+ "				String msg = queue.take();\r\n"
				+ "				producer.send(new ProducerRecord<String, String>(\""+flumeTopicName+"\", msg));\r\n"
				+ "				System.out.println(\"Sent: \"+msg);\r\n"
				+ "			}\r\n"
				+ "			producer.close();\r\n"
				+ "			client.stop();\r\n"
				+ "		}\r\n"
				+ "		catch(Exception e){\r\n"
				+ "			System.out.println(e);\r\n"
				+ "		}\r\n"
				+ "	}\r\n"
				+ "}";
		return result;
	}
	
	public static void saveClass(String strClass) 
			throws IOException {
		CommandLine.createJavaClass(strClass, "KafkaTwitterProducer");
	}
}

package isg.classes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

public class Preprocess {
	
	// This function will go to the HDFS and return all the tweets texts
	public static String[] holdOnlyTweets(String topic) 
			throws IOException {
		ArrayList<String> list = new ArrayList<String>();
		Configuration conf = new Configuration();
		conf.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\core-site.xml"));
		conf.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\hdfs-site.xml"));

		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String filePath = "hdfs://localhost:9000/"+topic+"/avro/desertweets.txt";

		Path path = new Path(filePath);
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream inputStream = fs.open(path);
		String line = null;
		while((line = inputStream.readLine()) != null) {
			JSONObject json = new JSONObject(line);
			list.add(json.getString("tweet"));
		}
		fs.close();
		
		String[] results = new String[list.size()];
		for(int i=0; i<list.size(); i++) {
			results[i] = list.get(i);
		}
		return results;
	}
	
	// this function returns an array with the amount of times each keyword exists in each tweet
	public static Integer[][] arrayTweetsBasedOnTerms(String[] tweets, String[] keywords){
		Integer[][] results = new Integer[tweets.length][keywords.length];
		
		// give to all positions the zero value as a starting value
		for(int i=0; i<tweets.length; i++) {
			for(int j=0; j<keywords.length; j++) {
				results[i][j] = 0;
			}
			
		}
		
		// for each tweet
		for(String tweet : tweets) {
			String[] words = new String[0];
			if(!tweet.equals("")) words = tweet.split(" ");
			// for each word inside of it
			for(String word : words) {
				// check if this word is one of the keywords
			}
		}
		
		// for each tweet
		for(int i=0; i<tweets.length; i++) {
			String tweet = tweets[i];
			String[] words = new String[0];
			if(!tweet.equals("")) words = tweet.split(" ");
			// for each word inside of it
			for(int j=0; j<words.length; j++) {
				String word = words[j];
				// check if this word is one of the keywords
				for(int z=0; z<keywords.length; z++) {
					if(word.equals(keywords[z])) {
						//if it is one of the keywords then sum it with the rest and then go to the next word
						results[i][z]++;
						break;
					}
				}
			}
		}
		
		// now i know how many times exist each keyword in each tweet
		return results;
	}
	
	// saves the output of arrayTweetsBasedOnTerms method to the HDFS
	public static void savePreDataToHDFS(String topic, Integer[][] data, int amountOfTweets, int amountOfTerms) 
			throws IOException {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fileSystem = FileSystem.get(configuration);
		//Create a path
		String fileName = "/"+topic+"/preprocessed/preprocessed.dat";
		
		Path hdfsWritePath = new Path(fileName);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
		BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
		/** Output to the HDFS **/
		for(int i=0; i<amountOfTweets; i++) {
			for(int j=0; j<amountOfTerms-1; j++) {
				bufferedWriter.write(data[i][j]+" ");
			}
			bufferedWriter.write(data[i][amountOfTerms-1]+"");
			bufferedWriter.newLine();
		}
		
		bufferedWriter.close();
		fileSystem.close();
	}

}

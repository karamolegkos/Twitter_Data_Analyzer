package isg.classes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONObject;

public class MRMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
	
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
    {    
    	String jsonLine = value.toString();
    	
    	JSONObject jsonObject = new JSONObject(jsonLine);
    	String tweet = jsonObject.getString("tweet");
    	tweet = tweet.toLowerCase();
    	
    	String[] terms = MRRunner.Jobterms;
    	
    	if(!tweet.equals("")) {
    		String[] words = tweet.split(" ");
    		// gives 1 for every searching term in the tweet
    		for(String word : words) {
    			for(String term : terms) {
    				if(word.equals(term)) {
    					output.collect(new Text(word), new IntWritable(1));
    					break;
    				}
    			}
    		}
    	}
    }
}

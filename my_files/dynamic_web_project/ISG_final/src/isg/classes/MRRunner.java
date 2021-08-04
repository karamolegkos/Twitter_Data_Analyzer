package isg.classes;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MRRunner {
	
	public static String JobTopic;
	public static String[] Jobterms;
	
	public MRRunner(String topic, String[] terms) {
		MRRunner.JobTopic = topic;
		MRRunner.Jobterms = terms;
	}
	
	public static void doMapReduceJob() throws IOException {
		
		// Create a file to save the deserialized tweets.
		String strFile = "C:\\TwitterAnalyzer\\deser_tweets.txt";
		File file = new File(strFile); 
    	if(file.exists()) file.delete();
    	file.createNewFile();
    	PrintWriter writer = new PrintWriter(strFile, "UTF-8");
    	
    	// Copy the HDFS file inside the new file
    	Configuration configuration = new Configuration();
    	configuration.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\core-site.xml"));
    	configuration.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\hdfs-site.xml"));

		String hdfsFile = "hdfs://localhost:9000/"+MRRunner.JobTopic+"/avro/desertweets.txt";

		Path path = new Path(hdfsFile);
		FileSystem fs = path.getFileSystem(configuration);
		FSDataInputStream inputStream = fs.open(path);
		String line = null;
		int a = 0;
		while((line = inputStream.readLine()) != null) {
			writer.println(line);
		}
		fs.close();
		writer.close();
    	
		// Start the job.
		JobConf conf = new JobConf(MRRunner.class); 
		conf.setJobName(MRRunner.JobTopic);
       
		conf.setOutputKeyClass(Text.class);    
		conf.setOutputValueClass(IntWritable.class);
                  
		conf.setMapperClass(MRMapper.class);    
		conf.setCombinerClass(MRReducer.class);   
		conf.setReducerClass(MRReducer.class); 
       
		conf.setInputFormat(TextInputFormat.class);    
		conf.setOutputFormat(TextOutputFormat.class);   
        
		String arguments[] = new String[2];
		arguments[0] = strFile; 													//Input file
		arguments[1] = "hdfs://localhost:9000/"+MRRunner.JobTopic+"/mapreducejob"; 	//Output directory      
		FileInputFormat.setInputPaths(conf, new Path(arguments[0]));    
		FileOutputFormat.setOutputPath(conf, new Path(arguments[1]));     
	   
		try{
			JobClient.runJob(conf);
			System.out.println("Job was successful");
		}
		catch (Exception e){
			e.printStackTrace();
			System.out.println("Job was not successful");
		}
	}
	
	// returns a mapreduce file from HDFS
	public static String getHDFSFileContent(String topic) 
			throws IOException {
		String result = "";
		Configuration conf = new Configuration();
		conf.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\core-site.xml"));
		conf.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\hdfs-site.xml"));

		String filePath = "hdfs://localhost:9000/"+topic+"/mapreducejob/part-00000";

		Path path = new Path(filePath);
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream inputStream = fs.open(path);
		String line = null;
		int a = 0;
		while((line = inputStream.readLine()) != null) {
			result+=line+"<br>";
		}
		fs.close();
		return result;
	}

}

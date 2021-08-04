package isg.classes;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

public class AvroClass {
	
	public static void avroDeserialize(String topic) 
			throws IllegalArgumentException, IOException {
		// my files
		String avroFile = "hdfs://localhost:9000/"+topic+"/avro/avrotweets.avro";
		String schemaFile = "C:/TwitterAnalyzer/twitter.avsc";
		// using "/" in the start of the path will ensure to get the exact path that I want in HDFS
		String deserFile = "/"+topic+"/avro/desertweets.txt";
		
		// deserialize configuration
		Configuration conf = new Configuration();
		FsInput in = new FsInput(new Path(avroFile), conf);
		
		Schema schema = new Schema.Parser().parse(new File(schemaFile));
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(in, datumReader);
		GenericRecord em = null;
		
		// write on HDFS configuration
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fileSystem = FileSystem.get(configuration);
		
		Path hdfsWritePath = new Path(deserFile);
		FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
		BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
		
		while(dataFileReader.hasNext()) {
			em = dataFileReader.next(em);
			/** Output the results to the HDFS **/
			bufferedWriter.write(em.toString());
			bufferedWriter.newLine();
		}
		// close the HDFS connections
		bufferedWriter.close();
		fileSystem.close();
	}
	
	//builds an AVRO schema for the Twitter tweets
	//the schema will be in the path C:\TwitterAnalyzer\twitter.avsc
	public static void buildSchema() 
			throws IOException {
		String schema = "{\r\n"
				+ "  \"type\" : \"record\",\r\n"
				+ "  \"name\" : \"Tweet\",\r\n"
				+ "  \"namespace\" : \"com.miguno.avro\",\r\n"
				+ "  \"fields\" : [ {\r\n"
				+ "    \"name\" : \"username\",\r\n"
				+ "    \"type\" : \"string\",\r\n"
				+ "    \"doc\"  : \"Name of the user account on Twitter.com\"\r\n"
				+ "  }, {\r\n"
				+ "    \"name\" : \"tweet\",\r\n"
				+ "    \"type\" : \"string\",\r\n"
				+ "    \"doc\"  : \"The content of the user's Twitter message\"\r\n"
				+ "  }, {\r\n"
				+ "    \"name\" : \"timestamp\",\r\n"
				+ "    \"type\" : \"long\",\r\n"
				+ "    \"doc\"  : \"Unix epoch time in seconds\"\r\n"
				+ "  } ],\r\n"
				+ "  \"doc\" : \"A basic schema for storing Twitter messages\"\r\n"
				+ "}";
		
		File dir = new File("C:\\TwitterAnalyzer");
		if(!dir.exists()) {
			dir.mkdir();
		}
		
		// Get the file 
		File dest = new File("C:\\TwitterAnalyzer\\twitter.avsc"); 

		// Delete the file in case it is modified.
		if (dest.exists()) dest.delete();
		
		dest.createNewFile();
		FileWriter fr = new FileWriter(dest, true); // parameter 'true' is for append mode
		fr.write(schema);
		fr.close();
	}
	
	//takes a fileName and the topic that it is inside of and then serializes it in a parallel directory
	public static void avroSerialize(String topic, String fileName) 
			throws IOException {
		// Getting ready to AVRO
		buildSchema();	
		
		String schemaFile = "C:/TwitterAnalyzer/twitter.avsc";
		String avroFile = "hdfs://localhost:9000/"+topic+"/avro/avrotweets.avro";
		
		Schema schema = new Schema.Parser().parse(new File(schemaFile));
		
		
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(avroFile), conf);
		OutputStream out = fs.create(new Path(avroFile));
		dataFileWriter.create(schema, out);
		
		
		// Getting the non Avro file
		Configuration confi = new Configuration();
		confi.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\core-site.xml"));
		confi.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\hdfs-site.xml"));
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String nonAvroFile = "hdfs://localhost:9000/"+topic+"/mytweets/"+fileName;
		
		
		Path path = new Path(nonAvroFile);
		FileSystem fileSystem = path.getFileSystem(confi);
		FSDataInputStream inputStream = fileSystem.open(path);
		String line = null;
		int a = 0;
		while((line = inputStream.readLine()) != null) {
			if(!line.equals("")) {
				// appending
				JSONObject jsonObject = new JSONObject(line);
				String userName = "-";
				String text = "-";
				long timestamp = -1L;
				if(jsonObject.has("user")) {
					userName = jsonObject.getJSONObject("user").getString("name");
				}
				if(jsonObject.has("text")) {
					text = jsonObject.getString("text");
				}
				if(jsonObject.has("timestamp_ms")) {
					timestamp = Long.parseLong(jsonObject.getString("timestamp_ms"));
				}
				GenericRecord gr = new GenericData.Record(schema);
				gr.put("username",userName);
				gr.put("tweet",text);
				gr.put("timestamp",timestamp);
				dataFileWriter.append(gr);
			}
		}
		dataFileWriter.close();
		fileSystem.close();
	}
	
	// returns a file with tweets from HDFS
	public static String getHDFSFileContent(String topic, String dir, String fileName) 
			throws IOException {
		String result = "";
		Configuration conf = new Configuration();
		conf.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\core-site.xml"));
		conf.addResource(new Path("C:\\hadoop-3.1.0\\etc\\hadoop\\hdfs-site.xml"));

		String filePath = "hdfs://localhost:9000/"+topic+"/"+dir+"/"+fileName;

		Path path = new Path(filePath);
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream inputStream = fs.open(path);
		String line = null;
		int a = 0;
		while((line = inputStream.readLine()) != null) {
			if(!line.equals("")) {
				result+="TWEET NUMBER "+ (++a)+"<br>";
				result+=line+"<br><br>";
			}
		}
		fs.close();
		return result;
	}

}

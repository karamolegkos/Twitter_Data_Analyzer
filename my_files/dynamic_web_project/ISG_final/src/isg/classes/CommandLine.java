package isg.classes;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class CommandLine {

	// A function to only open a CMD
	public static void openCMD() {
		try
        {
           Runtime.getRuntime().exec(new String[] {"cmd", "/K", "Start"});
  
        }
        catch (Exception e)
        {
            System.out.println("Something is going wrong!");
            e.printStackTrace();
        }
	}
	
	// A function to open a CMD with some starting commands
	public static void execCommand(String command)	// it could be "dir && ping localhost" // many all together in the start
    {
        try
        { 
         Runtime.getRuntime().exec("cmd /c start cmd.exe /K \""+command+"\"");
        }
        catch (Exception e)
        {
        	System.out.println("Something is going wrong!");
            e.printStackTrace();
        }
    }
	
	// A function to make the whole program wait for some time
	// This function will be used to wait for some CMD commands to run before executing the next command
	public static void waitForSeconds(int amount) 
			throws InterruptedException {
		TimeUnit.SECONDS.sleep(amount);
	}
	
	// The function below will run two CMD windows with the zookeeper and Kafka running
	public static void runKafka() 
			throws InterruptedException {
		String firstCommand = "cd %KAFKA_HOME%";
		String secondCommand = ".\\bin\\windows\\zookeeper-server-start.bat .\\config\\zookeeper.properties";
		CommandLine.execCommand(firstCommand + " && " + secondCommand);
		
		CommandLine.waitForSeconds(10);
		
		firstCommand = "cd %KAFKA_HOME%";
		secondCommand = ".\\bin\\windows\\kafka-server-start.bat .\\config\\server.properties";
		CommandLine.execCommand(firstCommand + " && " + secondCommand);
	}
	
	// This function will create a flume agent in the right directory of the user's system and then run it
	public static void runFlumeAgent(String topic) 
			throws IOException {
		createFlumeAgent(topic);	
		
		String firstCommand = "cd %FLUME_HOME%";
		String secondCommand = "bin\\flume-ng agent --conf .\\conf -f conf\\twitter_flume.conf -property \"flume.root.logger=info,console\" -n KafkaAgent";
		CommandLine.execCommand(firstCommand + " && " + secondCommand);
	}
	
	// builds the flumeAgent from the lib files of this project inside the user's system
	public static void createFlumeAgent(String topic) 	// Gets the kafka topic to use it as a name for HDFS
			throws IOException {
		// Get the file 
		File dest = new File("C:\\apache-flume-1.9.0-bin\\conf\\twitter_flume.conf"); 

		// Delete the file in case it is modified.
		if (dest.exists()) dest.delete();
		
		String inputString = "";
		inputString += "KafkaAgent.sources  = source1\r\n"
				+ "KafkaAgent.channels = channel1\r\n"
				+ "KafkaAgent.sinks = sink1\r\n"
				+ "\r\n"
				+ "KafkaAgent.sources.source1.type = org.apache.flume.source.kafka.KafkaSource\r\n"
				+ "KafkaAgent.sources.source1.kafka.bootstrap.servers = localhost:9092\r\n"
				+ "KafkaAgent.sources.source1.kafka.topics = "+topic+"\r\n"
				+ "KafkaAgent.sources.source1.kafka.consumer.group.id = flume\r\n"
				+ "KafkaAgent.sources.source1.channels = channel1\r\n"
				+ "KafkaAgent.sources.source1.interceptors = i1\r\n"
				+ "KafkaAgent.sources.source1.interceptors.i1.type = timestamp\r\n"
				+ "KafkaAgent.sources.source1.kafka.consumer.timeout.ms = 100\r\n"
				+ "\r\n"
				+ "KafkaAgent.channels.channel1.type = memory\r\n"
				+ "KafkaAgent.channels.channel1.capacity = 10000\r\n"
				+ "KafkaAgent.channels.channel1.transactionCapacity = 1000\r\n"
				+ "\r\n"
				+ "KafkaAgent.sinks.sink1.type = hdfs\r\n"
				+ "KafkaAgent.sinks.sink1.hdfs.path = hdfs://localhost:9000/"+topic+"/mytweets\r\n"
				+ "KafkaAgent.sinks.sink1.hdfs.rollInterval = 0\r\n"
				+ "KafkaAgent.sinks.sink1.hdfs.rollSize = 0\r\n"
				+ "KafkaAgent.sinks.sink1.hdfs.rollCount = 0\r\n"
				+ "KafkaAgent.sinks.sink1.hdfs.fileType = DataStream\r\n"
				+ "KafkaAgent.sinks.sink1.channel = channel1";
		
		dest.createNewFile();
		FileWriter fr = new FileWriter(dest, true); // parameter 'true' is for append mode
		fr.write(inputString);
		fr.close();
	}
	
	// Save a java Class in the directory C:/TwitterAnalyzer
	public static void createJavaClass(String strClass, String className) 
			throws IOException {
		File dir = new File("C:\\TwitterAnalyzer");
		if(!dir.exists()) {
			dir.mkdir();
		}
		
		// Get the file 
		File dest = new File("C:\\TwitterAnalyzer\\"+className+".java"); 

		// Delete the file in case it is modified.
		if (dest.exists()) dest.delete();
		
		dest.createNewFile();
		FileWriter fr = new FileWriter(dest, true); // parameter 'true' is for append mode
		fr.write(strClass);
		fr.close();
	}
	
	// This function will go and start all the needed scripts for the HDFS and YARN servers to run
	public static void runHDFSYARN() {
		String firstCommand = "cd %HADOOP_HOME%/sbin";
		String secondCommand = "start-all.cmd";
		String thirdCommand = "exit";
		CommandLine.execCommand(firstCommand + " && " + secondCommand + " && " + thirdCommand);
	}
	
	// Adds a directory inside HDFS
	// To be used, the "runHDFSYARN" function must be properly used before this one!
	public static void addHDFSDir(String dirPath) {
		// dirPath is the whole path to the new directory
		// without the root slash (the input is like this: dir/dir/dir/newDir)
		// it could be "twitterdata"
		dirPath = "/"+dirPath;
		String firstCommand = "cd %HADOOP_HOME%/sbin";
		String secondCommand = "hdfs dfsadmin -safemode leave";
		String thirdCommand = "hdfs dfs -mkdir "+dirPath;
		String forthCommand = "exit";
		CommandLine.execCommand(firstCommand + " && " +secondCommand+" && "+ thirdCommand + " && " + forthCommand);
	}
}

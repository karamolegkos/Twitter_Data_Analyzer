package isg.classes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class ClusteringClass {
	
	public static String getResults() 
			throws FileNotFoundException {
		String results = "";
		File myObj = new File("C:\\TwitterAnalyzer\\Mahout.txt");
	    Scanner myReader = new Scanner(myObj);
	    while (myReader.hasNextLine()) {
	      String data = myReader.nextLine();
	      results += data+"<br>";
	    }
	    myReader.close();
	    return results;
	}

	public static void kMeans(String topic, int testID, int numberOfClusters, int maxIterations, double convergenceDelta, String measurePref) 
			throws Exception {
		// Creating a File object that represents the disk file.
		String mahoutOutPut = "C:\\TwitterAnalyzer\\Mahout.txt";
		File mahoutOutPutFile = new File(mahoutOutPut);
		if (mahoutOutPutFile.exists()) mahoutOutPutFile.delete();
		mahoutOutPutFile.createNewFile();
        PrintStream o = new PrintStream(mahoutOutPutFile);
        
        // Store current System.out before assigning a new value
        PrintStream console = System.out;
        
        // Assign o to output stream
        System.setOut(o);
        
		String inputStr = "hdfs://localhost:9000/"+topic+"/preprocessed";
		String outPutDir = "hdfs://localhost:9000/"+topic+"/clustering/clustered"+testID;
		
		Path input = new Path(inputStr);
		Path output = new Path(outPutDir);
		
		Configuration conf = new Configuration();
		HadoopUtil.delete(conf, output);
		
		DistanceMeasure measure = null;
		
		if(measurePref.equals("Euclidean distance")) measure = new EuclideanDistanceMeasure();
		if(measurePref.equals("Squared Euclidean distance")) measure = new SquaredEuclideanDistanceMeasure();
		if(measurePref.equals("Cosine distance")) measure = new CosineDistanceMeasure();
		if(measurePref.equals("Manhattan distance")) measure = new ManhattanDistanceMeasure();
		if(measurePref.equals("Tanimoto distance")) measure = new TanimotoDistanceMeasure();
		
		run(conf, input, output, measure, numberOfClusters, convergenceDelta, maxIterations);
		
		// Use stored value for output stream
        System.setOut(console);
	}
	
	public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k, double convergenceDelta, int maxIterations) 
			throws Exception {
		Path directoryContainingConvertedInput = new Path(output, "KmeansOutputData");
		InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
		
		Path cluster = new Path(output, "random-seeds");
		cluster = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, cluster, k, measure);
		
		KMeansDriver.run(conf, directoryContainingConvertedInput, cluster, output, convergenceDelta, maxIterations, true, 0.0, false);
		
		Path outGlob = new Path(output, "clusters-*-final");
		Path clusteredPoints = new Path(output, "clusteredPoints");
		
		ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
		clusterDumper.printClusters(null);
	}
}

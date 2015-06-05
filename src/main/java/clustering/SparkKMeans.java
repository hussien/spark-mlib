package clustering;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;



public class SparkKMeans {


	public static JavaRDD<Vector> loadData(JavaRDD<String> data){
		
		JavaRDD<Vector> parsedData = data.map(
				new Function<String, Vector>() {
					public Vector call(String s) {
						String[] sarray = s.split(" ");
						double[] values = new double[sarray.length];
						for (int i = 0; i < sarray.length; i++)
							values[i] = Double.parseDouble(sarray[i]);
						return Vectors.dense(values);
					}
				}
				);

		return parsedData;

	}

	
	public static JavaRDD<Vector> loadPoint(JavaSparkContext sc, List<Double[]> pointList ){
		JavaRDD<Vector> parsedData = sc.parallelize(pointList).map(
				new Function<Double[], Vector>() {
					public Vector call(Double[] d) {
						double[] values = new double[d.length];
						for (int i = 0; i < d.length; i++)
							values[i] = d[i];
						return Vectors.dense(values);
					}
				}
				);

		return parsedData;

	}
	
	
	public static List<double[]> getCenters(JavaSparkContext sc, JavaRDD<Vector> parsedData, int numClusters, int numIterations){

		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		List<double[]> newList =new ArrayList<double[]>();
		System.out.print("center(s): ");
		for(Vector k: clusters.clusterCenters()){
			newList.add(k.toArray());
		}
		return newList;
		

	}
	
	


	public static void run(JavaSparkContext sc, JavaRDD<Vector> parsedData, int numClusters, int numIterations){

		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);
		
		System.out.print("center(s): ");
		for(Vector k: clusters.clusterCenters()){
			System.out.print(k.toString()+" , ");
		}
		
		System.out.println();
		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
	}
	
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("K-means Example").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
        List <Double[]> points =new ArrayList<Double[]>(); 
		// Load and parse data
		String path = "data/kmeans_data.txt";
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<Vector> parsedData = loadData(data);
		
		parsedData.cache();
		Double[] a = {1.0, 3.0};
		Double[] b = {2.0, 4.0};
		points.add(a);
		points.add(b);
		JavaRDD<Vector> pointData = loadPoint(sc, points);

		// Cluster the data into two classes using KMeans
		int numClusters = 2;
		int numIterations = 20;
		
		run(sc, pointData, 2, 10);
	
	}

}

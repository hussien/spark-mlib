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
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.clustering.GaussianMixture;

import util.DataUtil;

public class SparkGaussianMixture {

	public static List<double[]> getCenters(JavaSparkContext sc,
			JavaRDD<Vector> parsedData, int numClusters, int numIterations) {

		parsedData.cache();
		GaussianMixtureModel gmm = new GaussianMixture().setK(numClusters)
				.setMaxIterations(numIterations).run(parsedData.rdd());

		List<double[]> newList = new ArrayList<double[]>();
		for (int j = 0; j < gmm.k(); j++) {
			newList.add(gmm.gaussians()[j].mu().toArray());
		}
		return newList;

	}

	public static void run(JavaSparkContext sc, JavaRDD<Vector> parsedData,
			int numClusters, int numIterations) {

		parsedData.cache();
		GaussianMixtureModel gmm = new GaussianMixture().setK(numClusters)
				.setMaxIterations(numIterations).run(parsedData.rdd());

		// Output the parameters of the mixture model
		for (int j = 0; j < gmm.k(); j++) {
			System.out.println(gmm.gaussians()[j].mu());
		}
		
		for (int j = 0; j < gmm.k(); j++) {
			System.out.println(gmm.gaussians()[j].sigma());
			
		}
		
		
	
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GaussianMixture Example")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load and parse data
		String path = "data/gmm_data.txt";
		JavaRDD<String> data = sc.textFile(path);
		run(sc, DataUtil.loadData(data), 2, 10);


	}

}

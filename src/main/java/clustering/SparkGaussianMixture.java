package clustering;

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

public class SparkGaussianMixture {
	

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("GaussianMixture Example").setMaster("local");
	    JavaSparkContext sc = new JavaSparkContext(conf);

	    // Load and parse data
	    String path = "data/gmm_data.txt";
	    JavaRDD<String> data = sc.textFile(path);
	    JavaRDD<Vector> parsedData = data.map(
	      new Function<String, Vector>() {
	        public Vector call(String s) {
	          String[] sarray = s.trim().split(" ");
	          double[] values = new double[sarray.length];
	          for (int i = 0; i < sarray.length; i++)
	            values[i] = Double.parseDouble(sarray[i]);
	          return Vectors.dense(values);
	        }
	      }
	    );
	    parsedData.cache();

	    // Cluster the data into two classes using GaussianMixture
	    GaussianMixtureModel gmm = new GaussianMixture().setK(2).run(parsedData.rdd());

	    // Output the parameters of the mixture model
	    for(int j=0; j<gmm.k(); j++) {
	        System.out.println(gmm.gaussians()[j].mu());
	    }
	  }

}

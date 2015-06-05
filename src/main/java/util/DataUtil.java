package util;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class DataUtil {

	public static JavaRDD<Vector> loadData(JavaRDD<String> data){

		JavaRDD<Vector> parsedData = data.map(
				new Function<String, Vector>() {
					public Vector call(String s) {
						String[] sarray = s.trim().split(" ");
						double[] values = new double[sarray.length];
						for (int i = 0; i < sarray.length; i++){
							values[i] = Double.parseDouble(sarray[i].trim());
						}
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




}

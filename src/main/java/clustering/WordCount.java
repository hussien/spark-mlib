package clustering;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import java.util.regex.Pattern;

public class WordCount {

	private static final Pattern SPACE = Pattern.compile("\\W");

	public static boolean isAlpha(String name) {
		char[] chars = name.toCharArray();

		for (char c : chars) {
			if(!Character.isLetter(c)) {
				return false;
			}
		}

		return true;
	}

	public static void main(String[] args) {




		SparkConf conf = new SparkConf().setAppName("WordCount Example").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);


		// Load and parse the data
		String path = "data/*.txt";
		//JavaPairRDD<String,String> data = sc.wholeTextFiles(path);
		System.out.println("start collecting words...");
		//JavaPairRDD<String,String> files = sc.wholeTextFiles(path);
		JavaRDD<String> lines = sc.textFile(path);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		}).filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return (isAlpha(s)&&s.length()>3); }
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<String, Integer>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		JavaPairRDD<Integer,String> counts_sorted = counts.mapToPair(
				new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
						return t.swap();
					}
				}).sortByKey();


		//List<Tuple2<String, Integer>> output = counts.collect();
		List<Tuple2<Integer, String>> output = counts_sorted.collect();
		List<Tuple2<Integer, String>> output_pruned = output.subList(0, output.size()-10);
		
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		sc.stop();

	}

}

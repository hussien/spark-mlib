package spark;

import java.util.Arrays;
//import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.mutable.HashMap;


public class TestLda {

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
		SparkConf conf = new SparkConf().setAppName("LDA Example").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load and parse the data
		String path = "data/*.txt";

		JavaPairRDD<String,String> data = sc.wholeTextFiles(path);

		/*List<Tuple2<String, String>> output = data2.collect();

		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}*/

		JavaRDD<String> lines = sc.textFile(path);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		}).filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return (isAlpha(s)&&s.length()>4); }
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		
		// for pruning stop words, will implement later
		/*	JavaPairRDD<Integer,String> counts_sorted = counts.mapToPair(
				new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
						return t.swap();
					}
				}).sortByKey();*/

		final Map<String, Long> indice = counts.keys().zipWithIndex().collectAsMap();
		
		Map<Long,String> reverse = counts.keys().zipWithIndex().mapToPair(
				new PairFunction<Tuple2<String, Long>, Long, String>() {
					public Tuple2<Long, String> call(Tuple2<String, Long> t) {
						return t.swap();
					}
				}).collectAsMap();
		/*		for(Entry<String, Long> e:indice.entrySet())
		{
			System.out.println(e.getKey()+":"+e.getValue());
		}*/

		JavaPairRDD<Long,Vector> parsedData = data.values().zipWithIndex().mapToPair(
				new PairFunction<Tuple2<String, Long>, Long, Vector>() {
					public Tuple2<Long, Vector> call(Tuple2<String, Long> t) {
						String[] sarray = t._1.trim().split("\\W");
						HashMap<Object, Object> counts = new HashMap<Object, Object>();		
						for (int i = 0; i < sarray.length; i++){
							if(indice.containsKey(sarray[i])){
								int index = indice.get(sarray[i]).intValue();
								if(counts.contains(index))
									counts.put(index, (Double)counts.get(index).get()+1.0);
								else
									counts.put(index, 0.0);
							}
						}
						return new Tuple2<Long, Vector>(t._2, Vectors.sparse(indice.size(), counts.toSeq()));
					}
				}
				);

		parsedData.cache();

		DistributedLDAModel ldaModel = new LDA().setK(4).run(parsedData);

		System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
				+ " words):");

		/*Matrix topics = ldaModel.topicsMatrix();
		for (int topic = 0; topic < 3; topic++) {
			System.out.print("Topic " + topic + ":");
			for (int word = 0; word < ldaModel.vocabSize(); word++) {
				System.out.print(" " + topics.apply(word, topic));
			}
			System.out.println();
		}*/
		System.out.println("*********************************************************");
		
		
		for (Tuple2<int[],?> tuple : ldaModel.describeTopics()) {
			for(int i:tuple._1)
				System.out.print(reverse.get((long)i)+",");
			System.out.println();
		}
		
	




		sc.stop();
	}


}

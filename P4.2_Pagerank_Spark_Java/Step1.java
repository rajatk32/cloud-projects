import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class Step1 {

	public static void main(String[] args) {
		
		// initialize variables for master's address, the location of jar
		// which is required for spark to distribute across the cluster
		// and the HDFS/S3 input path:
		String masterAddr = "spark://ec2-52-6-24-63.compute-1.amazonaws.com:7077";
		String jarLocation = "/root/Step-1.jar";
		String hdfsInputPath = "s3n://s15-p42-part1-easy/data/";
		final String searchTerm = "cloud";	// this is the search term
		final long N;		// stores the total number of distinct titles
		
		// Initialize spark context:
		SparkConf sparkConf = new SparkConf().setAppName("Step1").setMaster(masterAddr);
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		jsc.addJar(jarLocation);
		JavaRDD<String> file = jsc.textFile(hdfsInputPath);
		
		// Create a JavaRDD which stores all titles:
		JavaRDD<String> titles = file.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String line) throws Exception {
				ArrayList<String> title = new ArrayList<String>(1);
				String[] split = line.split("\t");
				title.add(split[1]);
				return title;
			}
			
		});
		
		N = titles.distinct().count();	// count the total no. of distinct
										// titles
		
		// Split all text into words and replace extraneous characters 
		// with a space as given in the project description
		// Store the words in the form (word,title) as flatmap:
		JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {

			@Override
			public Iterable<String> call(String line) throws Exception {
				String[] split = line.split("\t");
				String title = split[1];
				String text = split[3];
				text = text.replaceAll("[<](.*?)[>]", " ");
				text = text.toLowerCase();
				text = text.replace("\\n", " ");
				text = text.replaceAll("[^\\p{L}]+", " ");
				text = text.trim();
				ArrayList<String> word = new ArrayList<String>();
				StringTokenizer st = new StringTokenizer(text);
				while(st.hasMoreTokens()){
					word.add(st.nextToken()+","+title);
				}
				return word;
			}
			
		});
		
		// convert the above flatmap to a PairMap with (word,title) as key
		// and 1 as value
		JavaPairRDD wordCountOnes = words.mapToPair(new PairFunction<String,String,Integer>(){

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word,1);
			}
			
		});
		
		// reduce the above PairMap by key to find how many times a word
		// appears in a specific title:
		JavaPairRDD<String, Integer> documentLevelWordCounts = wordCountOnes.reduceByKey(
				  new Function2<Integer, Integer, Integer>() {
				    public Integer call(Integer i1, Integer i2) {
				      return i1 + i2;
				    }
				  }
				);
		
		//documentLevelWordCounts = documentLevelWordCounts.sortByKey(true);
		
		/*documentLevelWordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>(){

			@Override
			public void call(Tuple2<String, Integer> arg0) throws Exception {
				System.out.println(arg0._1+"\t"+arg0._2);
				
			}
			
		});*/
		
		// Now we count how many times a word appears throughout all titles
		
		// For this, take out all keys i.e. (word,title) from documentLevelWordCounts
		// Keep only word from the above pair and discard titles
		// So now for every word, we will have word as key and 1 as value
		// the 1 denotes that this word has appeared in 1 distinct document
		JavaRDD<String> wordAndDocs = documentLevelWordCounts.keys();
		JavaPairRDD<String,Integer> noOfDocsContainingWord =  wordAndDocs.mapToPair(new PairFunction<String,String,Integer>(){

			@Override
			public Tuple2<String, Integer> call(String key) throws Exception {
				String[] split = key.split(",");
				return new Tuple2<String, Integer>(split[0],1);
			}
			
		});
		
		// Reduce the above PairMap by key to find out how many times a word
		// has appeared throughout all titles:
		final JavaPairRDD<String, Integer> docsContainingWord = noOfDocsContainingWord.reduceByKey(
			new Function2<Integer, Integer, Integer>() {

				@Override
				public Integer call(Integer i1, Integer i2)
						throws Exception {
					return i1 + i2;
				}
				
			});
		
		/*docsContainingWord.foreach(new VoidFunction<Tuple2<String,Integer>>(){

			@Override
			public void call(Tuple2<String, Integer> arg0) throws Exception {
				System.out.println(arg0._1+"\t"+arg0._2);			
			}
			
		});*/
		
		// We have documentLevelWordCounts which is PairMap that contains
		// (word,title) as key and the count of how many times this word
		// appeared in this title as value
		// What we are doing here is separate word and title and convert this 
		// MapPair to hold word as key and (count,title) as value:
		JavaPairRDD<String,String> keysSeparated = documentLevelWordCounts.mapToPair(new PairFunction<Tuple2<String,Integer>,String,String>(){

			@Override
			public Tuple2<String, String> call(Tuple2<String, Integer> tuple)
					throws Exception {
				String[] split = tuple._1.split(",");
				return new Tuple2<String,String>(split[0],tuple._2+","+split[1]);
			}
			
		});
		
		
		// Now keysSeparated contains word as key and (title,count) as value
		// and docsContainingWord contains word as key and total count where
		// this word occurs throughout all titles as value
		// We need to combine these both so that we can calculate tfidf easily
		// For this we do a join on both these RDDs by key:
		JavaPairRDD<String,Tuple2<String,Integer>> joinedtfidf = keysSeparated.join(docsContainingWord);
		
		/*joined.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Integer>>>(){

			@Override
			public void call(Tuple2<String, Tuple2<String, Integer>> arg0)
					throws Exception {
				System.out.println(arg0._1+"\t"+arg0._2._1+" "+arg0._2._2);
				
			}
			
		});*/
		
		// Using the above joined RDD, we have all the values needed to 
		// calculate the final tfidf for a word for each title. We have 
		// tf and df values for words in joinedtfidf. The value of N was
		// calculated previously in the program and is stored in var N.
		JavaPairRDD<String,Double> finaltfidf = joinedtfidf.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Integer>>,String,Double>(){

			@Override
			public Tuple2<String, Double> call(
					Tuple2<String, Tuple2<String, Integer>> tuple)
					throws Exception {
				String[] splitTitleTf = tuple._2._1.split(",");
				double tf = Double.parseDouble(splitTitleTf[0]);
				double idf = (double)tuple._2._2;
				double tfidf = tf * Math.log(N/idf);
				return new Tuple2<String,Double>(tuple._1+","+splitTitleTf[1], tfidf);
			}
			
		});
		
		/*finaltfidf.foreach(new VoidFunction<Tuple2<String,Double>>(){

			@Override
			public void call(Tuple2<String, Double> tuple) throws Exception {
				System.out.println(tuple._1+"\t"+tuple._2);
			}
			
		});*/
		
		// Finally after calculating the tfidf values for words, we need to 
		// filter out results that contain the word cloud:
		JavaPairRDD<String,Double> filtered = finaltfidf.filter(new Function<Tuple2<String,Double>,Boolean>(){

			@Override
			public Boolean call(Tuple2<String, Double> tuple) throws Exception {
				if(searchTerm.equals(tuple._1.split(",")[0]))
					return true;
				else
					return false;
			}
			
		});
		
		filtered.saveAsTextFile("/root/tfidffiltered");
		
		/*filtered.foreach(new VoidFunction<Tuple2<String,Double>>(){

			@Override
			public void call(Tuple2<String, Double> tuple) throws Exception {
				System.out.println(tuple._1 + "\t" + tuple._2);
			}
			
		});*/
		
		// Collect the filtered results in a list so that they can
		// be sorted easily:
		List<Tuple2<String,Double>> list = filtered.collect();
		
		// create a new comparator for the list to sort the results
		// in descending order of tfidf and if the tfidf are same
		// sort them by alphabetical order of title:
		list.sort(new Comparator<Tuple2<String,Double>>(){

			@Override
			public int compare(Tuple2<String, Double> o1,
					Tuple2<String, Double> o2) {
				if(o1._2 == o2._2){
					return o1._1.split(",")[1].compareTo(o2._1.split(",")[1]);
				}
				
				return (int) (o2._2 - o1._2);
			}
			
		});
		
		// If the list size is less than 100, store the full list as tsv
		// in the form title \t tfidf in a file named tfidf
		// Otherwise store only the top 100 results from the list in the 
		// file:
		try{
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("tfidf", true)));
			if(list.size() >= 100){
				for(int i=0;i<100;i++){
					Tuple2<String,Double> tuple = list.get(i);
					out.println(tuple._1.split(",")[1]+"\t"+tuple._2);
				}
			}
			else{
				for(Tuple2<String,Double> value:list){
					out.println(value._1.split(",")[1]+"\t"+value._2);
				}
			}
			
			out.close();
		} catch (IOException e) {
		    e.printStackTrace();
		}
		
		
		jsc.stop();		// Terminate the spark program
	}

}
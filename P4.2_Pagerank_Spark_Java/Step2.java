import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;


public class Step2 {
	
	public static int getIterableSize(Iterable<String> iter){
		if(iter == null)
			return -1;
		
		Iterator it = iter.iterator();
		int size = 0;
		while(it.hasNext()){
			size++;
			it.next();
		}
		return size;
	}
	
	private static class Sum implements Function2<Double, Double, Double> {
		@Override
		public Double call(Double a, Double b) {
			return a + b;
		}
	}
	
	public static void main(String[] args) {
		// initialize variables for master's address, the location of jar
		// which is required for spark to distribute across the cluster
		// and the HDFS/S3 input path:
		String masterAddr = "spark://JARVIS:7077";
		String jarLocation = "/home/rajat/Desktop/Step-2.jar";
		String hdfsInputPath = "hdfs://10.0.0.24:9000/sparkinput/sample_arcs";
		String hdfsInputPathMapp = "hdfs://10.0.0.24:9000/sparkinput/sample_mapping";
		int iter = 10;
		final List<Double> danglingContribsList = new ArrayList<Double>();

		// Initialize spark context:
		SparkConf sparkConf = new SparkConf().setAppName("Step2").setMaster(masterAddr);
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		jsc.addJar(jarLocation);
		JavaRDD<String> file = jsc.textFile(hdfsInputPath);

		JavaPairRDD<String,String> filePairs = file.mapToPair(new PairFunction<String,String,String>(){

			@Override
			public Tuple2<String, String> call(String lines) throws Exception {
				String[] split = lines.split("\t");
				return new Tuple2<String, String>(split[0],split[1]);
			}
			
		});
		
		JavaPairRDD<String, Iterable<String>> neighbors = filePairs.groupByKey();
		
		neighbors.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>(){

			@Override
			public void call(Tuple2<String, Iterable<String>> tuple2)
					throws Exception {
				System.out.println("N: "+tuple2._1+"\t"+tuple2._2.toString());				
			}
			
		});
		
		JavaRDD<String> mapFile = jsc.textFile(hdfsInputPathMapp);
		JavaPairRDD<String,Iterable<String>> allKeys = mapFile.mapToPair(new PairFunction<String,String,Iterable<String>>(){

			@Override
			public Tuple2<String, Iterable<String>> call(String value)
					throws Exception {
				return new Tuple2<String, Iterable<String>>(value.split("\t")[0],null);
			}
			
		});
		
		JavaRDD<String> dangles = allKeys.keys().subtract(neighbors.keys());
		
		dangles.foreach(new VoidFunction<String>(){

			@Override
			public void call(String arg0) throws Exception {
				System.out.println("D: "+arg0);
			}
			
		});
		
		JavaPairRDD<String,String> reversePairs = file.mapToPair(new PairFunction<String,String,String>(){

			@Override
			public Tuple2<String, String> call(String lines) throws Exception {
				String[] split = lines.split("\t");
				return new Tuple2(split[1],split[0]);
			}
			
		});
		
		JavaPairRDD<String, Iterable<String>> connectedBy = reversePairs.groupByKey();
		
		connectedBy.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>(){

			@Override
			public void call(Tuple2<String, Iterable<String>> arg0)
					throws Exception {
				System.out.println("C: "+arg0._1+"\t"+arg0._2.toString());				
			}
			
		});
		
		JavaPairRDD<String,Iterable<String>> danglingPairs = dangles.mapToPair(new PairFunction<String,String,Iterable<String>>(){
			
			
			@Override
			public Tuple2<String, Iterable<String>> call(String value)
					throws Exception {
				
				return new Tuple2<String,Iterable<String>>(value,null);
			}
			
		});
		
		JavaPairRDD<String, Iterable<String>> danglingNodes = connectedBy.intersection(danglingPairs);
		
		danglingNodes.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>(){

			@Override
			public void call(Tuple2<String, Iterable<String>> arg0)
					throws Exception {
				System.out.println("D: "+arg0._1+"\t"+arg0._2.toString());				
			}
			
		});
		
		JavaPairRDD<String,Iterable<String>> neighborsWithDangles = neighbors.union(danglingPairs);
		
		neighborsWithDangles.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>(){

			@Override
			public void call(Tuple2<String, Iterable<String>> arg0)
					throws Exception {
				if(arg0._2 == null)
					System.out.println("ND: "+arg0._1+"\tnull");
				else
					System.out.println("ND: "+arg0._1+"\t"+arg0._2.toString());
			}
			
		});
		
		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> nodeNeighborsAdjacents = neighborsWithDangles.join(connectedBy);
		
		JavaPairRDD<String, Double> rank = nodeNeighborsAdjacents.mapValues(new Function<Tuple2<Iterable<String>,Iterable<String>>, Double>(){

			@Override
			public Double call(Tuple2<Iterable<String>, Iterable<String>> arg0)
					throws Exception {
				return 1.0;
			}
			
		});
		
		JavaPairRDD<String, Tuple2<Tuple2<Iterable<String>, Iterable<String>>, Double>> joined = nodeNeighborsAdjacents.join(rank);
		
		joined.foreach(new VoidFunction<Tuple2<String,Tuple2<Tuple2<Iterable<String>,Iterable<String>>,Double>>>(){

			@Override
			public void call(
					Tuple2<String, Tuple2<Tuple2<Iterable<String>, Iterable<String>>, Double>> tuple)
					throws Exception {
				if(tuple._2._1._1!=null)
					System.out.println("J: "+tuple._1+"\t"+tuple._2._1._1.toString()+"\t"+tuple._2._1._2.toString()+"\t"+tuple._2._2);
				else
					System.out.println("J: "+tuple._1+"\t"+"null"+"\t"+tuple._2._1._2.toString()+"\t"+tuple._2._2);
				
			}
			
		});
		
		JavaDoubleRDD danglingContribs = joined.flatMapToDouble(new DoubleFlatMapFunction<Tuple2<String,Tuple2<Tuple2<Iterable<String>,Iterable<String>>,Double>>>(){

			@Override
			public Iterable<Double> call(
					Tuple2<String, Tuple2<Tuple2<Iterable<String>, Iterable<String>>, Double>> tuple)
					throws Exception {
				double size = 0;
				Double[] d = new Double[1];
				List<Double> list = new ArrayList<Double>();
				if(tuple._2._1._1==null){
					
					Iterator<String> it = tuple._2._1._2.iterator();
					while(it.hasNext()){
						size++;
						it.next();
					}
					
					list.add(1.0/size);
				}
				return list;
			}
			
		});
		
		danglingContribs.foreach(new VoidFunction<Double>(){

			@Override
			public void call(Double values) throws Exception {
				System.out.println("DV: "+values);
			}
			
		});
		
		final double danglingContribsSum = danglingContribs.sum();
		
		System.out.println(danglingContribsSum);
		
		for(int i=0;i<1;i++){
			 JavaPairRDD<String, Double> contribs = nodeNeighborsAdjacents.join(rank).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<Tuple2<Iterable<String>,Iterable<String>>,Double>>,String,Double>(){

				@Override
				public Iterable<Tuple2<String, Double>> call(
						Tuple2<String, Tuple2<Tuple2<Iterable<String>, Iterable<String>>, Double>> tuple)
						throws Exception {
					List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
					int neighborsCount = getIterableSize(tuple._2._1._1);
					if(neighborsCount > 0){
						for(String s:tuple._2._1._1){
							results.add(new Tuple2<String, Double>(s, (tuple._2._2/neighborsCount)+danglingContribsSum) );
						}
					}
					else if(neighborsCount == -1){
						results.add(new Tuple2<String,Double>(tuple._1, ( tuple._2._2 + danglingContribsSum )));
					}
					return results;
				}
				
			});
			 
			 contribs.foreach(new VoidFunction<Tuple2<String,Double>>(){

				@Override
				public void call(Tuple2<String, Double> tuple) throws Exception {
					System.out.println("CO: "+tuple._1+"\t"+tuple._2);					
				}
				 
			 });
			 
			 rank = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
			        @Override
			        public Double call(Double sum) {
			          return 0.15 + sum * 0.85;
			        }
			      });
		}
		
		List<Tuple2<String, Double>> output = rank.collect();
	    for (Tuple2<?,?> tuple : output) {
	        System.out.println(tuple._1() + " has rank: " + tuple._2() + ".");
	    }
		
		jsc.stop();		// Terminate the spark program
	}

}

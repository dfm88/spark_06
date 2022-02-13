package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		 SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the cluster
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read the content of the input file
		JavaRDD<String> inputRDD = sc.textFile(inputPath);

		// * * * * * * * * * * * * * * * * //
		// * Task 1 * //

		// 1 - create a PairRDD (user_id, list of product_ids reviewed by user_id) //

		//skip header line
		JavaRDD<String> tableRDD = inputRDD.filter(line->{
			String[] splitted = line.split(",");
			return !splitted[0].equals("Id");
		});
		// pair RDD <userId, ProductId>
		JavaPairRDD<String, String> userProdPairRDD = tableRDD.mapToPair(line -> {
				String[] splitted = line.split(",");
				return new Tuple2<>(splitted[2], splitted[1]);
		}).distinct();
        // pair RDD <userId, [ProductIds]>
		JavaPairRDD<String, Iterable<String>> userProdListPairRDD = userProdPairRDD.groupByKey();

		// 2 - counts the freq of pair products reviewed together //
		////// just for debug print result
		List<Tuple2<String, Iterable<String>>> abc = userProdListPairRDD.collect();
		abc.forEach(line-> System.out.println(line._1()+" "+line._2().toString()));
		//A3 [B3]
		//A4 [B1, B3, B4, B5]
		//A5 [B1, B3, B5]
		//A1 [B2]
		//A2 [B1, B3, B5]
		//////

		JavaRDD<Iterable<String>> prodReviewRDD = userProdListPairRDD.values();
		//[B3]
		//[B1, B3, B4, B5]
		//[B1, B3, B5]
		//[B2]
		//[B1, B3, B5]

		// returns a string <"prod1 prod", +1> for each prod reviewed together
		JavaPairRDD<String, Integer> prodPairPlusOne = prodReviewRDD.flatMapToPair(listProd ->{
			List<Tuple2<String, Integer>> prodsOcc = new ArrayList<>();

			for(String prod1 : listProd){
				for (String prod2 : listProd){
					if(prod1.compareTo(prod2)<0){
						prodsOcc.add(new Tuple2<>(prod1+" "+prod2, 1));
					}
				}

			}
			return prodsOcc.iterator();

		});
		////// for debug
		System.out.println("\nMine");
		List<Tuple2<String, Integer>> abc2 = prodPairPlusOne.collect();
		abc2.forEach(line-> System.out.println(line._1()+" "+line._2().toString()));
		///////

		// 3 - save the products that appear more than once together sorted by freq desc
		// and filter only those with Integer >1
		JavaPairRDD<String, Integer> prodsOccTotalPairRDD = prodPairPlusOne.
				reduceByKey((el1, el2)->el1+el2).
				filter(el->el._2()>1);

//		/**ORDER First method (top k + peronslized comparator)**/
//		// top k (k = rdd all the rdd) with personalized comparator based on Integer PairRDD (value not key)
//		List<Tuple2<String, Integer>> orderedList = prodsOccTotalPairRDD.top((int) prodsOccTotalPairRDD.count(), new CompareProdFreq());
//		//convert list to pairdd and save in output folder
//		JavaPairRDD<String, Integer> orderedPairRdd = sc.parallelizePairs(orderedList);
//		orderedPairRdd.saveAsTextFile(outputPath);

		/**ORDER Second method (switch key, value  + Order by key)**/
		//swap key and value
		JavaPairRDD<Integer, String> swappedTotalProdOccPairRDD = prodsOccTotalPairRDD.mapToPair(el -> {
			return new Tuple2<>(el._2(), el._1());
		});
		JavaPairRDD<Integer, String> orderedPairRdd2 = swappedTotalProdOccPairRDD.sortByKey(false);
		orderedPairRdd2.saveAsTextFile(outputPath);




		// * * * * * * * * * * * * * * * * //
		// * Task 2 * //

		// 1 - save top 10 most freq pairs of products and their freq //


		// Store the result in the output folder
		//resultRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}

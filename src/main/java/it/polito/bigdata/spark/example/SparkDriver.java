package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
	
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


		// 2 - counts the freq of pair products reviewed together //


		// 3 - save the products that appear more than once together sorted by freq desc //


		// * * * * * * * * * * * * * * * * //
		// * Task 2 * //

		// 1 - save top 10 most freq pairs of products and their freq //


		// Store the result in the output folder
		resultRDD.saveAsTextFile(outputPath);

		// Close the Spark context
		sc.close();
	}
}

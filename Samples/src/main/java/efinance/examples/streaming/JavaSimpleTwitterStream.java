package efinance.examples.streaming;

import scala.Tuple2;
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.regex.Pattern;


public class JavaSimpleTwitterStream {
	
	public static void main(String[] args) {
	    if (args.length < 2) {
	      System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
	      System.exit(1);
	    }

	    //StreamingExamples.setStreamingLogLevels();

	    // Create the context with a 1 second batch size
	    SparkConf sparkConf = new SparkConf().setAppName("JavaSimpleTwitterStream");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1), sparkHome, new String[]{jarFile});
	    
	    // create a DStream of twetter statuses
	    // continuous stream of RDDs containing objects of type twitter4j.Status. 
	    // As a very simple processing step, let’s try to print the status text of the some of the tweets.
	    JavaDStream<Status> tweets = ssc.twitterStream();
	    
	    // the map operation on tweets maps each Status object to its text to create a new ‘transformed’ DStream named statuses. 
	    // The print output operation tells the context to print first 10 records in 
	    //each RDD in a DStream, which in this case are 1 second batches of received status texts.	    
	    JavaDStream<String> statuses = tweets.map(
	    	      new Function<Status, String>() {
	    	        public String call(Status status) { return status.getText(); }
	    	      }
	    	    );
	    statuses.print();
	    
	    ssc.checkpoint(checkpointDir);
	    
	    ssc.start();
	}

}

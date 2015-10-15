package efinance.examples.streaming;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
   FROM {SPARK_HOME} START WITH
   .\bin\spark-submit  --class "efinance.examples.streaming.JavaHashtagFilter"   --master local[4]   target\Samples-0.0.1-SNAPSHOT.jar
   @author m.piunti 
 */
public class JavaHashtagFilter {
	
	public static void main(String[] args) {
		
	   
	    //StreamingExamples.setStreamingLogLevels();

	    // Create the context with a 1 second batch size
	    SparkConf sparkConf = new SparkConf().setAppName("JavaHashtagFilter");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	    
	    System.setProperty("http.proxyHost", "http://proxy.reply.it");
        System.setProperty("http.proxyPort", "8080");
        System.setProperty("https.proxyHost", "http://proxy.reply.it");
        System.setProperty("https.proxyPort", "8080");
	    
	    //twitter4j.auth.Authorization auth  
	    ConfigurationBuilder  cb = new ConfigurationBuilder() ;	   
	    cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("BQfJNLBe0f1XZ0TQWa8U87nwe")
		  .setOAuthConsumerSecret("wOrp2PSZnaJZWFFrsIlni1jJzIefLBvHKoGzb949j0GnASaoHf")
		  .setOAuthAccessToken("218617850-zpBYMlHI0mEvPcG2AKOawSEkKBx4mbOipqUgLiTY")
		  .setOAuthAccessTokenSecret("rbENmyQ427RxQ0mZiNQ0livDoB4V5VPjsHLRPz0FN2cWy");
	    Configuration conf = cb.build();
	    OAuthAuthorization  oauth = new OAuthAuthorization(conf);
	    
	    JavaDStream<Status> tweets = TwitterUtils.createStream(ssc, oauth);
	    
	    JavaDStream<String> statuses = tweets.map(
	    	      new Function<Status, String>() {
	    	        public String call(Status status) { return status.getText(); }
	    	      }
	    );
	    
	    JavaDStream<String> words = statuses.flatMap(
	    	     new FlatMapFunction<String, String>() {
	    	       public Iterable<String> call(String in) {
	    	         return Arrays.asList(in.split(" "));
	    	       }
	    	     }
	    	   );
	    
	    JavaDStream<String> hashTags = words.filter(
	    	     new Function<String, Boolean>() {
	    	       public Boolean call(String word) { return word.startsWith("#"); }
	    	     }
	    	   );

	    words.print();	    

	    ssc.start();
	    ssc.awaitTermination();    
	}

}

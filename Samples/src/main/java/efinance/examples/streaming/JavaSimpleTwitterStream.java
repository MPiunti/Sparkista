package efinance.examples.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
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
   .\bin\spark-submit  --class "efinance.examples.streaming.JavaSimpleTwitterStream"   --master local[4]   target\Samples-0.0.1-SNAPSHOT.jar
   @author m.piunti
 */
public class JavaSimpleTwitterStream {
	
	public static void main(String[] args) {
	  
	    //StreamingExamples.setStreamingLogLevels();
		Logger.getLogger("org").setLevel(Level.OFF);


	    // Create the context with a 1 second batch size
	    SparkConf sparkConf = new SparkConf().setAppName("JavaSimpleTwitterStream");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	    
	    
//	    System.setProperty("http.proxyHost", "http://proxy.reply.it");
//        System.setProperty("http.proxyPort", "8080");
//        System.setProperty("https.proxyHost", "http://proxy.reply.it");
//        System.setProperty("https.proxyPort", "8080");
//	    
	    //twitter4j.auth.Authorization AUTH
	    ConfigurationBuilder  cb = new ConfigurationBuilder() ;	  
	    cb.setDebugEnabled(true);
//		  .setOAuthConsumerKey("BQfJNLBe0f1XZ0TQWa8U87nwe")
//		  .setOAuthConsumerSecret("wOrp2PSZnaJZWFFrsIlni1jJzIefLBvHKoGzb949j0GnASaoHf")
//		  .setOAuthAccessToken("218617850-zpBYMlHI0mEvPcG2AKOawSEkKBx4mbOipqUgLiTY")
//		  .setOAuthAccessTokenSecret("rbENmyQ427RxQ0mZiNQ0livDoB4V5VPjsHLRPz0FN2cWy");
	    cb.setOAuthConsumerKey("LXMCzC2Xh03gRHa1c0Alc9at5");
	    cb.setOAuthConsumerSecret("CqiOJuoCuxol6ufvPjkRO44CDlhuAxf6jUhgxHIIsJm51u2xVe");
	    cb.setOAuthAccessToken("28091059-jn5EJuCDBbeDnk8XNsSAdfa6mkaF9oJoUgh6UWQ2I");
	    cb.setOAuthAccessTokenSecret("mXiZezgxaYwXHujZaB44tyYYDi6AfqheqAmmGDqehd0iG"); 
	    Configuration conf = cb.build();
	    OAuthAuthorization  oauth = new OAuthAuthorization(conf);
	    
	    JavaDStream<Status> tweets = TwitterUtils.createStream(ssc, oauth /*, StorageLevels.MEMORY_AND_DISK_SER */);
	    
	     
	    /*
	    // create a DStream of twetter statuses
	    // continuous stream of RDDs containing objects of type twitter4j.Status. 
	    // As a very simple processing step, let�s try to print the status text of the some of the tweets.
	    // JavaDStream<Status> tweets = ssc.twitterStream();
	    */
	    
	    // the map operation on tweets maps each Status object to its text to create a new �transformed� DStream named statuses. 
	    // The print output operation tells the context to print first 10 records in 
	    //each RDD in a DStream, which in this case are 1 second batches of received status texts.	    
	    JavaDStream<String> statuses = tweets.map(
	    	      new Function<Status, String>() {
	    	        public String call(Status status) { 
	    	        	System.out.println(" ********  TWEET:  " + status.getText() );
	    	        	return status.getText(); 
	    	        }
	    	      }
	    );
	    statuses.print();
	    
	    //ssc.checkpoint("./checkpoint/");	    

	    ssc.start();
	    ssc.awaitTermination();    
	}

}

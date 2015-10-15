package efinance.examples.streaming;

import org.apache.spark.SparkConf;
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
   .\bin\spark-submit  --class "efinance.examples.streaming.JavaFilteredTwitterStream"   --master local[4]   target\Samples-0.0.1-SNAPSHOT.jar  sun
   @author m.piunti 
 */
public class JavaFilteredTwitterStream {
	
	public static void main(String[] args) {
		
	    if (args.length < 1) {
	      System.err.println("Usage: JavaFilterTwitterStream <filter1>");
	      System.exit(1);
	    } 
	    final String FILTER = args[0];
	    System.out.println("Going to filter Twitter Stream with filter: '" + FILTER + "'");   

	    //StreamingExamples.setStreamingLogLevels();

	    // Create the context with a 1 second batch size
	    SparkConf sparkConf = new SparkConf().setAppName("JavaFilterTwitterStream");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
	    
	    //twitter4j.auth.Authorization auth  
	    ConfigurationBuilder  cb = new ConfigurationBuilder() ;	   
	    cb.setOAuthConsumerKey("LXMCzC2Xh03gRHa1c0Alc9at5");
	    cb.setOAuthConsumerSecret("CqiOJuoCuxol6ufvPjkRO44CDlhuAxf6jUhgxHIIsJm51u2xVe");
	    cb.setOAuthAccessToken("28091059-jn5EJuCDBbeDnk8XNsSAdfa6mkaF9oJoUgh6UWQ2I");
	    cb.setOAuthAccessTokenSecret("mXiZezgxaYwXHujZaB44tyYYDi6AfqheqAmmGDqehd0iG"); 
	    Configuration conf = cb.build();
	    OAuthAuthorization  oauth = new OAuthAuthorization( conf);
	    
	    JavaDStream<Status> tweets = TwitterUtils.createStream(ssc, oauth);
	    
	    System.exit(1);
	   	    
	    // the map operation on tweets maps each Status object to its text to create a new �transformed� DStream named statuses. 
	    // The print output operation tells the context to print first 10 records in 
	    //each RDD in a DStream, which in this case are 1 second batches of received status texts.	    
	    JavaDStream<String> statuses = tweets.map(
	    	      new Function<Status, String>() {
	    	        public String call(Status status) { return status.getText(); }
	    	      }
	    );
	    JavaDStream<String> filtered =  statuses.filter(new Function<String, Boolean>() {
	    	  public Boolean call(String s) { return s.contains(FILTER); }
	    });

	    filtered.print();	    

	    ssc.start();
	    ssc.awaitTermination();    
	}

}
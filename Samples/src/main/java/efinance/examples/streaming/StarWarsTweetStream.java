package efinance.examples.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

import com.google.common.base.Joiner;


/**
 * Spark Streaming Hello world!
.\bin\spark-submit  --class "efinance.examples.streaming.StarWarsTweetStream"   --master local[4]   target\Samples-0.0.1-SNAPSHOT.jar
 */
public class StarWarsTweetStream {
	public static void main(String... args) {
		StarWarsTweetStream swts = new StarWarsTweetStream();
		swts.readStream();
	}
	
	private transient JavaStreamingContext jssc;
	
	public StarWarsTweetStream(SparkConf sc) {
		jssc = new JavaStreamingContext(sc, Durations.seconds(5));
	}
	
	public StarWarsTweetStream() {
		this(new SparkConf()
				.setAppName(StarWarsTweetStream.class.getSimpleName())
				.setMaster("local[*]"));
	}
	
	public void readStream() {
		
		Logger.getLogger("org").setLevel(Level.OFF);

		
//	    System.setProperty("http.proxyHost", "http://proxy.reply.it");
//        System.setProperty("http.proxyPort", "8080");
//        System.setProperty("https.proxyHost", "http://proxy.reply.it");
//        System.setProperty("https.proxyPort", "8080");
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("BQfJNLBe0f1XZ0TQWa8U87nwe")
		  .setOAuthConsumerSecret("wOrp2PSZnaJZWFFrsIlni1jJzIefLBvHKoGzb949j0GnASaoHf")
		  .setOAuthAccessToken("218617850-zpBYMlHI0mEvPcG2AKOawSEkKBx4mbOipqUgLiTY")
		  .setOAuthAccessTokenSecret("rbENmyQ427RxQ0mZiNQ0livDoB4V5VPjsHLRPz0FN2cWy");
		Authorization auth = new OAuthAuthorization(cb.build());
		JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc, auth, new String[] {"Oscar", "StarWars"});
		
		/*
		stream.foreachRDD(rdd -> {
			rdd.foreach(status -> {
				System.out.println(Joiner.on(",").join(status.getHashtagEntities()));
				System.out.println(status.getText());
				System.out.println("------");
			});
			return null;
		});*/
		jssc.start();
		jssc.awaitTerminationOrTimeout(Durations.seconds(60).milliseconds());
		jssc.stop();
	}
}

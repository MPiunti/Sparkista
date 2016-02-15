package efinance.examples.clustering;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;



/**
FROM {SPARK_HOME} START WITH
.\bin\spark-submit  --class "efinance.examples.clustering.KMeansU30Example"   --master local[4]   target\Samples-0.0.1-SNAPSHOT.jar
@author m.piunti 
*/
public class KMeansU30Example {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("K-means Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Load and parse data
    //String path = "data/mllib/kmeans_data.txt";
    String path = "data/gen/genU30_cluster.csv";
    
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(
      new Function<String, Vector>() {
        public Vector call(String s) throws Exception {
          String[] sarray = s.split(";");
          double[] values = new double[sarray.length-2];          
          // only include double values
          for (int i = 0; i < sarray.length; i++){   
        	  
        	   	  
              if((sarray[sarray.length-1].equals("EQ") || sarray[sarray.length-1].equals("EV") || sarray[sarray.length-1].equals("TE") ) ) {
              

	        	  // values[i] = Double.parseDouble(sarray[i]);
	        	  if(i==0){   // Società Persona
		        	  try{
		        		  switch(sarray[i]) {
		        		  case "P" : values[0] = 2.0; 
		        		             break;
		        		  case "S" : values[0] = 1.0;
		        		             break;
		        		  default: values[0] =  0.0;
		        		  }       		 
		        	  }catch(Exception e){}
	        	  }
	        	  else if(i==1) // eta
	        		  values[1] = Double.parseDouble(sarray[i]);
	        	  
	        	  else if(i==2){   // Società Persona
		        	  try{
		        		  switch(sarray[i]) {
		        		  case "M" : values[2] = 2.0; 
		        		             break;
		        		  case "F" : values[2] = 1.0;
		        		             break;
		        		  default: values[2] =  0.0;
		        		  }       		 
		        	  }catch(Exception e){}
	        	  }
	        	  else if(i==3){}  /* provincia */
	        	  else if(i==4){   /* Regione
		        	  try{
		        		  switch(sarray[i]) {
		        		  case "M" : values[i] = 1.0; 
		        		             break;
		        		  case "F" : values[i] = 0.0;
		        		             break;
		        		  default: values[i] = -1.0;
		        		  }       		 
		        	  }catch(Exception e){}*/
	        	  }
	        	 else if(i==5){
	        		 try{
		        		  switch(sarray[i]) {
		        		       case "EQ" : values[3] = 3.0; 
		        		             break;
		        		       case "EV" : values[3] = 2.0;
		        		             break;
		        		       case "TE" : values[3] = 1.0;
	 		                         break;
		        		       default: values[3] = 0.0;
		        		  }       		 
		        	  }catch(Exception e){}
	        	 	}   
              }// if SDG is valid
          }
          if (values.length != (sarray.length-2)) {
              throw new Exception("\n\n\n\n\n  All vectors are not the same size! \n "+values[0] +", "
            		  +values[1]+", "+ +values[2]+", "+ +values[3] 
            		  +"  \n\n\n\n ");
          }
          
          return Vectors.dense(values);
        }
      }
    );
    parsedData.cache();

    // Cluster the data into two classes using KMeans
    int numClusters = 3;
    int numIterations = 10;
    KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations );

    
    
    System.out.println("Cluster centers:");
    for (Vector center : clusters.clusterCenters()) {
      System.out.println(" " + center);
    }

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    double WSSSE = clusters.computeCost(parsedData.rdd());
    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    // Save and load model
    clusters.save(sc.sc(), "myModelPath");
    KMeansModel sameModel = KMeansModel.load(sc.sc(), "myModelPath");
  }
}
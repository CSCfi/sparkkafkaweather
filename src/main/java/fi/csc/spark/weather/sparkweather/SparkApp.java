package fi.csc.spark.weather.sparkweather;

//import java.util.ArrayList;
//import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import fi.csc.spark.weather.kafka.KafkaWeatherConsumer;

public class SparkApp
{
    public static void main( String[] args ) throws InterruptedException
    {
    	//Juha
		UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("spark"));

    	SparkConf sparkConf = new SparkConf().setMaster("spark://testspark-spark-master:7077").setAppName("Weather Average");
    	sparkConf.set("spark.driver.host", System.getenv("IP"));
    	sparkConf.set("spark.driver.bindAddress", "0.0.0.0");
		sparkConf.set("spark.driver.port", "5050");


    	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    	JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, new Duration(2000));

    	KafkaWeatherConsumer kfc = new KafkaWeatherConsumer();

    	kfc.consume(ssc);


    	sparkContext.close();


    }
}

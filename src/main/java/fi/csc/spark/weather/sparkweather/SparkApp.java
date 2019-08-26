package fi.csc.spark.weather.sparkweather;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import fi.csc.spark.weather.kafka.KafkaWeatherConsumer;

/**
 * Hello world!
 *
 */
public class SparkApp 
{
    public static void main( String[] args ) throws InterruptedException
    {
       

    	SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Weather Average");

    	sparkConf.setMaster("local[*]");
    	sparkConf.set("spark.driver.host", "localhost");

    	
    	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    	
  
    	
    	
    	JavaStreamingContext ssc = new JavaStreamingContext(sparkContext, new Duration(2000));
    	
    	KafkaWeatherConsumer kfc = new KafkaWeatherConsumer();
    	
    	kfc.consume(ssc);
    	
    	
    	
    	sparkContext.close();

    	
    }
}

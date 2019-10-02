package fi.csc.spark.weather.kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import fi.csc.spark.weather.utils.SparkWeatherUtils;
import scala.Tuple2;

public class KafkaWeatherConsumer {


    Long mes = new Long(1);
    public void consume(JavaStreamingContext streamingContext) throws InterruptedException {

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "my-cluster-kafka:9092");
        kafkaParams.put("key.deserializer", LongDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "weather00");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("weatherHelsinki");

        JavaInputDStream<ConsumerRecord<Long, String>> kafkaStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<Long, String>Subscribe(topics, kafkaParams)
                );


        JavaPairDStream<Long,String> sparkStream = kafkaStream.mapToPair(record -> new Tuple2<Long,String>(record.key(), record.value()));

        JavaPairDStream<Long, Double> tempStream = sparkStream.mapValues(jsonValueStr -> SparkWeatherUtils.getAvgTemperature(jsonValueStr));

        JavaDStream<Tuple2<Double, Long>> countAndTempStream = tempStream.map(record -> new Tuple2<Double,Long>(record._2, new Long(1)));

        countAndTempStream.print();

        JavaDStream<Tuple2<Double, Long>> totalTempStream = countAndTempStream.reduce((a,b) -> new Tuple2<Double,Long>(a._1+b._1, a._2+b._2));


        JavaDStream<Double> avgTempStream = totalTempStream.map(record -> record._1/record._2);

        avgTempStream.print();

        avgTempStream.foreachRDD(rdd -> SparkWeatherUtils.appendAvgTemperatureToFile(rdd));

        streamingContext.start();

        streamingContext.awaitTermination();

    }

}

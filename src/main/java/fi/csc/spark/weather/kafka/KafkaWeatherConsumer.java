package fi.csc.spark.weather.kafka;

import java.util.*;

import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.*;

import fi.csc.spark.weather.utils.SparkWeatherUtils;
import scala.Tuple2;

public class KafkaWeatherConsumer {


    Long mes = new Long(1);
    public void consume(JavaStreamingContext streamingContext) throws InterruptedException {

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", "my-cluster-kafka:9092");
        kafkaParams.put("group.id", "weather00");
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("enable.auto.commit", "false");

        Set<String> topicsSet = new HashSet<>(Arrays.asList("weatherHelsinki"));

        // Create direct kafka stream with brokers and topics
        // There is no LongDecoder.class so decoding to string and parsing to Long later
        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(
                streamingContext,
                String.class,
                String.class,
                kafka.serializer.StringDecoder.class,
                kafka.serializer.StringDecoder.class,
                kafkaParams,
                topicsSet
        );


        // Parsing String to Long format
        JavaPairDStream<Long,String> sparkStream = kafkaStream.mapToPair(record -> new Tuple2<Long,String>(Long.parseLong(record._1), record._2));

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

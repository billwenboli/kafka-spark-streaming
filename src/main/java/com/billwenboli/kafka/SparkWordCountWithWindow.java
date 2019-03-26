package com.billwenboli.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

public class SparkWordCountWithWindow {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("SparkStreamingKafka").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(1));

        Collection<String> topics = Arrays.asList("sparkstream_in");

        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"group_stream");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Create initial Input DStream from kafka topic
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jsc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        // Read value of each message from Kafka and return it
        JavaDStream<String> lines = stream.map(record -> record.value());

        // Do word count operation on DStream
        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> wordMap = words.mapToPair(word -> new Tuple2<>(word, 1));

        // Compute based on window length
        JavaPairDStream<String, Integer> wordCount = wordMap.reduceByKeyAndWindow(
                (first, second) -> first + second, // Positive reduce operation
                (first, second) -> first - second, // Negative reduce operation
                Durations.minutes(3), // Window Duration
                Durations.seconds(3)  // Slide Interval Duration
        );

        // Process for each RDD
        wordCount.foreachRDD(rdd -> rdd.foreach(record -> {
            String formattedRecord = record._1 + " -> " + record._2;

            Producer<String, String> producer = OutputProducer.getProducer();

            producer.send(new ProducerRecord<>("sparkstream_out", "word-count-streaming", formattedRecord));
        }));

        jsc.checkpoint("/tmp/checkpoint");
        jsc.start();
        jsc.awaitTermination();
    }
}

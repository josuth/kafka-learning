package com.joseatorralba.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class PruebaProducer {

	public static void main(String[] args) throws ExecutionException, InterruptedException {

		Properties props = new Properties();
		// Broker de kafka al que nos vamos a conectar
		props.setProperty("bootstrap.servers", "localhost:9092");
		// Solicitamos ACKs de todos los nodos
		props.setProperty("acks", "all");
		// Define default String des/serializers
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		try (Producer<String, String> producer = new KafkaProducer<>(props)) {
			for (int i = 0; i < 10000; i++) {
				// Asynchronized moe
				producer.send(new ProducerRecord<>("prueba-topic", String.valueOf(i), "Mensaje " + i));
		
				// Synchronized mode
//				producer.send(new ProducerRecord<>("prueba-topic", String.valueOf(i), "Mensaje " + i)).get();
			}
			producer.flush();
		}
		
	}
}

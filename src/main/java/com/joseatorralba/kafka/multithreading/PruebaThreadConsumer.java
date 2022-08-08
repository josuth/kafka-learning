package com.joseatorralba.kafka.multithreading;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PruebaThreadConsumer extends Thread {

	private final KafkaConsumer<String, String> consumer;
	
	private final AtomicBoolean closed = new AtomicBoolean(false);
	
	public PruebaThreadConsumer(KafkaConsumer<String, String> consumer)	{
		this.consumer = consumer;
	}
	
	@Override
	public void run()	{
		consumer.subscribe(List.of("prueba-topic"));
		try	{
			while (!closed.get())	{
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords)	{
					log.info("Offset = {}, Partition = {}, Key = {}, Value = {}", 
							consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
				}
				
			}
		} catch(WakeupException e)	{
			if (!closed.get())	{
				throw e;
			}
		} finally {
			closed.set(true);
			
		}
	}
}

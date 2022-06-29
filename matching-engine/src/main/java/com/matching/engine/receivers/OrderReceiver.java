package com.matching.engine.receivers;

import com.ashish.marketdata.avro.Order;
import com.matching.engine.broker.KafkaBroker;
import com.matching.engine.service.BookManager;
import com.matching.engine.util.ExSimCache;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;

public class OrderReceiver implements Runnable {

    private BookManager bookManager;
    private String topic;
    private String serverUrl;
    private KafkaConsumer<String, String> kafkaConsumer;
    private CountDownLatch latch;
    private volatile boolean running = true;
    private ExSimCache cache = ExSimCache.getCache();
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderReceiver.class);

    public OrderReceiver(BookManager bookManager, CountDownLatch latch) {
        this.topic = cache.topic(ExSimCache.TXNTYPE.ORDER);
        this.serverUrl = cache.topic(ExSimCache.TXNTYPE.SERVER_URL);
        this.bookManager = bookManager;
        this.latch = latch;
        this.kafkaConsumer = new KafkaBroker(serverUrl).createConsumer(null);
        this.kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        while (isRunning()) {
            try {
                consumeFromKafka();
            } catch (WakeupException e) {
                LOGGER.error(Thread.currentThread().getId() + " Received shutdown signal");
                kafkaConsumer.close();
            }
        }
    }

    private void consumeFromKafka() throws WakeupException {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(10));
        for (ConsumerRecord<String, String> record : records) {
            String symbol = record.key();
            String data = record.value();
            byte[] decoded = Base64.getDecoder().decode(data);
            Order order = deSerealizeAvroHttpRequestJSON(decoded);
            bookManager.routOrder(order);
            LOGGER.info("Key: " + symbol + ", Value:" + data);
            LOGGER.info("Partition:" + record.partition() + ",Offset:" + record.offset());
        }
    }

    public Order deSerealizeAvroHttpRequestJSON(byte[] data) {
        DatumReader<Order> reader
                = new SpecificDatumReader<>(Order.class);
        Decoder decoder;
        try {
            decoder = DecoderFactory.get().jsonDecoder(Order.getClassSchema(), new String(data));
            return reader.read(null, decoder);
        } catch (IOException e) {
            LOGGER.error("Deserialization error:" + e.getMessage());
        }
        return null;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public void shutdown() {
        kafkaConsumer.wakeup();
    }


    @Override
    public String toString() {
        return "OrderReceiver{" +
                "topic='" + topic + '\'' +
                '}';
    }
}

package com.simulator.receivers;

import com.ashish.marketdata.avro.Order;
import com.simulator.broker.EMSBroker;
import com.simulator.broker.KafkaBroker;
import com.simulator.service.BookManager;
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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;

public class OrderReceiver implements Runnable {

    private boolean kafka;
    private BookManager bookManager;
    private String topic;
    private EMSBroker emsBroker;
    private KafkaConsumer<String, String> kafkaConsumer;
    private CountDownLatch latch;
    private volatile boolean running = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderReceiver.class);

    public OrderReceiver(String serverUrl, String topic, BookManager bookManager, boolean kafka, CountDownLatch latch) throws JMSException {
        this.topic = topic;
        this.kafka = kafka;
        this.bookManager = bookManager;
        this.latch=latch;
        if (!kafka) {
            emsBroker = new EMSBroker(serverUrl, null, null);
            emsBroker.createConsumer(topic, true);
        } else {
            this.kafkaConsumer = new KafkaBroker(serverUrl).createConsumer(null);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }
    }

    @Override
    public void run() {
        int ackMode = Session.AUTO_ACKNOWLEDGE;
        while (isRunning()) {
            if (!kafka) {
                try {
                    consumeFromEMS();
                } catch (JMSException e) {
                    LOGGER.error(Thread.currentThread().getId()+" Received shutdown signal");
                    try {
                        emsBroker.closeConsumer();
                    } catch (JMSException jmsException) {
                        jmsException.printStackTrace();
                    }
                }finally {
                    latch.countDown();
                }
            } else {
                try {
                    consumeFromKafka();
                } catch (WakeupException | JMSException e) {
                    LOGGER.error(Thread.currentThread().getId()+" Received shutdown signal");
                    kafkaConsumer.close();
                }finally {
                    latch.countDown();
                }
            }
        }
    }

    private void consumeFromKafka() throws WakeupException, JMSException {
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

    private void consumeFromEMS() throws JMSException {
        Message msg = emsBroker.consumer().receive();
        if (msg == null)
            return;
        if (msg instanceof TextMessage) {
            TextMessage message = (TextMessage) msg;
            byte[] decoded = Base64.getDecoder().decode(message.getText());
            Order order = deSerealizeAvroHttpRequestJSON(decoded);
            bookManager.routOrder(order);
        }
    }

    public Order deSerealizeAvroHttpRequestJSON(byte[] data) {
        DatumReader<Order> reader
                = new SpecificDatumReader<>(Order.class);
        Decoder decoder = null;
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

    public void shutdown(){
        kafkaConsumer.wakeup();
    }
}

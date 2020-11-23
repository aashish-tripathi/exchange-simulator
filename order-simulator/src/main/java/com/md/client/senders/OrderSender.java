package com.md.client.senders;

import com.ashish.marketdata.avro.Order;
import com.md.client.broker.EMSBroker;
import com.md.client.broker.KafkaBroker;
import com.md.client.service.PriceRange;
import com.md.client.util.Throughput;
import com.md.client.util.Utility;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;

public class OrderSender implements Runnable, ExceptionListener {

    private volatile boolean running = true;
    final private String topic;
    final private String[] symbols;
    final private String exchange;
    final private String brokerName;
    final private String brokerId;
    final private String clientId;
    final private String clientName;
    private PriceRange priceRange;
    private EMSBroker emsBroker;
    private boolean kafka;
    private KafkaProducer<String, String> kafkaProducer;
    private Throughput throughput;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderSender.class);

    public OrderSender(String serverUrl, String topic, String[] symbols, String exchange, String brokerName, String brokerId, String clientId, String clientName, boolean kafka, Throughput throughputWorker) throws JMSException {
        this.kafka = kafka;
        this.topic = topic;
        this.symbols = symbols;
        this.exchange = exchange;
        this.brokerName = brokerName;
        this.brokerId = brokerId;
        this.clientId = clientId;
        this.clientName = clientName;
        if (!kafka) {
            emsBroker = new EMSBroker(null, null, null);
            emsBroker.createProducer(topic, true);
        } else {
            kafkaProducer = new KafkaBroker(serverUrl).createProducer((null)); // create producer
        }
        this.throughput = throughputWorker;
        LOGGER.info("Order sending started by client {} ", clientName);
    }

    @Override
    public void run() {
        ThreadLocalRandom localRandom = ThreadLocalRandom.current();
        long start = System.currentTimeMillis();
        int msgCount = 0;
        while (isRunning()) {
            try {
                String randomStock = symbols[localRandom.nextInt(symbols.length)];
                Order newOrder = OrderCreator.createSingleOrder(randomStock, exchange, brokerName, brokerId, clientId, clientName);
                if (newOrder == null) {
                    continue;
                }
                byte[] encoded = Utility.serealizeAvroHttpRequestJSON(newOrder);
                String encodedOrder = Base64.getEncoder().encodeToString(encoded);
                if (!kafka) {
                    publishToEMS(newOrder, encodedOrder);
                } else {
                    publishToKafka(newOrder, encodedOrder);
                }
                Thread.sleep(1000);
                LOGGER.info("Order {} sent by {}", newOrder, newOrder.getClientName());
            } catch (JMSException | RuntimeException | InterruptedException e) {
                LOGGER.error("Error occurred while sending order " + e.fillInStackTrace());
                try {
                    if (e instanceof JMSException) {
                        emsBroker.closeProducer();
                    }
                } catch (JMSException ex) {
                    LOGGER.error("Error occurred while sending order " + ex.fillInStackTrace());
                }
            }
            msgCount++;
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getName());
        long end = System.currentTimeMillis();
        long timeT= (end-start)/1000;
        long msgF = msgCount;
        LOGGER.info("Message rate/sec {}",msgF/timeT);
    }

    private void publishToKafka(Order newOrder, String encodedOrder) {
        String symbol = newOrder.getSymbol().toString();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, symbol, encodedOrder);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    LOGGER.info("Key {}", symbol);
                    LOGGER.info("Topic {} ", recordMetadata.topic());
                    LOGGER.info("Partition {}", recordMetadata.partition());
                    LOGGER.info("Offset {}", recordMetadata.offset());
                } else {
                    LOGGER.info("Exception Occurred while sending order through kafka... {}", e.getLocalizedMessage());
                }
            }
        });
    }

    private void publishToEMS(Order newOrder, String encodedOrder) throws JMSException {
        TextMessage message = emsBroker.createMessage();
        message.setText(encodedOrder);
        emsBroker.send(message);
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    @Override
    public void onException(JMSException e) {

    }
}

package com.matching.engine.senders;

import com.ashish.marketdata.avro.Order;
import com.matching.engine.broker.KafkaBroker;
import com.matching.engine.util.EXSIMCache;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ExecutionsSender implements Runnable {

    private volatile boolean running = true;
    private String topic;
    private String serverUrl;
    private EXSIMCache cache= EXSIMCache.getCache();
    private KafkaProducer<String, String> kafkaProducer;
    private BlockingQueue<Order> executions = new LinkedBlockingQueue<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionsSender.class);

    public ExecutionsSender(String symbol) {
        this.topic = cache.topic(EXSIMCache.TXNTYPE.EXECUTION);
        this.serverUrl = cache.topic(EXSIMCache.TXNTYPE.SERVER_URL);
        Properties optionalProperties = new Properties();
        optionalProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        optionalProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        optionalProperties.put(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        optionalProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProducer = new KafkaBroker(serverUrl).createProducer((optionalProperties)); // create producer
        new Thread(this).start();
        LOGGER.info("Execution sender has started for stock {} ", symbol);
    }

    public void addExecutions(final Order execution) {
        executions.add(execution);
    }

    @Override
    public void run() {
        while (isRunning()) {
            Order execution = executions.poll();
            if (execution != null) {
                byte[] encoded = serealizeAvroHttpRequestJSON(execution);
                String encodedExecution = Base64.getEncoder().encodeToString(encoded);
                publishToKafka(execution.getSymbol().toString(), encodedExecution);
                LOGGER.info("Execution sent...{}", execution);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToKafka(String symbol, String encodedExecution) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, symbol, encodedExecution);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                LOGGER.info("Key {}", symbol);
                LOGGER.info("Topic {} ", recordMetadata.topic());
                LOGGER.info("Partition {}", recordMetadata.partition());
                LOGGER.info("Offset {}", recordMetadata.offset());
            } else {
                LOGGER.info("Exception Occurred while sending execution through kafka... {}", e.getLocalizedMessage());
            }
        });
    }

    public byte[] serealizeAvroHttpRequestJSON(
            Order request) {
        DatumWriter<Order> writer = new SpecificDatumWriter<>(
                Order.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    Order.getClassSchema(), stream);
            writer.write(request, jsonEncoder);
            jsonEncoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {
            LOGGER.error("Serialization error: {}" , e.getMessage());
        }
        return data;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}

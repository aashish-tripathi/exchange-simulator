package com.simulator.senders;

import com.ashish.marketdata.avro.Order;
import com.simulator.broker.EMSBroker;
import com.simulator.broker.KafkaBroker;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ExecutionsSender implements Runnable {

    private volatile boolean running = true;
    private boolean kafka;
    private String topic;
    private EMSBroker emsBroker;
    private KafkaProducer<String, String> kafkaProducer;
    private Map<String, BlockingQueue<Order>> executionsMap = new ConcurrentHashMap<>();
    private BlockingQueue<Order> executions = new LinkedBlockingQueue<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutionsSender.class);

    public ExecutionsSender(String serverUrl, String topic, String symbol, boolean kafka) throws JMSException {
        this.topic = topic;
        this.kafka = kafka;
        if (!kafka) {
            emsBroker = new EMSBroker(null, null, null);
            emsBroker.createProducer(topic, true);
        } else {
            Properties optionalProperties = new Properties();
            optionalProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            optionalProperties.put(ProducerConfig.ACKS_CONFIG, "all");
            optionalProperties.put(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
            optionalProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            kafkaProducer = new KafkaBroker(serverUrl).createProducer((optionalProperties)); // create producer
        }
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
                if (!kafka) {
                    publishToEMS(encodedExecution);
                } else {
                    publishToKafka(execution.getSymbol().toString(), encodedExecution);
                }
                LOGGER.info("Execution sent...{}", execution);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToEMS(String encodedExecution) {
        try {
            TextMessage message = emsBroker.createMessage();
            message.setText(encodedExecution);
            emsBroker.send(message);
        } catch (JMSException e) {
            LOGGER.error(e.getLocalizedMessage());
        }

    }

    private void publishToKafka(String symbol, String encodedExecution) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, symbol, encodedExecution);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    /*LOGGER.info("Key {}", symbol);
                    LOGGER.info("Topic {} ", recordMetadata.topic());
                    LOGGER.info("Partition {}", recordMetadata.partition());
                    LOGGER.info("Offset {}", recordMetadata.offset());*/
                } else {
                    LOGGER.info("Exception Occurred while sending execution through kafka... {}", e.getLocalizedMessage());
                }
            }
        });
    }

    public byte[] serealizeAvroHttpRequestJSON(
            Order request) {
        DatumWriter<Order> writer = new SpecificDatumWriter<>(
                Order.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = null;
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

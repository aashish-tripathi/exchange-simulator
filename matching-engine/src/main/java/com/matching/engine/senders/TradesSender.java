package com.matching.engine.senders;

import com.ashish.marketdata.avro.Trade;
import com.matching.engine.broker.KafkaBroker;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TradesSender implements Runnable {

    private volatile boolean running = true;
    private String topic;
    private KafkaProducer<String, String> kafkaProducer;
    private Map<String, BlockingQueue<Trade>> tradeMap = new ConcurrentHashMap<>();
    private BlockingQueue<Trade> tradeQueue = new LinkedBlockingQueue<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(TradesSender.class);

    public TradesSender(String serverUrl, String topic, String symbol) {
        this.topic = topic;
        Properties optionalProperties = new Properties();
        optionalProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        optionalProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        optionalProperties.put(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        optionalProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProducer = new KafkaBroker(serverUrl).createProducer((optionalProperties)); // create producer
        new Thread(this).start();
        LOGGER.info("TradesSender has started for stock {} ", symbol);
    }

    public void addTrade(final Trade trade) {
        tradeQueue.add(trade);
    }

    @Override
    public void run() {
        while (isRunning()) {
            Trade trade = tradeQueue.poll();
            if (trade != null) {
                byte[] encoded = serealizeAvroHttpRequestJSON(trade);
                String encodedTrade = Base64.getEncoder().encodeToString(encoded);
                publishToKafka(trade.getSymbol().toString(), encodedTrade);
                LOGGER.info("Trade sent...{}", trade);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToKafka(String symbol, String encodedTrade) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, symbol, encodedTrade);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                LOGGER.info("Key {},", symbol);
                LOGGER.info("Topic {} ", recordMetadata.topic());
                LOGGER.info("Partition {}", recordMetadata.partition());
                LOGGER.info("Offset {}", recordMetadata.offset());
            } else {
                LOGGER.info("Exception Occurred while sending trade through kafka... {}", e.getLocalizedMessage());
            }
        });
    }

    public byte[] serealizeAvroHttpRequestJSON(
            Trade request) {
        DatumWriter<Trade> writer = new SpecificDatumWriter<>(
                Trade.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    Trade.getClassSchema(), stream);
            writer.write(request, jsonEncoder);
            jsonEncoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {
            LOGGER.error("Serialization error:" + e.getMessage());
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

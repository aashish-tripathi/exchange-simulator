package com.simulator.senders;

import com.ashish.marketdata.avro.Trade;
import com.simulator.broker.EMSBroker;
import com.simulator.broker.KafkaBroker;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class TradesSender implements Runnable {

    private volatile boolean running = true;
    private boolean kafka;
    private String topic;
    private EMSBroker emsBroker;
    private KafkaProducer<String, String> kafkaProducer;
    private Map<String, BlockingQueue<Trade>> tradeMap = new ConcurrentHashMap<>();
    private BlockingQueue<Trade> tradeQueue = new LinkedBlockingQueue<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(TradesSender.class);

    public TradesSender(String serverUrl, String topic, String symbol, boolean kafka) throws JMSException {
        this.topic = topic;
        this.kafka = kafka;
        if (!kafka) {
            emsBroker = new EMSBroker(null, null, null);
            emsBroker.createProducer(topic, true);
        } else {
            kafkaProducer = new KafkaBroker(serverUrl).createProducer((null)); // create producer
        }
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
                if (!kafka) {
                    publishToEMS(encodedTrade);
                } else {
                    publishToKafka(trade.getSymbol().toString(), encodedTrade);
                }
                LOGGER.info("Trade sent...{}", trade);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToEMS(String encodedTrade) {
        try {
            TextMessage message = emsBroker.createMessage();
            message.setText(encodedTrade);
            emsBroker.send(message);
        } catch (JMSException e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }

    private void publishToKafka(String symbol, String encodedTrade) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, symbol, encodedTrade);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    LOGGER.info("Key {}", symbol);
                    LOGGER.info("Topic {} ", recordMetadata.topic());
                    LOGGER.info("Partition {}", recordMetadata.partition());
                    LOGGER.info("Offset {}", recordMetadata.offset());
                } else {
                    LOGGER.info("Exception Occurred while sending trade through kafka... {}", e.getLocalizedMessage());
                }
            }
        });
    }

    public byte[] serealizeAvroHttpRequestJSON(
            Trade request) {
        DatumWriter<Trade> writer = new SpecificDatumWriter<>(
                Trade.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = null;
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

package com.matching.engine.senders;

import com.ashish.marketdata.avro.Quote;
import com.matching.engine.broker.KafkaBroker;
import com.matching.engine.util.ExSimCache;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class QuotesSender implements Runnable {

    private volatile boolean running = true;
    private String topic;
    private String serverUrl;
    private KafkaProducer<String, String> kafkaProducer;
    private final Map<String, BlockingQueue<Quote>> quoteMap = new ConcurrentHashMap<>();
    private BlockingQueue<Quote> quoteQueue = new LinkedBlockingQueue<>();
    private ExSimCache cache= ExSimCache.getCache();
    private static final Logger LOGGER = LoggerFactory.getLogger(QuotesSender.class);

    public QuotesSender(String symbol) {
        this.topic = cache.topic(ExSimCache.TXNTYPE.QUOTE);
        this.serverUrl = cache.topic(ExSimCache.TXNTYPE.SERVER_URL);
        Properties optionalProperties = new Properties();
        optionalProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        optionalProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        optionalProperties.put(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        optionalProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProducer = new KafkaBroker(serverUrl).createProducer((optionalProperties)); // create producer
        new Thread(this).start();
        LOGGER.info("QuotesSender has started for stock {} ", symbol);
    }

    public void addQuote(final Quote quote) {
        quoteQueue.add(quote);
    }

    @Override
    public void run() {
        while (isRunning()) {
            Quote quote = quoteQueue.poll();
            if (quote != null) {
                byte[] encoded = serealizeAvroHttpRequestJSON(quote);
                String encodedQuote = Base64.getEncoder().encodeToString(encoded);
                publishToKafka(quote.getSymbol().toString(), encodedQuote);
                LOGGER.info("Quote sent...{}", quote);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToKafka(String symbol, String encodedQuote) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, symbol, encodedQuote);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if (e == null) {
                LOGGER.info("Key {}", symbol);
                LOGGER.info("Topic {} ", recordMetadata.topic());
                LOGGER.info("Partition {}", recordMetadata.partition());
                LOGGER.info("Offset {}", recordMetadata.offset());
            } else {
                LOGGER.info("Exception Occurred while sending quote through kafka... {}", e.getLocalizedMessage());
            }
        });
    }

    public byte[] serealizeAvroHttpRequestJSON(
            Quote request) {
        DatumWriter<Quote> writer = new SpecificDatumWriter<>(
                Quote.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    Quote.getClassSchema(), stream);
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

package com.simulator.senders;

import com.ashish.marketdata.avro.MarketPrice;
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
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MarketPriceSender implements Runnable {
    private volatile boolean running = true;
    private boolean kafka;
    private String topic;
    private EMSBroker emsBroker;
    private KafkaProducer<String, String> kafkaProducer;
    private BlockingQueue<MarketPrice> marketPriceQueue = new LinkedBlockingQueue<>();
    private MarketPrice lastSnapshot;


    private static final Logger LOGGER = LoggerFactory.getLogger(MarketPriceSender.class);

    public MarketPriceSender(String serverUrl, String topic, String symbol, boolean kafka) throws JMSException {
        this.topic = topic;
        this.kafka = kafka;
        if(!kafka) {
            emsBroker = new EMSBroker(null, null, null);
            emsBroker.createProducer(topic, true);
        }else{
            Properties optionalProperties = new Properties();
            optionalProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            optionalProperties.put(ProducerConfig.ACKS_CONFIG, "all");
            optionalProperties.put(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
            optionalProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
            kafkaProducer = new KafkaBroker(serverUrl).createProducer((optionalProperties)); // create producer
        }
        new Thread(this).start();
        LOGGER.info("MarketPriceSender has started for stock {} ",symbol);
    }

    public void addORUpdateMarketPrice(final MarketPrice marketPrice) {
        lastSnapshot = marketPrice; // assign latest
        marketPriceQueue.add(marketPrice);
    }

    public MarketPrice marketPrice(final String symbol) {
        return lastSnapshot==null? new MarketPrice(): lastSnapshot;
    }

    @Override
    public void run() {
        while (isRunning()) {
            MarketPrice marketPrice = marketPriceQueue.poll();
            if (marketPrice != null) {
                byte[] encoded = serealizeAvroHttpRequestJSON(marketPrice);
                String encodedMarketPrice = Base64.getEncoder().encodeToString(encoded);
                if(!kafka){
                    publishToEMS(encodedMarketPrice);
                }else{
                    publishToKafka(marketPrice.getSymbol().toString(), encodedMarketPrice);
                }
                LOGGER.info("MarketPrice sent...{}", marketPrice);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToEMS(String encodedMarketPrice) {
       try {
            TextMessage message = emsBroker.createMessage();
            message.setText(encodedMarketPrice);
            emsBroker.send(message);
        } catch (JMSException e) {
           LOGGER.error(e.getLocalizedMessage());
        }

    }

    private void publishToKafka(String symbol, String encodedMarketPrice) {
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic,symbol, encodedMarketPrice);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    LOGGER.info("Key {}" ,symbol);
                    LOGGER.info("Topic {} " ,recordMetadata.topic());
                    LOGGER.info("Partition {}" ,recordMetadata.partition());
                    LOGGER.info("Offset {}" ,recordMetadata.offset());
                }else{
                    LOGGER.info("Exception Occurred while sending order through kafka... {}", e.getLocalizedMessage());
                }
            }
        });
    }

    public byte[] serealizeAvroHttpRequestJSON(
            MarketPrice request) {
        DatumWriter<MarketPrice> writer = new SpecificDatumWriter<>(
                MarketPrice.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = null;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    MarketPrice.getClassSchema(), stream);
            writer.write(request, jsonEncoder);
            jsonEncoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Serialization error:" + e.getMessage());
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

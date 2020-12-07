package com.simulator.senders;

import com.ashish.marketdata.avro.AskDepth;
import com.ashish.marketdata.avro.BidDepth;
import com.ashish.marketdata.avro.MarketByPrice;
import com.ashish.marketdata.avro.Order;
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
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class MarketByPriceSender implements Runnable{

    private volatile boolean running = true;
    private boolean kafka;
    private String topic;
    private EMSBroker emsBroker;
    private KafkaProducer<String, String> kafkaProducer;
    private Map<String, MarketByPrice> marketPriceMap = new ConcurrentHashMap<>();
    private BlockingQueue<MarketByPrice> marketByPriceQueue = new LinkedBlockingQueue<>();
    private MarketByPrice lastSnapshot;

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketByPriceSender.class);

    public MarketByPriceSender(String serverUrl, String topic, String symbol, boolean kafka) throws JMSException {
        this.topic = topic;
        this.kafka = kafka;
        if(!kafka) {
            emsBroker = new EMSBroker(null, null, null);
            emsBroker.createProducer(topic, true);
        }else{
            kafkaProducer = new KafkaBroker(serverUrl).createProducer((null)); // create producer
        }
        new Thread(this).start();
        LOGGER.info("MarketByPriceSender has started {}"+symbol);
    }

    @Override
    public void run() {
        while (isRunning()) {

            MarketByPrice marketByPrice = marketByPriceQueue.poll();
            if (marketByPrice != null) {
                byte[] encoded = serealizeAvroHttpRequestJSON(marketByPrice);
                String encodedMarketByPrice = Base64.getEncoder().encodeToString(encoded);
                if(!kafka){
                    publishToEMS(marketByPrice, encodedMarketByPrice);
                }else{
                    publishToKafka(marketByPrice, encodedMarketByPrice);
                }
                LOGGER.info("MarketByPrice sent {}" , marketByPrice);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToKafka(MarketByPrice marketByPrice, String encodedMarketByPrice) {
        String symbol = marketByPrice.getSymbol().toString();
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String,String>(topic,symbol, encodedMarketByPrice);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    LOGGER.info("Key {}" ,symbol);
                    LOGGER.info("Topic {} " ,recordMetadata.topic());
                    LOGGER.info("Partition {}" ,recordMetadata.partition());
                    LOGGER.info("Offset {}" ,recordMetadata.offset());
                }else{
                    LOGGER.info("Exception Occurred while sending MarketByPrice through kafka... {}", e.getLocalizedMessage());
                }
            }
        });
    }

    private void publishToEMS(MarketByPrice marketByPrice, String encodedMarketByPrice) {
        try {
            TextMessage message = emsBroker.createMessage();
            message.setText(encodedMarketByPrice);
            emsBroker.send(message);
            LOGGER.info("MarketByPrice sent...{}", marketByPrice);
        } catch (Exception e) {
            LOGGER.error(e.getLocalizedMessage());
        }
    }

    public void addORUpdateOrderBook(String symbol, NavigableMap<Double, BlockingQueue<Order>> buyOrders, NavigableMap<Double, BlockingQueue<Order>> sellOrders){
        MarketByPrice marketByPrice = new MarketByPrice();
        marketByPrice.setSymbol(symbol);
        marketByPrice.setAskList(new ArrayList<>());
        marketByPrice.setBidList(new ArrayList<>());

        int buylimit = 0;
        Iterator<Double> buyIterator = buyOrders.descendingKeySet().iterator();
        while (buyIterator.hasNext()) {
            double buyPrice = buyIterator.next();
            BlockingQueue<Order> buyOrder = buyOrders.get(buyPrice);
            long qty = getTotalQty(buyOrder);
            if (qty == 0) {
                //buyIterator.remove();
            } else {
                marketByPrice.getBidList().add(new BidDepth(buyPrice, qty));
            }
            buylimit++;
            if(buylimit==10){
                break;
            }
        }

        int selllimit = 0;
        Iterator<Double> sellIterator = sellOrders.navigableKeySet().iterator();
        while (sellIterator.hasNext()) {
            double sellPrice = sellIterator.next();
            BlockingQueue<Order> sellOrder = sellOrders.get(sellPrice);
            long qty = getTotalQty(sellOrder);
            if (qty == 0) {
                //sellIterator.remove();
            } else {
                marketByPrice.getAskList().add(new AskDepth(sellPrice, qty));
            }
            if(selllimit==10){
                break;
            }
        }
        marketByPriceQueue.add(marketByPrice); // update new depth
    }

    public byte[] serealizeAvroHttpRequestJSON(
            MarketByPrice request) {
        DatumWriter<MarketByPrice> writer = new SpecificDatumWriter<>(
                MarketByPrice.class);
        byte[] data = new byte[0];
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder jsonEncoder = null;
        try {
            jsonEncoder = EncoderFactory.get().jsonEncoder(
                    MarketByPrice.getClassSchema(), stream);
            writer.write(request, jsonEncoder);
            jsonEncoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {
            LOGGER.error(e.getLocalizedMessage());
        }
        return data;
    }


    private long getTotalQty(BlockingQueue<Order> value) {
        long qty = 0;
        if(value!=null) {
            Iterator<Order> orders = value.iterator();
            while (orders.hasNext()) {
                qty += orders.next().getQuantity();
            }
        }
        return qty;

    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}

package com.matching.engine.senders;

import com.ashish.marketdata.avro.AskDepth;
import com.ashish.marketdata.avro.BidDepth;
import com.ashish.marketdata.avro.MarketByPrice;
import com.ashish.marketdata.avro.Order;
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
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MarketByPriceSender implements Runnable{

    private volatile boolean running = true;
    private String topic;
    private String serverUrl;
    private ExSimCache cache= ExSimCache.getCache();
    private KafkaProducer<String, String> kafkaProducer;
    private BlockingQueue<MarketByPrice> marketByPriceQueue = new LinkedBlockingQueue<>();
    private MarketByPrice lastSnapshot;

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketByPriceSender.class);

    public MarketByPriceSender(String symbol) {
        this.topic = cache.topic(ExSimCache.TXNTYPE.MARKET_BY_PRICE);
        this.serverUrl = cache.topic(ExSimCache.TXNTYPE.SERVER_URL);
        Properties optionalProperties = new Properties();
        optionalProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        optionalProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        optionalProperties.put(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        optionalProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        kafkaProducer = new KafkaBroker(serverUrl).createProducer((optionalProperties)); // create producer
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
                publishToKafka(marketByPrice, encodedMarketByPrice);
                LOGGER.info("MarketByPrice sent {}" , marketByPrice);
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void publishToKafka(MarketByPrice marketByPrice, String encodedMarketByPrice) {
        String symbol = marketByPrice.getSymbol().toString();
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, symbol, encodedMarketByPrice);
        kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
            if(e==null){
                LOGGER.info("Key {}" ,symbol);
                LOGGER.info("Topic {} " ,recordMetadata.topic());
                LOGGER.info("Partition {}" ,recordMetadata.partition());
                LOGGER.info("Offset {}" ,recordMetadata.offset());
            }else{
                LOGGER.info("Exception Occurred while sending MarketByPrice through kafka... {}", e.getLocalizedMessage());
            }
        });
    }

    public void addORUpdateOrderBook(String symbol, NavigableMap<Double, BlockingQueue<Order>> buyOrdersMap, NavigableMap<Double, BlockingQueue<Order>> sellOrdersMap){
        MarketByPrice marketByPrice = new MarketByPrice();
        marketByPrice.setSymbol(symbol);
        marketByPrice.setBidList(new ArrayList<>());
        marketByPrice.setAskList(new ArrayList<>());

        int buylimit = 0;
        Iterator<Double> buyIterator = buyOrdersMap.descendingKeySet().iterator();
        while (buyIterator.hasNext()) {
            double buyPrice = buyIterator.next();
            BlockingQueue<Order> buyOrder = buyOrdersMap.get(buyPrice);
            long qty = getTotalQty(buyOrder);
            if (qty == 0) {
                //buyIterator.remove();
            } else {
                marketByPrice.getBidList().add(new BidDepth(buyPrice, qty, (long) buyOrder.size()));
            }
            buylimit++;
            if(buylimit==10){
                break;
            }
        }

        int selllimit = 0;
        Iterator<Double> sellIterator = sellOrdersMap.navigableKeySet().iterator();
        while (sellIterator.hasNext()) {
            double sellPrice = sellIterator.next();
            BlockingQueue<Order> sellOrder = sellOrdersMap.get(sellPrice);
            long qty = getTotalQty(sellOrder);
            if (qty == 0) {
                //sellIterator.remove();
            } else {
                marketByPrice.getAskList().add(new AskDepth(sellPrice, qty, (long) sellOrder.size()));
            }
            if(selllimit==10){
                break;
            }
        }
        if(marketByPrice.getBidList().isEmpty() && marketByPrice.getAskList().isEmpty()){
            return;
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
                qty += orders.next().getRemainingQuantity();
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

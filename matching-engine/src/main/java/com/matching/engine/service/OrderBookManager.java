package com.matching.engine.service;


import com.ashish.marketdata.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OrderBookManager implements BookManager {
    private String serverUrl;
    private String tradeTopic;
    private String quoteTopic;
    private String marketPriceTopic;
    private String marketByPriceTopic;
    private String executionTopic;
    private ConcurrentMap<String, OrderMatchingEngine> orderMatchingEngineMap;
    private boolean kafka;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookManager.class);

    public OrderBookManager(String serverUrl, String tradeTopic, String quoteTopic, String marketPriceTopic, String marketByPriceTopic, String executionTopic, boolean kafkaAsCarrier) {
        this.serverUrl = serverUrl;
        this.tradeTopic = tradeTopic;
        this.quoteTopic = quoteTopic;
        this.marketPriceTopic = marketPriceTopic;
        this.marketByPriceTopic = marketByPriceTopic;
        this.executionTopic = executionTopic;
        this.kafka = kafkaAsCarrier;
        this.orderMatchingEngineMap = new ConcurrentHashMap<>();
    }

    @Override
    public void routOrder(final Order order) throws JMSException {
        String symbol = String.valueOf(order.getSymbol());
        OrderMatchingEngine matchingEngine = orderMatchingEngineMap.get(symbol);
        if (matchingEngine == null) {
            matchingEngine = new OrderMatchingEngine(serverUrl, symbol, tradeTopic, quoteTopic, marketPriceTopic, marketByPriceTopic, executionTopic, kafka);
            orderMatchingEngineMap.put(symbol, matchingEngine);
            LOGGER.info("Matching thread created for {}", symbol);
        }
        matchingEngine.addORUpdateOrderBook(order);
    }

    public void stopAllMatchingEngine(){
        Set<String> symbols = orderMatchingEngineMap.keySet();
        symbols.forEach(s->{
            orderMatchingEngineMap.get(s).setRunning(false);
        });
    }
}

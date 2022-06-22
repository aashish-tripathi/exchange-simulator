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
    private ConcurrentMap<String, OrderMatchingEngine> orderMatchingEngineMap;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderBookManager.class);

    public OrderBookManager(String serverUrl) {
        this.serverUrl = serverUrl;
        this.orderMatchingEngineMap = new ConcurrentHashMap<>();
    }

    @Override
    public void routOrder(final Order order) {
        String symbol = String.valueOf(order.getSymbol());
        OrderMatchingEngine matchingEngine =  orderMatchingEngineMap.computeIfAbsent(symbol,k-> new OrderMatchingEngine(serverUrl, symbol));
        if(!orderMatchingEngineMap.containsKey(symbol)) {
            LOGGER.info("Matching thread created for {}", symbol);
        }
        matchingEngine.addORUpdateOrderBook(order);
    }

}

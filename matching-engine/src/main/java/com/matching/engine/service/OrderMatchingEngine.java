package com.matching.engine.service;

import com.ashish.marketdata.avro.MarketPrice;
import com.ashish.marketdata.avro.Order;
import com.ashish.marketdata.avro.Quote;
import com.ashish.marketdata.avro.Trade;
import com.matching.engine.senders.*;
import com.matching.engine.util.Constant;
import com.simulator.senders.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.Calendar;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class OrderMatchingEngine implements Runnable {

    private volatile boolean running = true;
    private final String exchange;
    private final String symbol;
    private final NavigableMap<Double, BlockingQueue<Order>> buyOrders;
    private final NavigableMap<Double, BlockingQueue<Order>> sellOrders;

    // market data objects
    private final TradesSender tradeEngine;
    private final QuotesSender quoteEngine;
    private final MarketPriceSender marketPriceEngine;
    private final MarketByPriceSender marketByPriceSender;
    private final ExecutionsSender executionsSender;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderMatchingEngine.class);

    public OrderMatchingEngine(String serverUrl, String symbol, String tradeTopic, String quoteTopic, String marketPriceTopic, String marketByPriceTopic, String executionsTopic, boolean kafka) throws JMSException {
        this.symbol = symbol;
        this.exchange = "NSE";
        this.buyOrders = new ConcurrentSkipListMap<>();
        this.sellOrders = new ConcurrentSkipListMap<>();
        this.tradeEngine = new TradesSender(serverUrl, tradeTopic, symbol, kafka);
        this.quoteEngine = new QuotesSender(serverUrl, quoteTopic, symbol, kafka);
        this.marketPriceEngine = new MarketPriceSender(serverUrl, marketPriceTopic, symbol, kafka);
        this.marketByPriceSender = new MarketByPriceSender(serverUrl, marketByPriceTopic, symbol, kafka);
        this.executionsSender = new ExecutionsSender(serverUrl, executionsTopic, symbol, kafka);
        new Thread(this).start();
    }

    public void addORUpdateOrderBook(Order order) {
        if (order.getSide() == Constant.BUY) {
            BlockingQueue<Order> buyOrderQueue = buyOrders.computeIfAbsent(order.getLimitPrice(), k -> new ArrayBlockingQueue<>(1024));
            if (order.getLimitPrice() == 0d) { // market order handling
                getModifiedQueue(buyOrderQueue, order);
                LOGGER.info("Buy Market Order Received , modifying Queue position for symbol {}", symbol);
            } else {
                buyOrderQueue.add(order);
            }
        } else if (order.getSide() == Constant.SELL) {
            BlockingQueue<Order> sellOrderQueue = sellOrders.computeIfAbsent(order.getLimitPrice(), k -> new ArrayBlockingQueue<>(1024));
            if (order.getLimitPrice() == 0d) { // market order handling
                getModifiedQueue(sellOrderQueue, order);
                LOGGER.info("Sell Market Order Received , modifying Queue position for symbol {}", symbol);
            } else {
                sellOrderQueue.add(order);
            }
        }
        marketByPriceSender.addORUpdateOrderBook(order.getSymbol().toString(), getBuyOrders(), getSellOrders()); // send book
    }

    private void getModifiedQueue(BlockingQueue<Order> orderQueueByPrice, Order order) {
        BlockingQueue<Order> modifiedQueue = new ArrayBlockingQueue<>(1024);
        modifiedQueue.add(order);
        modifiedQueue.addAll(orderQueueByPrice);
        orderQueueByPrice.clear();
        orderQueueByPrice.addAll(modifiedQueue);
    }

    @Override
    public void run() {
        while (isRunning()) {
            NavigableMap<Double, BlockingQueue<Order>> buyMap = getBuyOrders();
            NavigableMap<Double, BlockingQueue<Order>> sellMap = getSellOrders();
            if (!buyMap.isEmpty() && !sellMap.isEmpty()) {
                Map.Entry<Double, BlockingQueue<Order>> buyEntry = buyMap.lastEntry();
                Map.Entry<Double, BlockingQueue<Order>> selEntry = sellMap.firstEntry();
                double buyPrice = buyEntry.getKey();
                double sellPrice = selEntry.getKey();
                BlockingQueue<Order> buyOrders = buyEntry.getValue();
                BlockingQueue<Order> sellOrders = selEntry.getValue();
                boolean match = match(buyPrice, sellPrice) && !buyOrders.isEmpty() && !sellOrders.isEmpty();
                if (match) {
                    System.out.println("Match found for stock " + symbol);
                    final long buyQty = getTotalQty(buyOrders);
                    final long sellQty = getTotalQty(sellOrders);
                    addQuote(buyPrice, buyQty, sellPrice, sellQty);
                    execute(buyMap, sellMap, buyPrice, buyOrders, sellOrders,sellPrice);
                    marketByPriceSender.addORUpdateOrderBook(symbol, getBuyOrders(), getSellOrders()); // send book
                }
            }
        }
        LOGGER.warn("Thread {} received shutdown signal ", Thread.currentThread().getId());
        LOGGER.warn("Thread {} shutdown completed ", Thread.currentThread().getId());
    }

    private void execute(NavigableMap<Double, BlockingQueue<Order>> buyMap, NavigableMap<Double, BlockingQueue<Order>> sellMap, double buyPrice, BlockingQueue<Order> buyOrders, BlockingQueue<Order> sellOrders, double sellPrice) {

        Order bOrder = buyOrders.peek();
        Order sOrder = sellOrders.peek();
        if (bOrder != null && sOrder != null && bOrder.getRemainingQuantity() >= sOrder.getRemainingQuantity()) {
            long remQty = bOrder.getRemainingQuantity() - sOrder.getRemainingQuantity();
            bOrder.setRemainingQuantity(remQty);
            bOrder.setFilledQuantity(sOrder.getQuantity() + bOrder.getFilledQuantity());
            if (bOrder.getFilledQuantity().equals(bOrder.getQuantity())) {
                bOrder.setOrderStatus("completed");
            } else {
                bOrder.setOrderStatus("partial filled");
            }
            executionsSender.addExecutions(bOrder);

            sOrder.setFilledQuantity(sOrder.getQuantity());
            sOrder.setRemainingQuantity(0L);
            sOrder.setOrderStatus("completed");
            if (sellOrders.size() == 1) {
                sellMap.remove(sellPrice);
                sellMap.clear();
            }
            executionsSender.addExecutions(sOrder);
            sellOrders.remove(sOrder);

            marketPriceUpdate(sOrder);
            addTrade(sOrder, sOrder.getQuantity());

        } else if (bOrder != null && sOrder != null && bOrder.getRemainingQuantity() <= sOrder.getRemainingQuantity()) {
            long remQty = sOrder.getRemainingQuantity() - bOrder.getRemainingQuantity();
            sOrder.setRemainingQuantity(remQty);
            sOrder.setFilledQuantity(bOrder.getQuantity() + sOrder.getFilledQuantity());
            if (sOrder.getFilledQuantity().equals(sOrder.getQuantity())) {
                sOrder.setOrderStatus("completed");
            } else {
                sOrder.setOrderStatus("partial filled");
            }
            executionsSender.addExecutions(sOrder);

            bOrder.setFilledQuantity(bOrder.getQuantity());
            bOrder.setRemainingQuantity(0L);
            bOrder.setOrderStatus("completed");
            if (buyOrders.size() == 1) {
                buyMap.remove(buyPrice);
                buyMap.clear();
            }
            executionsSender.addExecutions(bOrder);
            buyOrders.remove(bOrder);

            marketPriceUpdate(bOrder);
            addTrade(bOrder, bOrder.getQuantity());
        }
    }

    private boolean match(double buyPrice, double sellPrice) {
        return buyPrice >= sellPrice;
    }

    private void executeTransaction(double buyPrice, double sellPrice, BlockingQueue<Order> buyQueue, BlockingQueue<Order> sellQueue) {
        System.err.println("Match found for stock " + symbol);
        long buyQty = getTotalQty(buyQueue);
        long sellQty = getTotalQty(sellQueue);
        addQuote(buyPrice, buyQty, sellPrice, sellQty);
        if (buyQty > sellQty) {
            Order bOrder = buyQueue.peek();
            Order sOrder = sellQueue.peek();
            if (bOrder != null && sOrder != null && bOrder.getQuantity() > sOrder.getQuantity()) {
                long remQty = bOrder.getQuantity() - sOrder.getQuantity();
                bOrder.setRemainingQuantity(remQty);
                bOrder.setFilledQuantity(sOrder.getQuantity());
                bOrder.setOrderStatus("partial filled");

                sOrder.setFilledQuantity(sOrder.getQuantity());
                sOrder.setRemainingQuantity(0l);
                sOrder.setOrderStatus("completed");
                sellQueue.remove(sOrder);

                marketPriceUpdate(sOrder);
                addTrade(sOrder, sOrder.getQuantity());

            } else if (bOrder != null && sOrder != null && bOrder.getQuantity() < sOrder.getQuantity()) {
                long remQty = sOrder.getQuantity() - bOrder.getQuantity();
                sOrder.setRemainingQuantity(remQty);
                sOrder.setFilledQuantity(bOrder.getQuantity());
                sOrder.setOrderStatus("partial filled");

                bOrder.setFilledQuantity(bOrder.getQuantity());
                bOrder.setRemainingQuantity(0l);
                bOrder.setOrderStatus("completed");
                buyQueue.remove(sOrder);

                marketPriceUpdate(bOrder);
                addTrade(bOrder, bOrder.getQuantity());
            }
            marketByPriceSender.addORUpdateOrderBook(symbol, getBuyOrders(), getSellOrders()); // send book

        } else if (buyQty < sellQty) {

        } else if (buyQty == sellQty) {

        }

    }


    private void marketPriceUpdate(Order order) {
        MarketPrice marketPrice = marketPriceEngine.marketPrice(symbol);
        if (marketPrice.getOpen() == null || marketPrice.getOpen() == 0.0) {
            marketPrice.setOpen(order.getLimitPrice());
        }
        if (marketPrice.getHigh() == null || marketPrice.getHigh() < order.getLimitPrice()) {
            marketPrice.setHigh(order.getLimitPrice());
        }
        if (marketPrice.getLow() == null || marketPrice.getLow() > order.getLimitPrice()) {
            marketPrice.setLow(order.getLimitPrice());
        }
        if (marketPrice.getVolume() == null) {
            marketPrice.setVolume(order.getQuantity());
        } else {
            long vol = marketPrice.getVolume();
            marketPrice.setVolume(vol + order.getQuantity());
        }
        marketPrice.setExchange(order.getExchange());
        marketPrice.setSymbol(symbol);
        marketPrice.setUperCircuit(Constant.upperCircuit);
        marketPrice.setLowerCircuit(Constant.lowerCircuit);
        marketPrice.setClose(order.getLimitPrice());
        marketPrice.setLastPrice(order.getLimitPrice());
        marketPrice.setLastTradeSize(order.getQuantity());
        marketPrice.setLastTradeTime(Calendar.getInstance().getTimeInMillis());
        marketPriceEngine.addORUpdateMarketPrice(marketPrice);
        // printBook();
    }

    private void addTrade(Order buyOrd, long sQty) {
        Trade trade = new Trade();
        trade.setPrice(buyOrd.getLimitPrice());
        trade.setSize(sQty);
        trade.setSymbol(buyOrd.getSymbol());
        trade.setExchange(buyOrd.getExchange());
        trade.setTime(Calendar.getInstance().getTimeInMillis());
        tradeEngine.addTrade(trade);
    }

    private void addQuote(double buyPrice, long buyQty, double sellPrice, long sellQty) {
        Quote quote = new Quote();
        quote.setBidprice(buyPrice);
        quote.setBidsize(buyQty);
        quote.setAskprice(sellPrice);
        quote.setAsksize(sellQty);
        quote.setSymbol(symbol);
        quote.setExchange(exchange);
        quote.setTime(Calendar.getInstance().getTimeInMillis());
        quoteEngine.addQuote(quote);
    }

    private boolean match(Order buyOrder, Order sellOrder) {
        return (buyOrder != null && sellOrder != null) && ((buyOrder.getLimitPrice() >= sellOrder.getLimitPrice()));
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(final boolean running) {
        this.running = running;
        marketPriceEngine.setRunning(running);
        marketByPriceSender.setRunning(running);
        tradeEngine.setRunning(running);
        quoteEngine.setRunning(running);
    }

    public NavigableMap<Double, BlockingQueue<Order>> getBuyOrders() {
        return buyOrders;
    }

    public NavigableMap<Double, BlockingQueue<Order>> getSellOrders() {
        return sellOrders;
    }

    public void printBook() {
        LOGGER.info("Symbol " + symbol + "");
        LOGGER.info("  Bid " + "  Size  ");
        for (double buyPrice : buyOrders.navigableKeySet()) {
            BlockingQueue<Order> buyOrder = buyOrders.get(buyPrice);
            long qty = getTotalQty(buyOrder);
            if (qty == 0) {
                //buyIterator.remove();
            } else {
                LOGGER.info("  " + buyPrice + "   " + qty + "  ");
            }
        }
        LOGGER.info("  Offer " + " Size  ");
        for (double sellPrice : sellOrders.navigableKeySet()) {
            BlockingQueue<Order> sellOrder = sellOrders.get(sellPrice);
            long qty = getTotalQty(sellOrder);
            if (qty == 0) {
                //sellIterator.remove();
            } else {
                LOGGER.info("  " + sellPrice + "   " + qty + "  ");
            }
        }
    }

    private long getTotalQty(BlockingQueue<Order> value) {
        long qty = 0;
        for (Order order : value) {
            qty += order.getRemainingQuantity();
        }
        return qty;

    }
}


package com.simulator.service;

import com.ashish.marketdata.avro.MarketPrice;
import com.ashish.marketdata.avro.Order;
import com.ashish.marketdata.avro.Quote;
import com.ashish.marketdata.avro.Trade;
import com.simulator.senders.MarketByPriceSender;
import com.simulator.senders.MarketPriceSender;
import com.simulator.senders.QuotesSender;
import com.simulator.senders.TradesSender;
import com.simulator.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;

public class OrderMatchingEngine implements Runnable {

    private volatile boolean running = true;
    private final String symbol;
    private final NavigableMap<Double, BlockingQueue<Order>> buyOrders;
    private final NavigableMap<Double, BlockingQueue<Order>> sellOrders;

    // market data objects
    private final TradesSender tradeEngine;
    private final QuotesSender quoteEngine;
    private final MarketPriceSender marketPriceEngine;
    private final MarketByPriceSender marketByPriceSender;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderMatchingEngine.class);

    public OrderMatchingEngine(String serverUrl, String symbol, String tradeTopic, String quoteTopic, String marketPriceTopic, String marketByPriceTopic, boolean kafka) throws JMSException {
        this.symbol = symbol;
        this.buyOrders = new ConcurrentSkipListMap<>();
        this.sellOrders = new ConcurrentSkipListMap<>();
        tradeEngine = new TradesSender(serverUrl,tradeTopic,symbol, kafka);
        quoteEngine = new QuotesSender(serverUrl, quoteTopic,symbol,kafka);
        marketPriceEngine = new MarketPriceSender(serverUrl, marketPriceTopic,symbol,kafka);
        marketByPriceSender = new MarketByPriceSender(serverUrl, marketByPriceTopic,symbol,kafka);
        new Thread(this).start();
    }

    public void addORUpdateOrderBook(Order order) {
        if (order.getSide() == Constant.BUY) {
            BlockingQueue<Order> buyOrderQueue = buyOrders.computeIfAbsent(order.getLimitPrice(), k -> new ArrayBlockingQueue<>(1024));
            if (order.getLimitPrice() == 0d) { // market order handling
                getModifiedQueue(buyOrderQueue, order);
                LOGGER.info("Buy Market Order Received , modifying Queue position for symbol {}",symbol);
            } else {
                buyOrderQueue.add(order);
            }
        } else if (order.getSide() == Constant.SELL) {
            BlockingQueue<Order> sellOrderQueue = sellOrders.computeIfAbsent(order.getLimitPrice(), k -> new ArrayBlockingQueue<>(1024));
            if (order.getLimitPrice() == 0d) { // market order handling
                getModifiedQueue(sellOrderQueue, order);
                LOGGER.info("Sell Market Order Received , modifying Queue position for symbol {}",symbol);
            } else {
                sellOrderQueue.add(order);
            }
        }
        marketByPriceSender.addORUpdateOrderBook(order.getSymbol().toString(),buyOrders, sellOrders); // send book
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
                double buyPrice = buyMap.descendingKeySet().iterator().next();
                double sellPrice = sellMap.navigableKeySet().iterator().next();
                BlockingQueue<Order> buyQueue = buyMap.get(buyPrice);
                BlockingQueue<Order> sellQueue = sellMap.get(sellPrice);
                Order buyOrd = buyQueue.peek();
                Order sellOrd = sellQueue.peek();
                boolean match = match(buyOrd, sellOrd);
                if (match) {
                    executeTransaction(buyMap, sellMap, buyPrice, sellPrice, buyQueue, sellQueue, buyOrd, sellOrd);
                }
            }
        }
    }

    private void executeTransaction(NavigableMap<Double, BlockingQueue<Order>> buyMap, NavigableMap<Double, BlockingQueue<Order>> sellMap, double buyPrice, double sellPrice, BlockingQueue<Order> buyQueue, BlockingQueue<Order> sellQueue, Order buyOrd, Order sellOrd) {
        System.err.println("Match found for stock " + buyOrd.getSymbol());
        addQuote(buyOrd, sellOrd);
        long bQty = buyOrd.getQuantity();
        long sQty = sellOrd.getQuantity();
        if (bQty > sQty) {
            long remQty = bQty - sQty;
            marketPriceUpdate(sellOrd);
            buyOrd.setQuantity(remQty);
            addTrade(buyOrd, sQty);
            if (sellQueue.size() == 1) {
                sellMap.remove(sellPrice);
            } else {
                sellQueue.remove(sellOrd);// update sell position
            }
        } else if (bQty < sQty) {
            long remQty = sQty - bQty;
            marketPriceUpdate(buyOrd);
            sellOrd.setQuantity(remQty);
            addTrade(buyOrd, bQty);
            if (buyQueue.size() == 1) {
                buyMap.remove(buyPrice);
            } else {
                buyQueue.remove(buyOrd);// update buy position
            }
        } else {
            buyQueue.remove(buyOrd);// update buy position
            sellQueue.remove(sellOrd); // update sell position
            addTrade(buyOrd, bQty);
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
        printBook();
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

    private void addQuote(Order buyOrd, Order sellOrder ) {
        Quote quote = new Quote();
        quote.setBidprice(buyOrd.getLimitPrice());
        quote.setBidsize(buyOrd.getQuantity());
        quote.setAskprice(sellOrder.getLimitPrice());
        quote.setAsksize(sellOrder.getQuantity());
        quote.setSymbol(symbol);
        quote.setExchange(buyOrd.getExchange());
        quote.setTime(Calendar.getInstance().getTimeInMillis());
        quoteEngine.addQuote(quote);
    }

    private boolean match(Order buyOrder, Order sellOrder) {
        return (buyOrder != null && sellOrder != null) && ((buyOrder.getLimitPrice() >= sellOrder.getLimitPrice()));
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public NavigableMap<Double, BlockingQueue<Order>> getBuyOrders() {
        return buyOrders;
    }

    public NavigableMap<Double, BlockingQueue<Order>> getSellOrders() {
        return sellOrders;
    }

    public void printBook() {
        System.out.println("Symbol " + symbol + "");
        System.out.println("  Bid " + "  Size  ");
        for (double buyPrice : buyOrders.navigableKeySet()) {
            BlockingQueue<Order> buyOrder = buyOrders.get(buyPrice);
            long qty = getTotalQty(buyOrder);
            if (qty == 0) {
                //buyIterator.remove();
            } else {
                System.out.println("  " + buyPrice + "   " + qty + "  ");
            }
        }
        System.out.println("  Offer " + " Size  ");
        for (double sellPrice : sellOrders.navigableKeySet()) {
            BlockingQueue<Order> sellOrder = sellOrders.get(sellPrice);
            long qty = getTotalQty(sellOrder);
            if (qty == 0) {
                //sellIterator.remove();
            } else {
                System.out.println("  " + sellPrice + "   " + qty + "  ");
            }
        }
    }

    private long getTotalQty(BlockingQueue<Order> value) {
        long qty = 0;
        for (Order order : value) {
            qty += order.getQuantity();
        }
        return qty;

    }
}

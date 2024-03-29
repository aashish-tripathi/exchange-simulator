package com.matching.engine;

import com.matching.engine.receivers.OrderReceiver;
import com.matching.engine.senders.MarketByPriceSender;
import com.matching.engine.service.OrderBookManager;
import com.matching.engine.util.ExSimCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class StartMatchingEngineApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketByPriceSender.class);

    public static void main(String[] args) throws IOException {
        String configPath;
        if (args.length == 0) {
            LOGGER.warn("Config file not provided, loading file from default directory");
            configPath = "/exsim-me.properties";
        } else {
            configPath = args[0];
        }
        Properties properties = new Properties();
        InputStream inputStream = StartMatchingEngineApp.class.getResourceAsStream(configPath);
        properties.load(inputStream);

        ExSimCache cache= ExSimCache.getCache();

        String serverUrl =properties.getProperty("exsim.kafka.bootstrap.servers");
        cache.add(ExSimCache.TXNTYPE.SERVER_URL,serverUrl);
        final String orderTopic = properties.getProperty("exsim.nse.ordertopic");
        cache.add(ExSimCache.TXNTYPE.ORDER,orderTopic);
        final String tradeTopic = properties.getProperty("exsim.nse.tradetopic");
        cache.add(ExSimCache.TXNTYPE.TRADE,tradeTopic);
        final String quoteTopic = properties.getProperty("exsim.nse.quotestopic");
        cache.add(ExSimCache.TXNTYPE.QUOTE,quoteTopic);
        final String marketPriceTopic = properties.getProperty("exsim.nse.marketpricetopic");
        cache.add(ExSimCache.TXNTYPE.MARKET_PRICE,marketPriceTopic);
        final String marketByPriceTopic = properties.getProperty("exsim.nse.marketbypricetopic");
        cache.add(ExSimCache.TXNTYPE.MARKET_BY_PRICE,marketByPriceTopic);
        final String executionTopic = properties.getProperty("exsim.nse.executionstopic");
        cache.add(ExSimCache.TXNTYPE.EXECUTION,executionTopic);
        final int workers = Integer.parseInt(properties.getProperty("exsim.nse.consumer.threads"));

        ExecutorService service = Executors.newFixedThreadPool(10, r -> {
            Thread t = new Thread(r, "Order Consumer...");
            t.setUncaughtExceptionHandler((t1, e) -> LoggerFactory.getLogger(t1.getName()).error(e.getMessage(), e));
            return t;
        });
        final OrderBookManager orderBookManager = new OrderBookManager();
        final CountDownLatch latch = new CountDownLatch(workers);
        final List<OrderReceiver> receivers = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            OrderReceiver orderReceiver = new OrderReceiver(orderBookManager, latch);
            receivers.add(orderReceiver);
        }
        AtomicInteger integer = new AtomicInteger(0);

        receivers.forEach(r -> {
            service.submit(r);
            LOGGER.info("OrderReceivers {} has started {} ",integer.incrementAndGet(), r);
        });


        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            receivers.forEach(r->r.shutdown());
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOGGER.info("Application has exited...");
        }));

        try {
            latch.await();
        }catch (Exception e){
            LOGGER.error("Application got interrupted!");
        }finally {
            LOGGER.error("Application is closing!");
        }
    }
}

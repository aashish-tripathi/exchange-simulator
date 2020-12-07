package com.simulator;

import com.simulator.receivers.OrderReceiver;
import com.simulator.senders.MarketByPriceSender;
import com.simulator.service.OrderBookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class StartMatchingEngineApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(MarketByPriceSender.class);

    public static void main(String[] args) throws JMSException, InterruptedException, IOException {
        String configPath = null;
        if (args.length == 0) {
            LOGGER.warn("Config file not provided, loading file from default directory");
            configPath = "/exsim-me.properties";
        } else {
            configPath = args[0];
        }
        Properties properties = new Properties();
        InputStream inputStream = StartMatchingEngineApp.class.getResourceAsStream(configPath);
        properties.load(inputStream);

        final boolean kafkaAsCarrier = true;
        String serverUrl = kafkaAsCarrier ? properties.getProperty("exsim.kafka.bootstrap.servers") : properties.getProperty("exsim.tibcoems.serverurl");

        final String orderTopic = properties.getProperty("exsim.nse.ordertopic");
        final String tradeTopic = properties.getProperty("exsim.nse.tradetopic");
        final String quoteTopic = properties.getProperty("exsim.nse.quotestopic");
        final String marketPriceTopic = properties.getProperty("exsim.nse.marketpricetopic");
        final String marketByPriceTopic = properties.getProperty("exsim.nse.marketbypricetopic");
        final String executionTopic = properties.getProperty("exsim.nse.marketbypricetopic");
        final int workers = Integer.parseInt(properties.getProperty("exsim.nse.consumer.threads"));

        ExecutorService service = Executors.newFixedThreadPool(10, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "Order Consumer...");
                t.setUncaughtExceptionHandler((t1, e) -> LoggerFactory.getLogger(t1.getName()).error(e.getMessage(), e));
                return t;
            }
        });
        final OrderBookManager orderBookManager = new OrderBookManager(serverUrl, tradeTopic, quoteTopic, marketPriceTopic, marketByPriceTopic, executionTopic, kafkaAsCarrier);
        final CountDownLatch latch = new CountDownLatch(workers);
        final List<OrderReceiver> receivers = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            OrderReceiver orderReceiver = new OrderReceiver(serverUrl, orderTopic, orderBookManager, kafkaAsCarrier,latch);
            receivers.add(orderReceiver);
        }
        receivers.forEach(r -> service.submit(r));
        LOGGER.info("{} OrderReceivers has started...", workers);

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
/*
        Scanner scanner = new Scanner(System.in);
        LOGGER.warn("Enter to stop this engine...");
        scanner.nextLine();
        receivers.forEach(r -> r.setRunning(false));
*/

    }
}

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
import java.util.Scanner;
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
        final int workers = Integer.parseInt(properties.getProperty("exsim.nse.consumer.threads"));

        ExecutorService service = Executors.newFixedThreadPool(10, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "Order Consumer ");
                t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                    @Override
                    public void uncaughtException(Thread t, Throwable e) {
                        LoggerFactory.getLogger(t.getName()).error(e.getMessage(), e);
                    }
                });
                return t;
            }
        });
        final OrderBookManager orderBookManager = new OrderBookManager(serverUrl, tradeTopic, quoteTopic, marketPriceTopic, marketByPriceTopic, kafkaAsCarrier);
        final List<OrderReceiver> receivers = new ArrayList<>();
        for (int i = 0; i < workers; i++) {
            OrderReceiver orderReceiver = new OrderReceiver(serverUrl, orderTopic, orderBookManager, kafkaAsCarrier);
            receivers.add(orderReceiver);
        }
        receivers.forEach(r -> service.submit(r));
        LOGGER.info("{} OrderReceivers has started...", workers);

        Scanner scanner = new Scanner(System.in);
        LOGGER.warn("Enter to stop this engine...");
        scanner.nextLine();
        receivers.forEach(r -> r.setRunning(false));

    }
}

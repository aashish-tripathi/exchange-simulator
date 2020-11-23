package com.md.client.service;

import com.md.client.senders.OrderSender;
import com.md.client.util.Throughput;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class OrderSimulator {
    private boolean kafka;
    private String topic;
    private String []symbols;
    private String serverUrl;
    private ExecutorService service;
    private List<OrderSender> workerThreads;
    private Throughput throughputWorker;
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderSimulator.class);

    public OrderSimulator(String serverUrl, final String topic, boolean kafka) {
        this.serverUrl=serverUrl;
        this.topic = topic;
        this.kafka = kafka;
        this.service = Executors.newFixedThreadPool(10, new ThreadFactory() {
            @Override
            public Thread newThread(@NotNull Runnable r) {
                return  new Thread(r, "Order Sending Thread");
            }
        });
    }

    public void startSimulator(final String[] symbols, final String exchange, final String brokerName, final String brokerId, final String clientId, final String clientName, int workers) throws JMSException {
        workerThreads = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            OrderSender senderEMS = new OrderSender(serverUrl, topic, symbols, exchange, brokerName, brokerId, clientId, clientName, kafka,throughputWorker);
            workerThreads.add(senderEMS);
        }
        workerThreads.forEach(t -> service.submit(t));
    }

    public void shutDown(){
        if(workerThreads !=null) {
            workerThreads.forEach(t -> t.setRunning(false));
        }
        service.shutdown();
        LOGGER.info("All threads has been shutdown!");
    }
    public void startManualMode(){

    }
}

package com.md.client;

import com.ashish.marketdata.avro.Order;
import com.md.client.service.OrderSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class StartOrderSimulator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartOrderSimulator.class);

    public static void main(String[] args) throws IOException, JMSException, InterruptedException {
        String configPath = null;
        if (args.length == 0) {
            LOGGER.warn("Config file not provided, loading file from default directory");
            configPath = "/order-sim.properties";
        } else {
            configPath = args[0];
        }

        final boolean kafkaAsCarrier = true;

        Properties properties = new Properties();
        InputStream inputStream = StartOrderSimulator.class.getResourceAsStream(configPath);
        properties.load(inputStream);
        String serverUrl = kafkaAsCarrier ? properties.getProperty("exsim.kafka.bootstrap.servers") : properties.getProperty("exsim.tibcoems.serverurl");
        String orderTopic = properties.getProperty("exsim.tibcoems.ordertopic");
        String[] clients = properties.getProperty("exsim.order.sim.clients").split(",");
        String[] stocks = properties.getProperty("exsim.order.sim.symbols").split(",");
        String exchange = properties.getProperty("exsim.order.sim.exchange");
        String brokerName = properties.getProperty("exsim.order.sim.broker");
        String brokerId = properties.getProperty("exsim.order.sim.brokerId");
        String[] clientDetails = clients[0].split("-");
        int workers = Integer.parseInt(properties.getProperty("exsim.order.sim.workers"));
        String runningMode = properties.getProperty("exsim.running.mode");

        OrderSimulator orderSimulator = new OrderSimulator(serverUrl, orderTopic, kafkaAsCarrier);
        if (runningMode != null && runningMode.equalsIgnoreCase("auto")) {
            runAutomationMode(stocks, exchange, brokerName, brokerId, clientDetails, workers, orderSimulator);
        } else {
            runManualMode(stocks, exchange, brokerName, brokerId, clientDetails, workers, orderSimulator);
        }
    }

    private static void runManualMode(String[] stocks, String exchange, String brokerName, String brokerId, String[] clientDetails, int workers, OrderSimulator orderSimulator) throws JMSException, InterruptedException {
        BlockingQueue<Order> orderQueueForManual = new ArrayBlockingQueue<Order>(1024);
        orderSimulator.startSimulatorInManualMode(stocks, exchange, brokerName, brokerId, clientDetails[0], clientDetails[1], workers, true, orderQueueForManual);
        LOGGER.info("Order Simulator has been started in manual mode {}", Calendar.getInstance().getTime());
        String data = "";
        Scanner scanner = new Scanner(System.in);
        boolean stopManual = true;
        LOGGER.warn("Enter exit to stop");
        while (stopManual) {
            System.out.println("Enter symbol ");
            Order order = new Order();
            data = scanner.nextLine();
            stopManual = isStopManual(data, stopManual);
            order.setSymbol(data);
            System.out.println("Enter qunantity ");
            data = scanner.nextLine();
            stopManual = isStopManual(data, stopManual);
            order.setQuantity(Long.parseLong(data));
            System.out.println("Enter price ");
            data = scanner.nextLine();
            stopManual = isStopManual(data, stopManual);
            order.setLimitPrice(Double.parseDouble(data));
            System.out.println("Enter side (Buy 1, Sell 2 ) ");
            data = scanner.nextLine();
            stopManual = isStopManual(data, stopManual);
            order.setSide(Integer.parseInt(data));
            order.setExchange("NSE");
            order.setBrokerId("Zero001");
            order.setBrokerName("Zerodha");
            order.setClientId("Black001");
            order.setClientName("Black");
            order.setOrderStatus("open");
            order.setFilledQuantity(0l);
            order.setRemainingQuantity(order.getQuantity());
            order.setOrdertime(Calendar.getInstance().getTimeInMillis());
            order.setOrderId(UUID.randomUUID().toString());
            orderQueueForManual.put(order);
        }
        orderSimulator.shutDown();
    }

    private static void runAutomationMode(String[] stocks, String exchange, String brokerName, String brokerId, String[] clientDetails, int workers, OrderSimulator orderSimulator) throws JMSException {
        orderSimulator.startSimulatorInAutomaticMode(stocks, exchange, brokerName, brokerId, clientDetails[0], clientDetails[1], workers, false, null);
        LOGGER.info("Order Simulator has been started in automatic mode {}", Calendar.getInstance().getTime());
        Scanner scanner = new Scanner(System.in);
        LOGGER.warn("Enter exit to stop automatic mode ");
        String run = scanner.nextLine();
        while (!run.equals("exit")) {
            orderSimulator.shutDown();
            System.exit(-1);
        }
    }

    private static boolean isStopManual(String data, boolean stopManual) {
        if (data.equals("exit")) {
            stopManual = false;
            System.exit(-1);
        }
        return stopManual;
    }
}

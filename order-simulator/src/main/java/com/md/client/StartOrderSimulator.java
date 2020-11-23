package com.md.client;

import com.md.client.service.OrderSimulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Properties;
import java.util.Scanner;

public class StartOrderSimulator {

    private static final Logger LOGGER = LoggerFactory.getLogger(StartOrderSimulator.class);

    public static void main(String[] args) throws IOException, JMSException {

        String configPath =null;
        if(args.length ==0){
            LOGGER.warn("Config file not provided, loading file from default directory");
            configPath = "/order-sim.properties";
        }else{
            configPath = args[0];
        }

        final boolean kafkaAsCarrier = true;

        Properties properties= new Properties();
        InputStream inputStream = StartOrderSimulator.class.getResourceAsStream(configPath);
        properties.load(inputStream);

        String serverUrl = kafkaAsCarrier ? properties.getProperty("exsim.kafka.bootstrap.servers"): properties.getProperty("exsim.tibcoems.serverurl");

        String orderTopic = properties.getProperty("exsim.tibcoems.ordertopic");
        String [] clients = properties.getProperty("exsim.order.sim.clients").split(",");
        String [] stocks = properties.getProperty("exsim.order.sim.symbols").split(",");
        String exchange = properties.getProperty("exsim.order.sim.exchange");
        String brokerName = properties.getProperty("exsim.order.sim.broker");
        String brokerId = properties.getProperty("exsim.order.sim.brokerId");
        int workers = Integer.parseInt(properties.getProperty("exsim.order.sim.workers"));

        String [] clientDetails = clients[0].split("-");
        OrderSimulator orderSimulator = new OrderSimulator(serverUrl,orderTopic, kafkaAsCarrier);
        orderSimulator.startSimulator(stocks, exchange, brokerName, brokerId, clientDetails[0],clientDetails[1],workers);
        LOGGER.info("Order Simulator has been started {}", Calendar.getInstance().getTime());

        Scanner scanner= new Scanner(System.in);
        LOGGER.warn("Enter to stop");
        scanner.nextLine();
        orderSimulator.shutDown();



    }
}

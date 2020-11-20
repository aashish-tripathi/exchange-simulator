package com.simulator.senders;

import com.simulator.broker.EMSBroker;
import com.simulator.util.Utility;

import javax.jms.JMSException;

public class ExecutionsSender implements Runnable {

    private EMSBroker emsBroker;

    public ExecutionsSender(String topic, String symbol) throws JMSException {
        emsBroker = new EMSBroker(null, null,null);
        emsBroker.createProducer(topic,true);
        new Thread(this).start();
        System.out.println("ExecutionsSender has started "+symbol);
    }

    @Override
    public void run() {

    }
}

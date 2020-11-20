package com.simulator.service;


import com.ashish.marketdata.avro.Order;

import javax.jms.JMSException;
import java.util.LinkedList;
import java.util.SortedMap;

public interface BookManager {
    public void routOrder(Order order) throws JMSException;
}

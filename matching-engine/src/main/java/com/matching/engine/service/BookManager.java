package com.matching.engine.service;


import com.ashish.marketdata.avro.Order;

import javax.jms.JMSException;

public interface BookManager {
    public void routOrder(Order order) throws JMSException;
}

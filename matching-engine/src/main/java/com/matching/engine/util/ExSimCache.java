package com.matching.engine.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ExSimCache {

    private static ExSimCache cache=new ExSimCache();
    private Map<TXNTYPE,String> topicCache = new HashMap<>();

    private ExSimCache(){

    }

    public void add(TXNTYPE txntype, String topic){
        Objects.nonNull(txntype);
        Objects.nonNull(topic);
        topicCache.put(txntype,topic);
    }

    public String topic(TXNTYPE txntype){
        Objects.nonNull(txntype);
        return topicCache.get(txntype);
    }

    public enum TXNTYPE{
        ORDER,EXECUTION,TRADE,QUOTE,MARKET_PRICE,MARKET_BY_PRICE,SERVER_URL
    }

    public static ExSimCache getCache(){
        return cache;
    }
}

package com.matching.engine.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class EXSIMCache {

    private static EXSIMCache cache=new EXSIMCache();
    private Map<TXNTYPE,String> topicCache = new HashMap<>();

    private EXSIMCache(){

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
        ORDER,EXECUTION,TRADE,QUOTE,MARKET_PRICE,MARKET_BY_PRICE
    }

    public static EXSIMCache getCache(){
        return cache;
    }
}

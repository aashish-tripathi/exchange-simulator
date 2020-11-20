package com.md.client.service;

import com.md.client.util.Utility;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

public class PriceRange {

    private ConcurrentMap<String,Circuit> stockCircuit;
    private static PriceRange priceRange = new PriceRange();

    private  PriceRange(){
        stockCircuit = new ConcurrentHashMap<>();
    }

    public static PriceRange getInstance(){
        return priceRange;
    }

    public Circuit getTodaysSymbolCircuit(final String symbol){
     if(!stockCircuit.containsKey(symbol)){
         ThreadLocalRandom random = ThreadLocalRandom.current();
         double price =  random.nextDouble(10, 100);
         price =Double.valueOf(Utility.dataFormat.format(price));
         stockCircuit.put(symbol, new Circuit(price, price-5,price+5));
     }
        return stockCircuit.get(symbol);
    }

    public static class Circuit{
        private volatile double priceRange;
        private volatile double lowerCircuit;
        private volatile double upperCircuit;

        public Circuit() {
        }

        public double getPriceRange() {
            return priceRange;
        }

        public Circuit(double priceRange, double lowerCircuit, double upperCircuit) {
            this.priceRange = priceRange;
            this.lowerCircuit = lowerCircuit;
            this.upperCircuit = upperCircuit;
        }

        public void setPriceRange(double priceRange) {
            this.priceRange = priceRange;
        }

        public double getLowerCircuit() {
            return lowerCircuit;
        }

        public void setLowerCircuit(double lowerCircuit) {
            this.lowerCircuit = lowerCircuit;
        }

        public double getUpperCircuit() {
            return upperCircuit;
        }

        public void setUpperCircuit(double upperCircuit) {
            this.upperCircuit = upperCircuit;
        }

        @Override
        public String toString() {
            return "Circuit{" +
                    "priceRange=" + priceRange +
                    ", lowerCircuit=" + lowerCircuit +
                    ", upperCircuit=" + upperCircuit +
                    '}';
        }
    }
}

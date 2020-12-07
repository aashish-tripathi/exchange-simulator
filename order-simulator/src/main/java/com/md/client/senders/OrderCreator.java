package com.md.client.senders;

import com.ashish.marketdata.avro.Order;
import com.md.client.service.PriceRange;
import com.md.client.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class OrderCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderCreator.class);
    private static PriceRange priceRange = PriceRange.getInstance();

    public static Order createSingleOrder(@org.jetbrains.annotations.NotNull final String symbol, final String exchange, final String brokerName, final String brokerId, final String clientId, final String clientName){
        Order order = null;
        ThreadLocalRandom random = ThreadLocalRandom.current();
        UUID orderId = UUID.randomUUID();

        PriceRange.Circuit circuit = priceRange.getTodaysSymbolCircuit(symbol);
        double price = circuit.getPriceRange();
        final double sendingPrice = Double.valueOf(Utility.dataFormat.format(random.nextDouble(price, price + 5)));
        double lowerCircuit = circuit.getLowerCircuit();
        double upperCircuit = circuit.getUpperCircuit();
        if (sendingPrice >= lowerCircuit && sendingPrice <= upperCircuit && spreadValidity(sendingPrice)) {
            long qty =  random.nextLong(5000);
            order = Order.newBuilder()
                    .setSymbol(symbol.toString())
                    .setExchange(exchange).setBrokerId(brokerId)
                    .setBrokerName(brokerName)
                    .setBrokerId(brokerId)
                    .setClientId(clientId)
                    .setQuantity(qty)
                    .setLimitPrice(sendingPrice)
                    .setSide(random.nextInt(1, 3))
                    .setOrderStatus("open")
                    .setFilledQuantity(0l)
                    .setRemainingQuantity(qty)
                    .setOrdertime(Calendar.getInstance().getTimeInMillis())
                    .setOrderId(UUID.randomUUID().toString())
                    .setClientName(clientName).build();

        } else {
            LOGGER.warn("Order has been rejected by broker due to out of range price for stock {} , circuit {} OR spread check failed " , symbol, circuit);
            return null;
        }
        return  order;
    }

    private static boolean spreadValidity(double sendingPrice) {
        return ((sendingPrice%5)*10)%5==0;
    }

}


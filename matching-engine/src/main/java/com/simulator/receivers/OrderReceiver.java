package com.simulator.receivers;

import com.ashish.marketdata.avro.Order;
import com.simulator.broker.EMSBroker;
import com.simulator.broker.KafkaBroker;
import com.simulator.service.BookManager;
import com.simulator.util.Constant;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;

public class OrderReceiver implements Runnable {

    private boolean kafka;
    private BookManager bookManager;
    private String topic;
    private EMSBroker emsBroker;
    private KafkaConsumer<String,String> kafkaConsumer;
    private volatile boolean running = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderReceiver.class);

    public OrderReceiver(String serverUrl, String topic, BookManager bookManager, boolean kafka) throws JMSException {
        this.topic = topic;
        this.kafka = kafka;
        this.bookManager = bookManager;
        if(!kafka) {
            emsBroker = new EMSBroker(null, null, null);
            emsBroker.createConsumer(topic, true);
        }else{
            this.kafkaConsumer = new KafkaBroker(serverUrl).createConsumer(null);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }
    }

    @Override
    public void run() {
        int ackMode = Session.AUTO_ACKNOWLEDGE;
        while (isRunning()) {
            if(!kafka){
                try {
                    consumeFromEMS();
                } catch (JMSException e) {
                    LOGGER.error(e.getLocalizedMessage());
                }
            }else{
                try {
                    consumeFromKafka();
                } catch (JMSException e) {
                    LOGGER.error(e.getLocalizedMessage());
                }
            }
        }
        LOGGER.warn(Thread.currentThread().getName()+ " has been stopped");
    }

    private void consumeFromKafka() throws JMSException{
        ConsumerRecords<String,String> records=kafkaConsumer.poll(Duration.ofMillis(10));
        for(ConsumerRecord<String,String> record: records){
            String symbol = record.key();
            String data = record.value();
            byte[] decoded = Base64.getDecoder().decode(data);
            Order order = deSerealizeAvroHttpRequestJSON(decoded);
            bookManager.routOrder(order);
            LOGGER.info("Key: "+ symbol + ", Value:" +data);
            LOGGER.info("Partition:" + record.partition()+",Offset:"+record.offset());
        }
    }

    private void consumeFromEMS() throws JMSException {
        Message msg = emsBroker.consumer().receive();
        if (msg == null)
            return;
        if (msg instanceof TextMessage) {
            TextMessage message = (TextMessage) msg;
            byte[] decoded = Base64.getDecoder().decode(message.getText());
            Order order = deSerealizeAvroHttpRequestJSON(decoded);
            bookManager.routOrder(order);
        }
    }

    private boolean isInCircuit(Order order) {
        return  order.getLimitPrice() > Constant.lowerCircuit && order.getLimitPrice() < Constant.upperCircuit;
    }

    public Order deSerealizeAvroHttpRequestJSON(byte[] data) {
        DatumReader<Order> reader
                = new SpecificDatumReader<>(Order.class);
        Decoder decoder = null;
        try {
            decoder = DecoderFactory.get().jsonDecoder(Order.getClassSchema(), new String(data));
            return reader.read(null, decoder);
        } catch (IOException e) {
            LOGGER.error("Deserialization error:" + e.getMessage());
        }
        return null;
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}

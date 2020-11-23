package com.md.client.util;

import com.md.client.senders.OrderSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class Throughput implements Runnable {

    // 1000000 nano to 1 milisec
    // 1000 milisec to 1 sec
    private final int nanoSec = 1000000;
    private final int miliSec = 1000;
    private ConcurrentMap<Long, Long> idToTime = new ConcurrentHashMap<>();
    private ConcurrentMap<Long, Long> idToMsg = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    private long msgProcessed;
    private static final Logger LOGGER = LoggerFactory.getLogger(Throughput.class);

    public void addData(long initialTime, long id) {
        Long initTime = idToTime.get(id);
        if (initTime == null) {
            idToTime.putIfAbsent(id, initTime);
            idToMsg.putIfAbsent(id, 1l);
        }
        idToMsg.put(id, idToMsg.get(id) + 1);
    }


    @Override
    public void run() {
        // compute throughput
        // LOGGER.info("Total Message/sec {}", getTotalMessages()/getTime());
        while (isRunning()) {
            Set<Long> keys = idToTime.keySet();
            for (Long k : keys) {
                long startTime = idToTime.get(k);
                long msgs = idToMsg.get(k);
                long endTime = System.currentTimeMillis();
                long timeDiff = (endTime - startTime) / 1000;
                LOGGER.info("Total TimemiliSec {}", timeDiff);
                LOGGER.info("Message rate/sec {}", msgs / timeDiff);
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}

package com.overload.threadpool.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QueueMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueueMonitor.class);
    private final BlockingQueue<Message> blockingQueue;
    private final ScheduledExecutorService scheduledExecutorService;


    public QueueMonitor(BlockingQueue<Message> marbenQueue) {
        this.blockingQueue = marbenQueue;
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    public void start() {
        Runnable runnable = () -> LOGGER.info("Marben queue size : {}", blockingQueue.size());
        scheduledExecutorService.scheduleAtFixedRate(runnable, 0, 200, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        scheduledExecutorService.shutdown();
    }

}

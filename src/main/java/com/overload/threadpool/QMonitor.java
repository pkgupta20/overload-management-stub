package com.overload.threadpool;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class QMonitor {
    private static final Logger LOGGER = Logger.getLogger(QMonitor.class);
    private BlockingQueue<Message> blockingQueue;
    private ScheduledExecutorService scheduledExecutorService;


    public QMonitor(BlockingQueue<Message> marbenQueue) {
        this.blockingQueue= marbenQueue;
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    public void start(){
        Runnable runnable = ()->{
            LOGGER.info("Marben queue size : "+blockingQueue.size());
        };

        scheduledExecutorService.scheduleAtFixedRate(runnable,0,200, TimeUnit.MILLISECONDS);
    }

    public void stop(){
        scheduledExecutorService.shutdown();
    }

}

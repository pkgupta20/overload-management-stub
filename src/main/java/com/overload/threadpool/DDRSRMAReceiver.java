package com.overload.threadpool;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DDRSRMAReceiver extends Thread{
    BlockingQueue<Message> nodeQueue;
    BlockingQueue<Message> marbenResponseQueue;
    ExecutorService service;

    private static final Logger LOGGER = Logger.getLogger(DDRSRMAReceiver.class);

    private final int marbenResponseQueueConsumers;


    public DDRSRMAReceiver(BlockingQueue<Message> nodeQueue, BlockingQueue<Message> marbenResponseQueue, int marbenResponseQueueConsumers) {

        this.nodeQueue = nodeQueue;
        this.marbenResponseQueue = marbenResponseQueue;
        this.marbenResponseQueueConsumers = marbenResponseQueueConsumers;

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("DDRS_RMA_RECEIVER_Thread-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        this.service = Executors.newFixedThreadPool(5,factory);
    }

    @Override
    public void run() {
        int count = 0;
        while (true) {
            Message message = null;
            try {
                //message = nodeQueue.take();
                message = nodeQueue.poll(1, TimeUnit.SECONDS);
                //message = nodeRequestQueue.take();
                //wait for second and if message is null then exit from the loop
                if (message == null) {
                    LOGGER.info("wait for second and message is null so exiting from the loop");
                    break;
                }
                service.submit(new MarbenResponseSendingTask(message, marbenResponseQueue));
                count++;

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        shutdownService();
    }

    private void shutdownService() {
        LOGGER.info("MarbenResponse Queue size:"+marbenResponseQueue.size());
        service.shutdown();

    }
}

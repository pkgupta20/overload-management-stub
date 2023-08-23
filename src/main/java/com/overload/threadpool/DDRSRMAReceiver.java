package com.overload.threadpool;

import org.apache.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DDRSRMAReceiver extends Thread{
    BlockingQueue<Message> nodeQueue;
    BlockingQueue<Message> marbenResponseQueue;
    ExecutorService service;
    private static final String TERM_SIGNAL = "TERM";

    private static final Logger LOGGER = Logger.getLogger(DDRSRMAReceiver.class);

    private final int marbenResponseQueueConsumers;

    public DDRSRMAReceiver(BlockingQueue<Message> nodeQueue, BlockingQueue<Message> marbenResponseQueue, int producerThreads) {

        this.nodeQueue = nodeQueue;
        this.marbenResponseQueue = marbenResponseQueue;
        this.marbenResponseQueueConsumers = producerThreads;
        this.service = Executors.newFixedThreadPool(1);
    }

    @Override
    public void run() {
        int count = 0;
        while (true) {
            Message message = null;
            try {
                message = nodeQueue.take();
//                LOGGER.info(message);
                service.submit(new MarbenResponseSendingTask(message, marbenResponseQueue));
                count++;
                if (message.getType().equals(TERM_SIGNAL)) {
                    LOGGER.info(count + "Message received from response:");
                    break;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        shutdownService();
    }

    private void shutdownService() {
        LOGGER.info("MarbenResponse Queue size:"+marbenResponseQueue.size());
        service.shutdown();
        while(!service.isTerminated()){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        for(int i=0;i<marbenResponseQueueConsumers;i++) {
            Message message = new Message();
            message.setUuid(UUID.randomUUID());
            message.setType("TERM");
            marbenResponseQueue.offer(message);
        }
    }
}

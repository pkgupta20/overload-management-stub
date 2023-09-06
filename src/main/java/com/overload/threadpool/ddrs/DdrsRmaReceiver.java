package com.overload.threadpool.ddrs;


import com.overload.threadpool.util.Message;
import com.overload.threadpool.MarbenResponseSendingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DdrsRmaReceiver extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdrsRmaReceiver.class);
    private final BlockingQueue<Message> nodeQueue;
    private final BlockingQueue<Message> marbenResponseQueue;
    private final ExecutorService ddrsExecutorThreadPool;



    public DdrsRmaReceiver(BlockingQueue<Message> nodeQueue, BlockingQueue<Message> marbenResponseQueue, int ddrsExecutorThreadPoolSize) {
        this.nodeQueue = nodeQueue;
        this.marbenResponseQueue = marbenResponseQueue;
        this.ddrsExecutorThreadPool = Executors.newFixedThreadPool(ddrsExecutorThreadPoolSize);
    }

    @Override
    public void run() {
        int count = 0;
        while (true) {
            Message message;
            try {
                message = nodeQueue.poll(2, TimeUnit.SECONDS);
                if (message == null) {
                    LOGGER.info("Waited for Message for a second, but not received so exiting. Processed Messages:{}", count);
                    break;
                }
                ddrsExecutorThreadPool.submit(new MarbenResponseSendingTask(message, marbenResponseQueue));
                count++;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        shutdownService();
    }

    private void shutdownService() {
        LOGGER.info("MarbenResponse Queue size:{}", marbenResponseQueue.size());
        long startTime = System.currentTimeMillis();
        ddrsExecutorThreadPool.shutdown();
        LOGGER.info("Spent {} in termination now marbenResponseQueue size {}", (System.currentTimeMillis() - startTime) , marbenResponseQueue.size());


    }
}

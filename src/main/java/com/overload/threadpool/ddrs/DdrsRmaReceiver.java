package com.overload.threadpool.ddrs;


import com.overload.threadpool.util.Message;
import com.overload.threadpool.MarbenResponseSendingTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DdrsRmaReceiver extends Thread
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DdrsRmaReceiver.class);
    private final BlockingQueue<Message> nodeQueue;
    private final BlockingQueue<Message> marbenResponseQueue;
    private final ExecutorService ddrsExecutorThreadPool;
    private volatile boolean shouldExit;


    public DdrsRmaReceiver(BlockingQueue<Message> nodeQueue, BlockingQueue<Message> marbenResponseQueue, int ddrsExecutorThreadPoolSize) {
        this.nodeQueue = nodeQueue;
        this.marbenResponseQueue = marbenResponseQueue;
        this.ddrsExecutorThreadPool = Executors.newFixedThreadPool(ddrsExecutorThreadPoolSize);
        this.shouldExit = false;
    }

    @Override
    public void run() {
        while (!shouldExit) {
            Message message;
            try {
                message = nodeQueue.poll(20, TimeUnit.MILLISECONDS);
                if (message != null) {
                    ddrsExecutorThreadPool.submit(new MarbenResponseSendingTask(message, marbenResponseQueue));
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        shutdownExecutorService();
    }

    private void shutdownExecutorService() {
        long startTime = System.currentTimeMillis();
        ddrsExecutorThreadPool.shutdown();
        try {
            boolean result = ddrsExecutorThreadPool.awaitTermination(2, TimeUnit.SECONDS);
            LOGGER.info("Spent {} in termination now marbenResponseQueue size {}, terminated gracefully {}", (System.currentTimeMillis() - startTime), marbenResponseQueue.size(), result);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void stopDdrsRmaReceiver() {
        shouldExit = true;
    }
}

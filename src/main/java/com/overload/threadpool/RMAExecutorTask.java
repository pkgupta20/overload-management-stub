package com.overload.threadpool;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;

public class RMAExecutorTask implements Runnable {
    BlockingQueue<Message> responseQueue;
    Message message;
    private static final Logger LOGGER = Logger.getLogger(RMAExecutorTask.class);
    public RMAExecutorTask(Message message,BlockingQueue responseQueue) {
        this.message = message;
        this.responseQueue = responseQueue;
    }

    @Override
    public void run() {
        responseQueue.offer(message);

    }
}

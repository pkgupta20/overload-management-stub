package com.overload.threadpool.rma;


import com.overload.threadpool.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class RmaExecutorTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RmaExecutorTask.class);
    BlockingQueue<Message> responseQueue;
    Message message;

    public RmaExecutorTask(Message message,BlockingQueue<Message> responseQueue) {
        this.message = message;
        this.responseQueue = responseQueue;
    }

    @Override
    public void run() {
        boolean offerResult = responseQueue.offer(message);
        LOGGER.debug("{} pushed {}", message, offerResult);

    }
}

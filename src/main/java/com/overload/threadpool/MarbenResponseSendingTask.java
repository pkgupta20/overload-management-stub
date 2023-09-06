package com.overload.threadpool;

import com.overload.threadpool.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class MarbenResponseSendingTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MarbenResponseSendingTask.class);
    private final Message message;
    private final BlockingQueue<Message> marbenResponseQueue;


    public MarbenResponseSendingTask(Message message, BlockingQueue<Message> marbenResponseQueue) {
        this.message = message;
        this.marbenResponseQueue = marbenResponseQueue;
    }

    @Override
    public void run() {
        boolean offer = marbenResponseQueue.offer(message);
        LOGGER.debug("{} pushed {}", message, offer);
    }
}

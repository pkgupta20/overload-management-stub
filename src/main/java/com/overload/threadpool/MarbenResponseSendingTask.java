package com.overload.threadpool;

import java.util.concurrent.BlockingQueue;

public class MarbenResponseSendingTask implements Runnable {
    private Message message;
    private BlockingQueue<Message> marbenResponseQueue;

    public MarbenResponseSendingTask(Message message, BlockingQueue<Message> marbenResponseQueue) {
        this.message = message;
        this.marbenResponseQueue = marbenResponseQueue;
    }

    @Override
    public void run() {
        marbenResponseQueue.offer(message);

    }
}

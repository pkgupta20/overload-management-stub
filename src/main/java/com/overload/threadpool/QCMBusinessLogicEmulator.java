package com.overload.threadpool;


import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QCMBusinessLogicEmulator implements Runnable
{
    Message message;

    ExecutorService rmaExecutorThreadPool;

    BlockingQueue<Message> responseQueue;
    private static final String TERM_SIGNAL = "TERM";

    private static final Logger LOGGER = Logger.getLogger(QCMBusinessLogicEmulator.class);
    public QCMBusinessLogicEmulator(Message message, BlockingQueue<Message> responseQueue, ExecutorService rmaExecutorThreadPool) {
        this.message = message;
        this.rmaExecutorThreadPool  = rmaExecutorThreadPool;
        this.responseQueue = responseQueue;
    }

    @Override
    public void run() {
        this.rmaExecutorThreadPool.submit(new RMAExecutorTask(message,responseQueue));

    }


}

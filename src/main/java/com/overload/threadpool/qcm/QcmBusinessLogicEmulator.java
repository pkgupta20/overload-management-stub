package com.overload.threadpool.qcm;




import com.overload.threadpool.rma.RmaExecutorTask;
import com.overload.threadpool.util.Message;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class QcmBusinessLogicEmulator implements Runnable {
    private final Message message;

    private final ExecutorService rmaExecutorThreadPool;

    private final BlockingQueue<Message> responseQueue;
    private final int processingTimeMs;


    public QcmBusinessLogicEmulator(Message message, BlockingQueue<Message> responseQueue, ExecutorService rmaExecutorThreadPool, int processingTimeMs) {
        this.message = message;
        this.rmaExecutorThreadPool = rmaExecutorThreadPool;
        this.responseQueue = responseQueue;
        this.processingTimeMs = processingTimeMs;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(this.processingTimeMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.rmaExecutorThreadPool.submit(new RmaExecutorTask(message, responseQueue));

    }


}

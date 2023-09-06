package com.overload.threadpool.qcm;




import com.overload.threadpool.rma.RmaExecutorTask;
import com.overload.threadpool.util.Message;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class QcmBusinessLogicEmulator implements Runnable {
    private final Message message;

    private final ExecutorService rmaExecutorThreadPool;

    private final BlockingQueue<Message> responseQueue;
    private final int processingTime;


    public QcmBusinessLogicEmulator(Message message, BlockingQueue<Message> responseQueue, ExecutorService rmaExecutorThreadPool, int processingTime) {
        this.message = message;
        this.rmaExecutorThreadPool = rmaExecutorThreadPool;
        this.responseQueue = responseQueue;
        this.processingTime = processingTime;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(this.processingTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.rmaExecutorThreadPool.submit(new RmaExecutorTask(message, responseQueue));

    }


}

package com.overload.threadpool;

import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QCMRMAReceiver extends Thread{
    BlockingQueue<Message> nodeRequestQueue;
    BlockingQueue<Message> nodeResponseQueue;
    ExecutorService qcmInputAdapterThreadPool;
    ExecutorService qcmRmaExecutorThreadPool;
    private static final String TERM_SIGNAL = "TERM";
    private static final Logger LOGGER = Logger.getLogger(QCMRMAReceiver.class);

    public QCMRMAReceiver(BlockingQueue<Message> nodeRequestQueue, BlockingQueue<Message> nodeResponseQueue) {
        this.nodeRequestQueue = nodeRequestQueue;
        this.nodeResponseQueue = nodeResponseQueue;
        this.qcmInputAdapterThreadPool = Executors.newFixedThreadPool(1);
        this.qcmRmaExecutorThreadPool = Executors.newFixedThreadPool(1);
    }

    @Override
    public void run() {
        int count = 0;
        Message message;
        while (true) {
            try {
                message = nodeRequestQueue.take();
                qcmInputAdapterThreadPool.submit(new QCMBusinessLogicEmulator(message, nodeResponseQueue, qcmRmaExecutorThreadPool));
                count++;
                if (message.getType().equals(TERM_SIGNAL)) {
                    LOGGER.info(count + " Message received in QCM Receiver.");
                    break;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        shutdownThreadPool();
    }


    private void shutdownThreadPool() {
        qcmInputAdapterThreadPool.shutdown();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        qcmRmaExecutorThreadPool.shutdown();


        LOGGER.info("RMA Response Queue size:"+nodeResponseQueue.size());
    }
}

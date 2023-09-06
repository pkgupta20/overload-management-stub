package com.overload.threadpool.qcm;


import com.overload.threadpool.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class QcmRmaReceiver extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(QcmRmaReceiver.class);
    private final int processingTime;
    private final BlockingQueue<Message> nodeRequestQueue;
    private final BlockingQueue<Message> nodeResponseQueue;
    private final ExecutorService qcmInputAdapterThreadPool;
    private final ExecutorService qcmRmaExecutorThreadPool;


    public QcmRmaReceiver(BlockingQueue<Message> nodeRequestQueue, BlockingQueue<Message> nodeResponseQueue, int qcmInputAdapterThreadpoolSize, int qcmRmaExecutorThreadpoolSize, int processingTime) {
        this.nodeRequestQueue = nodeRequestQueue;
        this.nodeResponseQueue = nodeResponseQueue;
        this.qcmInputAdapterThreadPool = Executors.newFixedThreadPool(qcmInputAdapterThreadpoolSize);
        this.qcmRmaExecutorThreadPool = Executors.newFixedThreadPool(qcmRmaExecutorThreadpoolSize);
        this.processingTime = processingTime;
    }

    @Override
    public void run() {
        int count = 0;
        Message message;
        while (true) {
            try {
                message = nodeRequestQueue.poll(2, TimeUnit.SECONDS);
                if (message == null) {
                    LOGGER.info("{} Message received in QCM Receiver.", count);
                    break;
                }
                count++;
                qcmInputAdapterThreadPool.submit(new QcmBusinessLogicEmulator(message, nodeResponseQueue, qcmRmaExecutorThreadPool, processingTime));

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        shutdownThreadPool();
    }


    private void shutdownThreadPool() {
        qcmInputAdapterThreadPool.shutdown();
        qcmRmaExecutorThreadPool.shutdown();
        LOGGER.info("RMA Response Queue size:{}", nodeResponseQueue.size());
    }
}

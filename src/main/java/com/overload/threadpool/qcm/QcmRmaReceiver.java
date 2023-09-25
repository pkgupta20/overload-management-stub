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
    private final int processingTimeMs;
    private final BlockingQueue<Message> nodeRequestQueue;
    private final BlockingQueue<Message> nodeResponseQueue;
    private final ExecutorService qcmInputAdapterThreadPool;
    private final ExecutorService qcmRmaExecutorThreadPool;
    private volatile boolean shouldExit;


    public QcmRmaReceiver(BlockingQueue<Message> nodeRequestQueue, BlockingQueue<Message> nodeResponseQueue, int qcmInputAdapterThreadpoolSize, int qcmRmaExecutorThreadpoolSize, int processingTimeMs) {
        this.nodeRequestQueue = nodeRequestQueue;
        this.nodeResponseQueue = nodeResponseQueue;
        this.qcmInputAdapterThreadPool = Executors.newFixedThreadPool(qcmInputAdapterThreadpoolSize);
        this.qcmRmaExecutorThreadPool = Executors.newFixedThreadPool(qcmRmaExecutorThreadpoolSize);
        this.processingTimeMs = processingTimeMs;
        this.shouldExit = false;
    }

    @Override
    public void run() {
        Message message;
        while (!shouldExit) {
            try {
                message = nodeRequestQueue.poll(20, TimeUnit.MILLISECONDS);
                if (message != null) {
                    qcmInputAdapterThreadPool.submit(new QcmBusinessLogicEmulator(message, this.nodeResponseQueue, this.qcmRmaExecutorThreadPool, this.processingTimeMs));
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
            boolean resultInput = qcmInputAdapterThreadPool.awaitTermination(1, TimeUnit.SECONDS);
            qcmRmaExecutorThreadPool.shutdown();
            boolean resultRmaExecutor = qcmRmaExecutorThreadPool.awaitTermination(1, TimeUnit.SECONDS);
            LOGGER.debug("RMA Response Queue size:{}, InputAdapter terminated gracefully {}, RMAExecutor terminated gracefully {}", nodeResponseQueue.size(), resultInput, resultRmaExecutor);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public void stopQcmRmaReceiver() {
        shouldExit = true;
    }
}

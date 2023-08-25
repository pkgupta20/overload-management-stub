package com.overload.threadpool;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        BasicThreadFactory factory1 = new BasicThreadFactory.Builder()
                .namingPattern("Qcm_Input_Adapter_Thread-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        BasicThreadFactory factory2 = new BasicThreadFactory.Builder()
                .namingPattern("Qcm_Rma_Executor_Thread-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        this.qcmInputAdapterThreadPool = Executors.newFixedThreadPool(5,factory1);
        this.qcmRmaExecutorThreadPool = Executors.newFixedThreadPool(5,factory2);
    }

    @Override
    public void run() {
        int count = 0;
        Message message;
        while (true) {
            try {
                message = nodeRequestQueue.poll(1, TimeUnit.SECONDS);

                if(message == null)
                {
                    LOGGER.info(count + " Message received in QCM Receiver.");
                    break;
                }
                count++;
                qcmInputAdapterThreadPool.submit(new QCMBusinessLogicEmulator(message, nodeResponseQueue, qcmRmaExecutorThreadPool));


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

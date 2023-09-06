package com.overload.threadpool.rma;


import com.overload.threadpool.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class RmaInputThreadPool {
    private static final Logger LOGGER = LoggerFactory.getLogger(RmaInputThreadPool.class);
    private final ExecutorService service;
    private final List<BlockingQueue<Message>> qcmSiteNodeQueueList;


    public RmaInputThreadPool(ExecutorService service,List<BlockingQueue<Message>> qcmSiteNodeQueueList) {
        this.service = service;
        this.qcmSiteNodeQueueList = qcmSiteNodeQueueList;
    }

    public void execute(Message message){
        service.submit(()->
        {
            int queueAddress = message.getNodeId();
            boolean offerResult = qcmSiteNodeQueueList.get(queueAddress).offer(message);
            LOGGER.debug("{} pushed in qcm node {}:{}", message, message.getNodeId(), offerResult);

        });
    }

    public void stop() throws InterruptedException {
        service.shutdown();
        boolean terminationResult = service.awaitTermination(100, TimeUnit.MILLISECONDS);
        LOGGER.debug("RMAInputThreadPool shutdown normally {}",terminationResult);

    }
}

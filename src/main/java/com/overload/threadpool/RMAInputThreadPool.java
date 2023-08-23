package com.overload.threadpool;

import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class RMAInputThreadPool {
    private ExecutorService service;
    private List<BlockingQueue<Message>> qcmSiteNodeQueueList;
    private static final Logger LOGGER = Logger.getLogger(RMAInputThreadPool.class);

    public RMAInputThreadPool(ExecutorService service,List<BlockingQueue<Message>> qcmSiteNodeQueueList) {
        this.service = service;
        this.qcmSiteNodeQueueList = qcmSiteNodeQueueList;
    }

    public void execute(Message message){
        service.submit(()->
        {
            int queueAddress = message.getNodeId();
            qcmSiteNodeQueueList.get(queueAddress).offer(message);

        });
    }

    public void stop() throws InterruptedException {
        service.shutdown();
        service.awaitTermination(100, TimeUnit.MILLISECONDS);
        qcmSiteNodeQueueList.forEach(qcmSiteNodeQueue -> {
            int i = 0;
            LOGGER.info("Queue "+i+" size:"+qcmSiteNodeQueue.size());
            i++;
        });
    }
}

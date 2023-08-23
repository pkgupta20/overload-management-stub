package com.overload.threadpool;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class QCMRMAReceiverThreadPool {
    private ExecutorService service;
    private List<BlockingQueue<Message>> qcmSiteNodeQueueResponseList;

    public QCMRMAReceiverThreadPool(ExecutorService service) {
        this.service = service;
    }

    public void execute(Message message){
        service.submit(()->
        {
            int queueAddress = message.getNodeId();
            qcmSiteNodeQueueResponseList.get(queueAddress).offer(message);
        });
    }

    public void stop() throws InterruptedException {
        service.shutdown();
        service.awaitTermination(100, TimeUnit.MILLISECONDS);
    }
}

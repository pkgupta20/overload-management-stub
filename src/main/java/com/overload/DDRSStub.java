package com.overload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.*;

public class DDRSStub {
    private BlockingQueue<Message> marbenQueue;
    private final ExecutorService consumerExecutor;

    private int numberOfThreads;
    private BlockingQueue<Message> rmaInputQueue;
    private int qcmSites;
    private int qcmNodePerSite;
    private static final Logger LOGGER = Logger.getLogger(DDRSStub.class);
    public DDRSStub(BlockingQueue<Message> marbenQueue, BlockingQueue<Message> rmaInputQueue, int qcmSites, int numberOfThreads,int qcmNodePerSite){
        this.marbenQueue = marbenQueue;
        this.rmaInputQueue = rmaInputQueue;
        this.qcmSites = qcmSites;
        this.numberOfThreads = numberOfThreads;
        this.qcmNodePerSite = qcmNodePerSite;
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("DDRS-Stub-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        consumerExecutor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 20,TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(),factory);
    }

    public void start() throws InterruptedException {
        Random randomSite = new Random();
        Random randomNode = new Random();
        Runnable consumerTask = () -> {
            Message message = null;
            try {
                int count = 0;
                while(true) {
                    message = marbenQueue.take();
                    if (message != null) {

                        int siteId = randomSite.nextInt(qcmSites);
                        int nodeId = randomNode.nextInt(qcmNodePerSite);
                        message = processMessage(message,siteId,nodeId);
                        count++;
                        if(message.getType()=="TERM") {
                            LOGGER.info(" Message received in thread ::  " + message.getType() + "thread name: " + Thread.currentThread().getName());
                            break;
                        }
                    }
                }
                LOGGER.info(count+" Message processed in "+Thread.currentThread().getName());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        for (int i = 0; i < numberOfThreads; i++) {
            consumerExecutor.execute(consumerTask);
        }
    }


    private Message processMessage(Message message,int siteId,int nodeId) throws InterruptedException {
        message.setDestinationId(siteId);
        message.setNodeId(nodeId);
        Thread.sleep(10);
        return message;
    }

    public void stop() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("DDRSStub shutdown triggered:");
        consumerExecutor.shutdown();
        long endTime = System.currentTimeMillis();
        LOGGER.info("DDRSStub shutdown passed:"+(endTime - startTime));
        startTime = System.currentTimeMillis();
        consumerExecutor.awaitTermination(10000,TimeUnit.SECONDS);
        LOGGER.info("DDRSStub Waited for Termination:"+( System.currentTimeMillis() - startTime));

    }


}
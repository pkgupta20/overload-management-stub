package com.overload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.*;

public class RMAQueueProcessor {
    private BlockingQueue<Message> rmaInputQueue;
    private BlockingQueue<Message> rmaOutputQueue;

    private BlockingQueue<Message> responseQueue;

    private final ExecutorService consumerExecutor;

    private ExecutorService responseExecutor;

    private int numberOfThreads;
    private static final Logger LOGGER = Logger.getLogger(RMAQueueProcessor.class);

    private List<BlockingQueue<Message>> qcmSiteList;

    private int numberOfMessages;


    public RMAQueueProcessor(BlockingQueue<Message> rmaInputQueue, BlockingQueue<Message> rmaOutputQueue, BlockingQueue<Message> responseQueue, List<BlockingQueue<Message>> qcmSites, int numberOfMessage, int numberOfThreads) {
        this.rmaInputQueue = rmaInputQueue;
        this.rmaOutputQueue = rmaOutputQueue;
        this.responseQueue = responseQueue;
        this.qcmSiteList = qcmSites;
        this.numberOfMessages = numberOfMessage;
        this.numberOfThreads = numberOfThreads;
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("RMA-Queue-Processor-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        consumerExecutor = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 20, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), factory);
    }


    public void sendToQCMSites() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        Runnable consumerTask = () -> {
            try {
                while (true) {
                    Message message = rmaInputQueue.take();
                    if (message.getType() == "TERM") {
                        LOGGER.info("Message type is TERM TYPE so breaking the loop in RMAQueueProcessor in " + Thread.currentThread().getName());
                        break;
                    }

                    sendMessageToDestination(message);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info("Message pushed to QCMSites in " + (endTime - startTime) + " milis");
        };

        for (int i = 0; i < numberOfThreads; i++) {
            consumerExecutor.execute(consumerTask);
        }
    }

    private void sendMessageToDestination(Message message) {
        int siteId = message.getDestinationId();
        qcmSiteList.get(siteId).offer(message);
    }


    public void sendResponse() {
        Runnable responseTask = () -> {
            try {
                while (true) {
                    Message message = rmaOutputQueue.take();
                    //LOGGER.info("Message:: "+message);
                    if (message.getType() == "TERM") {
                        LOGGER.info("Message type is TERM TYPE so breaking the loop in sendResponse in " + Thread.currentThread().getName());
                        break;
                    }
                    responseQueue.offer(message);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("RMA-Response-Handler-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        responseExecutor = Executors.newFixedThreadPool(numberOfThreads,factory);
        for(int i=0;i<numberOfThreads;i++){
            responseExecutor.submit(responseTask);
        }
    }

            public void stop () throws InterruptedException {

                for (int i = 0; i < qcmSiteList.size(); i++) {
                    Message message = new Message("TERM");
                    message.setDestinationId(i);
                    qcmSiteList.get(i).offer(message);
                }

                long startTime = System.currentTimeMillis();
                LOGGER.info("Workload shutdown triggered:");
                consumerExecutor.shutdown();

                long endTime = System.currentTimeMillis();
                LOGGER.info("Workload shutdown passed:" + (endTime - startTime));
                startTime = System.currentTimeMillis();
                consumerExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
                LOGGER.info("Waited for Termination:" + (System.currentTimeMillis() - startTime));
                responseExecutor.shutdown();
                consumerExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);
            }
        }
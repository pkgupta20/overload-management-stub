package com.overload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class RMAQueueProcessor {
    private BlockingQueue<Message> rmaInputQueue;
    private BlockingQueue<Message> rmaOutputQueue;

    private BlockingQueue<Message> responseQueue;

    private final ExecutorService consumerExecutor;

    private ExecutorService responseExecutor;

    private int rmaInputQueueConsumers;
    private int rmaOutputQueueConsumers;
    private int responseQueueConsumers;
    private static final Logger LOGGER = Logger.getLogger(RMAQueueProcessor.class);

    private List<BlockingQueue<Message>> qcmSiteList;


    public RMAQueueProcessor(BlockingQueue<Message> rmaInputQueue, BlockingQueue<Message> rmaOutputQueue, BlockingQueue<Message> responseQueue, List<BlockingQueue<Message>> qcmSites, int rmaInputQueueConsumers, int rmaOutputQueueConsumers, int responseQueueConsumers) {
        this.rmaInputQueue = rmaInputQueue;
        this.rmaOutputQueue = rmaOutputQueue;
        this.responseQueue = responseQueue;
        this.qcmSiteList = qcmSites;
        this.rmaInputQueueConsumers = rmaInputQueueConsumers;
        this.rmaOutputQueueConsumers = rmaOutputQueueConsumers;
        this.responseQueueConsumers = responseQueueConsumers;
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("RMA-Queue-Processor-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        consumerExecutor = new ThreadPoolExecutor(this.rmaInputQueueConsumers, this.rmaInputQueueConsumers, 20, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), factory);
    }

    public void sendToQCMSites() throws InterruptedException {
        Runnable consumerTask = () -> {
            long startTime = System.currentTimeMillis();
            int count =  0;
            try {
                while (true) {
                    Message message = rmaInputQueue.take();
                    if (message.getType() == "TERM") {
                        LOGGER.info("Message type is TERM TYPE so breaking the loop in RMAQueueProcessor in " + Thread.currentThread().getName());
                        break;
                    }

                    sendMessageToDestination(message);
                    count++;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            long endTime = System.currentTimeMillis();
            LOGGER.info(count+" Message pushed to QCMSites in " + (endTime - startTime) + " milis");
        };

        List<Runnable> runnableList = new ArrayList<>();
        for (int i = 0; i < rmaInputQueueConsumers; i++) {
            runnableList.add(consumerTask);
        }

        for (Runnable runnable:runnableList) {
            consumerExecutor.execute(runnable);
        }
    }

    private void sendMessageToDestination(Message message) {
        int siteId = message.getDestinationId();
        qcmSiteList.get(siteId).offer(message);
    }


    public void sendResponse() {
        Runnable responseTask = () -> {
            int count = 0;
            try {
                while (true) {
                    Message message = rmaOutputQueue.take();
                    //LOGGER.info("Message:: "+message);
                   if (message.getType() == "TERM") {
                        LOGGER.info("Message type is TERM TYPE so breaking the loop in sendResponse in " + Thread.currentThread().getName());
                        break;
                    }
                    responseQueue.offer(message);
                   count++;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            LOGGER.info(count +" Message read from rmaOutputQueue by "+Thread.currentThread().getName());
        };

        List<Runnable> runnableList = new ArrayList<>();
        for (int i = 0; i < rmaOutputQueueConsumers; i++) {
            runnableList.add(responseTask);
        }

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("RMA-Response-Handler-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        responseExecutor = Executors.newFixedThreadPool(rmaOutputQueueConsumers,factory);

        for(Runnable runnable:runnableList){
            responseExecutor.submit(runnable);
        }
    }

            public void stop () throws InterruptedException {
                long startTime = System.currentTimeMillis();
                LOGGER.info("Workload shutdown triggered:");
                consumerExecutor.shutdown();
                while(!consumerExecutor.isTerminated()){
                    Thread.sleep(100);
                }
                for (int i = 0; i < qcmSiteList.size(); i++) {
                    Message message = new Message("TERM");
                    message.setDestinationId(i);
                    qcmSiteList.get(i).offer(message);
                }
                long endTime = System.currentTimeMillis();
                LOGGER.info("Workload shutdown passed:" + (endTime - startTime));
                startTime = System.currentTimeMillis();
                consumerExecutor.awaitTermination(1000, TimeUnit.SECONDS);
                LOGGER.info("Waited for Termination:" + (System.currentTimeMillis() - startTime));

            }

            public void shutdownResponseProcess() throws InterruptedException {
                responseExecutor.shutdown();
                while (!responseExecutor.isTerminated()) {
                    Thread.sleep(100);
                }
                for (int i = 0; i < this.responseQueueConsumers; i++) {
                    Message message = new Message("TERM");
                    responseQueue.offer(message);
                }

                responseExecutor.awaitTermination(1000, TimeUnit.SECONDS);
            }
        }
package com.overload.threadpool;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class NetworkEmulator {
    private final BlockingQueue<Message> marbenMessageQueue;
    private final BlockingQueue<Message> marbenResponseQueue;
    private final ExecutorService publisherExecutor;

    private final ExecutorService receiverExecutor;

    private Map<UUID,Message> messageMap;

    private final int numberOfMessages;
    private final int numberOfThreads;

    private final int numberOfConsumerThreads;

    private static final Logger LOGGER = Logger.getLogger(NetworkEmulator.class);

    public NetworkEmulator(int numberOfMessages, int numberOfThreads, BlockingQueue<Message> mQueue, int numberOfConsumerThreads, BlockingQueue<Message> marbenResponseQueue) {
        this.numberOfMessages = numberOfMessages;
        this.numberOfThreads = numberOfThreads;
        this.marbenMessageQueue = mQueue;
        this.numberOfConsumerThreads =  numberOfConsumerThreads;
        this.messageMap = new ConcurrentHashMap<>();
        this.marbenResponseQueue = marbenResponseQueue;
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("Workload-generator-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        publisherExecutor = Executors.newFixedThreadPool(numberOfThreads,factory);
        receiverExecutor = Executors.newFixedThreadPool(numberOfThreads,factory);
    }

    public void start() {
        publishMessage();
        receiveMessage();

    }

    private void publishMessage() {
        int numberOfMessagePerThread = numberOfMessages / numberOfThreads;
        Runnable publisherTask = () -> {
            for (int i = 0; i < numberOfMessagePerThread; i++) {
                Message message = getMessage();
                marbenMessageQueue.offer(message);
            }
            LOGGER.info(numberOfMessagePerThread +" Message pushed in "+Thread.currentThread().getName());
        };

        for (int i = 0; i < numberOfThreads; i++) {
             publisherExecutor.execute(publisherTask);
         }
    }


    private void receiveMessage() {
       Runnable receiverTask = () -> {
           int count = 0;
           while(true)
           try {

               Message receivedMessage = marbenResponseQueue.poll(1,TimeUnit.SECONDS);
               count++;
               if(receivedMessage == null) {
                   LOGGER.info("Total Read message by this thread:"+count+" now exiting."+Thread.currentThread().getName());
                   break;
               }
               Message message = messageMap.remove(receivedMessage.getUuid());
               if(message != null)
                   message.setTotalTimeInProcessing(System.currentTimeMillis() - message.getInitTime());
               LOGGER.info(message);

           } catch (InterruptedException e) {
               throw new RuntimeException(e);
           }
        };

        for (int i = 0; i < numberOfThreads; i++) {
            receiverExecutor.execute(receiverTask);
        }
    }

    private Message getMessage() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        UUID uuid = UUID.randomUUID();
        String type = getType();
        long initTime = System.currentTimeMillis();
        Message message = new Message(uuid,type,initTime);
        messageMap.put(uuid,message);
        return message;
    }

    private String getType() {
        int randomNumber = ThreadLocalRandom.current().nextInt(4);
        switch (randomNumber) {
            case 0:
                return "CCRI";
            case 1:
                return "CCRU";
            case 2:
                return "CCRT";
            case 3:
                return "CCRE";
        }
        return null;
    }

    public void stop() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Workload shutdown triggered:");
        publisherExecutor.shutdown();

        long endTime = System.currentTimeMillis();
        LOGGER.info("Workload shutdown passed:"+(endTime - startTime));
        startTime = System.currentTimeMillis();
        publisherExecutor.awaitTermination(1000,TimeUnit.SECONDS);
        LOGGER.info("Waited for Termination:"+( System.currentTimeMillis() - startTime));
        LOGGER.info("Map Size:"+messageMap.size());

        receiverExecutor.shutdown();

    }

}
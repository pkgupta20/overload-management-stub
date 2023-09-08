package com.overload.threadpool;


import com.overload.threadpool.util.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.overload.threadpool.util.Message;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class NetworkEmulator {
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulator.class);
    private static final long NANO_TO_MILIS = 1000000;
    private final BlockingQueue<Message> marbenMessageQueue;
    private final BlockingQueue<Message> marbenResponseQueue;
    private final ScheduledExecutorService publisherExecutor;

    private final ExecutorService receiverExecutor;

    private final Map<UUID, Message> messageMap;

    private final int numberOfMessages;
    private final int numberOfThreads;
    private final int numberOfIteration;





    public NetworkEmulator(int numberOfMessages, int numberOfIteration, int numberOfThreads, BlockingQueue<Message> mQueue, BlockingQueue<Message> marbenResponseQueue) {
        this.numberOfMessages = numberOfMessages;
        this.numberOfThreads = numberOfThreads;
        this.marbenMessageQueue = mQueue;
        this.messageMap = new ConcurrentHashMap<>();
        this.marbenResponseQueue = marbenResponseQueue;
        this.numberOfIteration = numberOfIteration;

        publisherExecutor = Executors.newScheduledThreadPool(1);
        receiverExecutor = Executors.newFixedThreadPool(this.numberOfThreads);
    }

    public void start() {
        publishMessage();
        receiveMessage();
    }

    private void publishMessage() {
        AtomicInteger iterationCount = new AtomicInteger(0);
        RateLimiter rateLimiter = new RateLimiter(numberOfMessages);
        Runnable publisherTask = () -> {
            rateLimiter.start();
            for (int i = 0; i < numberOfMessages; i++) {
                rateLimiter.delayIfNeeded();
                Message message = getMessage();
                boolean offerResult = marbenMessageQueue.offer(message);
                LOGGER.debug("{} pushed in marbenQueue {} ",message, offerResult);
            }
            rateLimiter.reset();
            LOGGER.info("{} Message pushed in {} ", numberOfMessages, Thread.currentThread().getName());
            iterationCount.incrementAndGet();
            if (iterationCount.get() == numberOfIteration)
                publisherExecutor.shutdown();
        };
        publisherExecutor.scheduleAtFixedRate(publisherTask, 0, 1, TimeUnit.SECONDS);


    }

    private void receiveMessage() {
        Runnable receiverTask = () -> {
            int count = 0;
            while (true)
                try {

                    Message receivedMessage = marbenResponseQueue.poll(2, TimeUnit.SECONDS);
                    count++;
                    if (receivedMessage == null) {
                        LOGGER.info("Total Read message by this thread: {} now exiting.{}", count, Thread.currentThread().getName());
                        break;
                    }
                    Message message = messageMap.remove(receivedMessage.getUuid());
                    if (message != null) {
                        long elapsedTimeInMilis = (System.nanoTime() - message.getInitTime())/NANO_TO_MILIS;
                        message.setTotalTimeInProcessing(elapsedTimeInMilis);
                        LOGGER.info("{}",message);

                    }


                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
        };

        for (int i = 0; i < numberOfThreads; i++) {
            receiverExecutor.execute(receiverTask);
        }
    }

    private Message getMessage() {
        UUID uuid = UUID.randomUUID();
        String type = getType();

        long initTime = System.nanoTime();
        Message message = new Message(uuid, type, initTime);
        messageMap.put(uuid, message);
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

    public void stop() {
        LOGGER.info("Map Size:{}", messageMap.size());
        receiverExecutor.shutdown();
    }

}
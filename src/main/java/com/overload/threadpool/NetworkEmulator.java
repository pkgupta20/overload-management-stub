package com.overload.threadpool;


import com.overload.threadpool.util.MetricSampler;
import com.overload.threadpool.util.RateLimiter;
import io.micrometer.core.instrument.Timer;
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

    private static AtomicInteger messageCount;
    private final BlockingQueue<Message> marbenMessageQueue;
    private final BlockingQueue<Message> marbenResponseQueue;
    private final ScheduledExecutorService publisherExecutor;

    private final ExecutorService receiverExecutor;

    private final Map<UUID, Message> messageMap;

    private final int numberOfMessages;
    private final int numberOfThreads;
    private final int numberOfIteration;
    private final Timer timer;
    private final int expectedMessages;

    private MetricSampler metricSampler;

    private final int timeoutMs;

    private volatile boolean shouldExit;



    public NetworkEmulator(int numberOfMessages, int numberOfIteration, int numberOfThreads, BlockingQueue<Message> mQueue, BlockingQueue<Message> marbenResponseQueue, Timer timer, int timeOutMs) {
        this.numberOfMessages = numberOfMessages;
        this.numberOfThreads = numberOfThreads;
        this.marbenMessageQueue = mQueue;
        this.messageMap = new ConcurrentHashMap<>();
        this.marbenResponseQueue = marbenResponseQueue;
        this.numberOfIteration = numberOfIteration;
        this.timer = timer;
        this.expectedMessages = (this.numberOfMessages * this.numberOfIteration);
        this.shouldExit = false;
        this.timeoutMs = timeOutMs;
        messageCount = new AtomicInteger(0);



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
            LOGGER.debug("{} Message pushed in {} ", numberOfMessages, Thread.currentThread().getName());
            iterationCount.incrementAndGet();
            if (iterationCount.get() == numberOfIteration) {
                publisherExecutor.shutdown();

            }
        };
        publisherExecutor.scheduleAtFixedRate(publisherTask, 0, 1, TimeUnit.SECONDS);


    }

    private void receiveMessage() {

        Runnable receiverTask = () -> {
            while (!shouldExit)
                try {
                    Message receivedMessage = marbenResponseQueue.poll(10,TimeUnit.MILLISECONDS);
                    if(receivedMessage != null ){
                        Message message = messageMap.remove(receivedMessage.getUuid());

                        if (message != null) {

                            long elapsedTimeInNanos = (System.nanoTime() - message.getInitTime());
                            long elapsedTimeInMillis = elapsedTimeInNanos/NANO_TO_MILIS;
                            if(elapsedTimeInMillis < timeoutMs){
                                messageCount.incrementAndGet();
                                timer.record(elapsedTimeInNanos, TimeUnit.NANOSECONDS);
                            } else{
                                metricSampler.incTimedOutMessageCount();
                            }
                            message.setTotalTimeInProcessing(elapsedTimeInMillis);
                        }
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
        Message messageInMap = new Message(uuid, type, initTime);
        Message messageToProcess = new Message(uuid, type, initTime);
        messageMap.put(uuid, messageInMap);
        return messageToProcess;
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

    public boolean stop() throws InterruptedException {
        while(true){
            if (expectedMessages <= (messageCount.get() + metricSampler.getTimedOutMessageCount())) {
                LOGGER.info("Total Read message: {},timedOut :{} till now exiting.{}", messageCount.get(), metricSampler.getTimedOutMessageCount(), Thread.currentThread().getName());
                shouldExit = true;
                break;
            }else{
                Thread.sleep(500);
            }
        }
        receiverExecutor.shutdown();
        boolean result  = receiverExecutor.awaitTermination(1, TimeUnit.SECONDS);
        LOGGER.debug("Map Size:{} after termination gracefully {}", messageMap.size(), result);
        return true;
    }

    public Map<UUID, Message> getMessageMap() {
        return messageMap;
    }

    public void setMetricSampler(MetricSampler metricSampler) {
        this.metricSampler = metricSampler;
    }

}
package com.overload.threadpool;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class MessagePublisher implements Runnable{

    private int noOfMessagePerSecond;
    private int noOfIteration;
    
    private AtomicInteger count;
    private BlockingQueue<Message> marbenMessageQueue;

    private Map<UUID,Message> messageMap;

    private ScheduledExecutorService scheduledExecutorService;

    public MessagePublisher(int noOfMessagePerSecond, int noOfIteration, BlockingQueue<Message> marbenMessageQueue, Map<UUID, Message> messageMap, ScheduledExecutorService scheduledExecutorService)

    {
        this.noOfMessagePerSecond = noOfMessagePerSecond;
        this.noOfIteration = noOfIteration;
        this.marbenMessageQueue = marbenMessageQueue;
        this.count = new AtomicInteger(0);
        this.messageMap = messageMap;
        this.scheduledExecutorService = scheduledExecutorService;

    }

    public void run(){
        count.incrementAndGet();
        for (int i = 0; i < noOfMessagePerSecond; i++) {
            Message message = getMessage();
            marbenMessageQueue.offer(message);
        }
        if(count.get()==noOfIteration)
            scheduledExecutorService.shutdown();
    }


    private Message getMessage() {

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
}

package com.overload.threadpool.ddrs;


import com.overload.threadpool.util.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class DdrsNode implements Runnable
{

    private final BlockingQueue<Message> marbenQueue;
    private final DdrsRmaSender ddrsRmaEmulator;
    private final DdrsRmaReceiver ddrsRmaReceiver;

    private final int qcmNodes;
    private final int processingTimeMs;
    private volatile boolean shouldExit;


    public DdrsNode(BlockingQueue<Message> marbenQueue, int qcmNodes, DdrsRmaSender ddrsRmaEmulator, DdrsRmaReceiver ddrsRmaReceiver, int processingTimeMs) {
        this.marbenQueue = marbenQueue;
        this.qcmNodes = qcmNodes;
        this.ddrsRmaEmulator = ddrsRmaEmulator;
        this.ddrsRmaReceiver = ddrsRmaReceiver;
        this.processingTimeMs = processingTimeMs;
        this.shouldExit = false;
    }

    @Override
    public void run() {
        ddrsRmaReceiver.start();
        Message message;
        try {
            while (!shouldExit) {

                message = marbenQueue.poll(20, TimeUnit.MILLISECONDS);
                if (message != null) {
                    message.setNodeId(ThreadLocalRandom.current().nextInt(this.qcmNodes));
                    processMessage(message);

                }
            }

            ddrsRmaEmulator.shutdownRmaInputThreadPool();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public void stopDdrsNode() {
        shouldExit = true;
    }

    private void processMessage(Message message) throws InterruptedException {
        long startTime = System.nanoTime();
        Thread.sleep(processingTimeMs);
        long elapsedTime = System.nanoTime() - startTime;
        message.setTimeSpentInDDRS(elapsedTime);
        this.ddrsRmaEmulator.submit(message);
    }
}
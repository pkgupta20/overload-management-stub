package com.overload.threadpool.ddrs;


import com.overload.threadpool.util.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DdrsNode implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DdrsNode.class);
    private final BlockingQueue<Message> marbenQueue;
    private final DdrsRmaSender ddrsRmaEmulator;
    private final DdrsRmaReceiver ddrsRmaReceiver;

    private final int qcmNodes;
    private final int processingTime;


    public DdrsNode(BlockingQueue<Message> marbenQueue, int qcmNodes, DdrsRmaSender ddrsRmaEmulator, DdrsRmaReceiver ddrsRmaReceiver, int processingTime) {
        this.marbenQueue = marbenQueue;
        this.qcmNodes = qcmNodes;
        this.ddrsRmaEmulator = ddrsRmaEmulator;
        this.ddrsRmaReceiver = ddrsRmaReceiver;
        this.processingTime = processingTime;
    }

    @Override
    public void run() {
        ddrsRmaReceiver.start();
        Random randomNode = new Random();
        Message message;
        int count = 0;
        while (true) {
            try {
                message = marbenQueue.poll(2, TimeUnit.SECONDS);
                if (message == null) {

                    LOGGER.info("{} message processed, and now quitting because message not received within 1 second.",count);
                    break;
                }
                message.setNodeId(randomNode.nextInt(this.qcmNodes));
                processMessage(message);
                count++;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        try {
            ddrsRmaEmulator.shutdownRmaInputThreadPool();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private void processMessage(Message message) throws InterruptedException {
        Thread.sleep(processingTime);
        this.ddrsRmaEmulator.submit(message);
    }
}

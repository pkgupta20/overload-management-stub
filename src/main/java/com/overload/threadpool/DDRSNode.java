package com.overload.threadpool;

import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class DDRSNode implements Runnable{
    BlockingQueue<Message> marbenQueue;
    DDRSRMAEmulator ddrsRmaEmulator;
    DDRSRMAReceiver ddrsRmaReceiver;


    int qcmNodes;
    private static final Logger LOGGER = Logger.getLogger(DDRSNode.class);

    public DDRSNode(BlockingQueue<Message> marbenQueue, int qcmNodes, DDRSRMAEmulator ddrsRmaEmulator, DDRSRMAReceiver ddrsRmaReceiver) {
        this.marbenQueue = marbenQueue;
        this.qcmNodes = qcmNodes;
        this.ddrsRmaEmulator = ddrsRmaEmulator;
        this.ddrsRmaReceiver = ddrsRmaReceiver;
    }

    @Override
    public void run() {
        ddrsRmaReceiver.start();
        Random randomNode = new Random();
        Message message;
        int count = 0;
        while(true){
            try {
                message = marbenQueue.poll(2, TimeUnit.SECONDS);
                if(message == null){

                    LOGGER.info(count+" Message received in thread ::  " + Thread.currentThread().getName());
                    break;
                }
                message.setNodeId(randomNode.nextInt(this.qcmNodes));
                message = processMessage(message);
                count++;
                if (message.getType() == "TERM") {
                    LOGGER.info(count+" Message received in thread ::  " + message.getType() + "thread name: " + Thread.currentThread().getName());
                    break;
                }
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

    private Message processMessage(Message message) throws InterruptedException {
        //Thread.sleep(10);
        this.ddrsRmaEmulator.submit(message);
        return message;
    }
}

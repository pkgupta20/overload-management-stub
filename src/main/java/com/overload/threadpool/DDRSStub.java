package com.overload.threadpool;


import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DDRSStub {
    private final List<DDRSRMAEmulator> ddrsRmaEmulators;
    private final List<DDRSRMAReceiver> ddrsRMAReceivers;
    private BlockingQueue<Message> marbenQueue;
    private final ExecutorService consumerExecutor;

    private int ddrsConsumers;
    private int qcmNodes;

    private static final Logger LOGGER = Logger.getLogger(DDRSStub.class);
    public DDRSStub(BlockingQueue<Message> marbenQueue, int ddrsConsumers, int qcmNodes, List<DDRSRMAEmulator> ddrsRmaEmulators, List<DDRSRMAReceiver> ddrsrmaReceivers){
        this.marbenQueue = marbenQueue;
        this.ddrsConsumers = ddrsConsumers;
        this.qcmNodes = qcmNodes;
        this.ddrsRmaEmulators = ddrsRmaEmulators;
        this.ddrsRMAReceivers = ddrsrmaReceivers;
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("DDRS-Stub-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        consumerExecutor = Executors.newFixedThreadPool(this.ddrsConsumers, factory);
    }

    public void start() throws InterruptedException {
        List<Runnable> ddrsNodes = new ArrayList<>(ddrsConsumers);
        for (int i = 0; i < ddrsConsumers; i++) {
            Runnable ddrsNode = new DDRSNode(this.marbenQueue, this.qcmNodes,this.ddrsRmaEmulators.get(i), this.ddrsRMAReceivers.get(i));
            ddrsNodes.add(ddrsNode);
        }

        for (Runnable ddrsNode: ddrsNodes) {
            consumerExecutor.execute(ddrsNode);
        }


    }

    public void stop() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("DDRSStub shutdown triggered:");
        consumerExecutor.shutdown();
        long endTime = System.currentTimeMillis();
        LOGGER.info("DDRSStub shutdown passed:" + (endTime - startTime));
        startTime = System.currentTimeMillis();
        consumerExecutor.awaitTermination(10000, TimeUnit.SECONDS);
        LOGGER.info("DDRSStub Waited for Termination:" + (System.currentTimeMillis() - startTime));

    }


}
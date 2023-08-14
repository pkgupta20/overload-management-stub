package com.overload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class QCMProcessor {
    private BlockingQueue<Message> rmaOutputQueue;



    private Map<Integer, List<BlockingQueue<Message>>> qcmNodeMap;

    private ExecutorService executorService;

    private int qcmNodePerSite;

    private int numberOfThreads;


    private static final Logger LOGGER = Logger.getLogger(QCMProcessor.class);

    public QCMProcessor(BlockingQueue<Message> rmaOutputQueue, int qcmNodePerSite, Map<Integer, List<BlockingQueue<Message>>> qcmNodeMap, int numberOfThreads){

        this.rmaOutputQueue = rmaOutputQueue;
        this.qcmNodePerSite=qcmNodePerSite;
        this.qcmNodeMap = qcmNodeMap;
        this.numberOfThreads=numberOfThreads;
    }



    public void spawnQCMThread(){
        List<Runnable> runnables= new LinkedList<>();
        for(Map.Entry<Integer,List<BlockingQueue<Message>>> entry : qcmNodeMap.entrySet()){

            List<BlockingQueue<Message>> qcmNodeList = entry.getValue();

            for (BlockingQueue<Message> qcmNode: qcmNodeList) {
                Runnable runnable = () -> {
                    while (true) {
                        try {
                            Message message = qcmNode.take();
                            if(message.getType()=="TERM"){
                                LOGGER.info("Message type is TERM TYPE so breaking the loop from QCMProcessor:: "+message);
                                break;
                            }
                            processMessage(message);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    LOGGER.info("Message Pushed to QCM Processor :: "+Thread.currentThread().getName());
                };
                runnables.add(runnable);
            }

        }


        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("QCMProcessor-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        executorService = Executors.newFixedThreadPool(runnables.size(),factory);
        for(Runnable qcmProcess:runnables){
            executorService.submit(qcmProcess);
        }
    }



    public void stop() throws InterruptedException {
        for (int i = 0; i < numberOfThreads; i++) {
            Message message = new Message("TERM");
            rmaOutputQueue.offer(message);
        }

        long startTime = System.currentTimeMillis();
        LOGGER.info("QCMProcessor shutdown triggered:");
        executorService.shutdown();
        long endTime = System.currentTimeMillis();
        LOGGER.info("QCMProcessor shutdown passed:"+(endTime - startTime));
        startTime = System.currentTimeMillis();
        executorService.awaitTermination(10000,TimeUnit.SECONDS);
        LOGGER.info("Waited for Termination:"+( System.currentTimeMillis() - startTime));
    }
    public void processMessage(Message message) throws InterruptedException {

        Thread.sleep(10);
        rmaOutputQueue.offer(message);
    }




}
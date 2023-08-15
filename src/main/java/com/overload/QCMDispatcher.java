package com.overload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class QCMDispatcher {



    private ExecutorService executorService;

    private Map<Integer, List<BlockingQueue<Message>>> qcmNodeMap;

    private List<BlockingQueue<Message>> qcmSiteList;

    private static final Logger LOGGER = Logger.getLogger(QCMDispatcher.class);

    private int qcmNodePerSite;



    public QCMDispatcher(Map<Integer, List<BlockingQueue<Message>>> qcmNodeMap, List<BlockingQueue<Message>> qcmSiteList, int qcmNodePerSite) {
        this.qcmNodeMap  = qcmNodeMap;
        this.qcmSiteList = qcmSiteList;
        this.qcmNodePerSite = qcmNodePerSite;
    }

    public void start(){
        List<Runnable> runnables= new LinkedList<>();;

        for (BlockingQueue<Message> qcmSite: qcmSiteList) {
                Runnable runnable = () -> {
                    int count = 0;
                    while (true) {
                        try {
                            Message message = qcmSite.take();
                            if(message.getType()=="TERM"){
                                LOGGER.info("Message type is TERM TYPE so breaking the loop from QCM Dispatcher :: "+message);
                                break;
                            }
                            processMessage(message);
                            count++;
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    LOGGER.info(count+" Message Pushed to QCM Node :: "+Thread.currentThread().getName());
                };

                runnables.add(runnable);
        }
        Runnable poisonPillTask = () -> {
            for(int i=0;i<qcmSiteList.size();i++) {
                for (int j = 0; j < qcmNodePerSite; j++) {
                    Message message = new Message("TERM");
                    message.setDestinationId(i);
                    message.setNodeId(j);
                    qcmNodeMap.get(i).get(j).offer(message);
                }
            }
        };
        runnables.add(poisonPillTask);

        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("QCMDispatcher-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        executorService = Executors.newFixedThreadPool(qcmSiteList.size(),factory);
        for(Runnable qcmProcess:runnables){
            executorService.submit(qcmProcess);
        }
    }



    public void stop() throws InterruptedException {

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
        int nodeId = message.getNodeId();
        int siteId = message.getDestinationId();
        qcmNodeMap.get(siteId).get(nodeId).offer(message);
    }

}
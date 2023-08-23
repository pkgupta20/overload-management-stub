package com.overload.threadpool;

import java.sql.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class QCMStub {

    List<BlockingQueue<Message>> qcmSiteNodeList;
    List<BlockingQueue<Message>> qcmResponseQueues;
    List<QCMNode>qcmNodes;

    public QCMStub(List<BlockingQueue<Message>> qcmSiteNodeList,List<BlockingQueue<Message>> qcmResponseQueues) {
        this.qcmSiteNodeList = qcmSiteNodeList;
        this.qcmResponseQueues = qcmResponseQueues;
        this.qcmNodes = initQcmNodes(qcmSiteNodeList);
    }

    private List<QCMNode> initQcmNodes(List<BlockingQueue<Message>> qcmSiteNodeList) {
        List<QCMNode> qcmNodeList = new ArrayList<>(qcmSiteNodeList.size());
        for (int i = 0; i < qcmSiteNodeList.size(); i++) {
            QCMRMAReceiver qcmrmaReceiver = new QCMRMAReceiver(qcmSiteNodeList.get(i),qcmResponseQueues.get(i));
            QCMNode qcmNode = new QCMNode(qcmrmaReceiver);
            qcmNodeList.add(qcmNode);
        }
        return qcmNodeList;
    }

    public void start(){
        qcmNodes.forEach(qcmNode -> qcmNode.start());
    }
}

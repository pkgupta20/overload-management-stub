package com.overload.threadpool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

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
        int count =0;
        for (int i = 0; i < qcmSiteNodeList.size(); i++) {
            QCMRMAReceiver qcmrmaReceiver = new QCMRMAReceiver(qcmSiteNodeList.get(i),qcmResponseQueues.get(count++));
            if(count == qcmResponseQueues.size())
                count=0;
            QCMNode qcmNode = new QCMNode(qcmrmaReceiver);
            qcmNodeList.add(qcmNode);
        }
        return qcmNodeList;
    }

    public void start(){
        qcmNodes.forEach(qcmNode -> qcmNode.start());
    }
}

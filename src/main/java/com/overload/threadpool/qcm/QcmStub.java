package com.overload.threadpool.qcm;

import com.overload.threadpool.util.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class QcmStub {

    List<BlockingQueue<Message>> qcmSiteNodeList;
    List<BlockingQueue<Message>> qcmResponseQueues;
    List<QcmNode> qcmNodes;


    public QcmStub(List<BlockingQueue<Message>> qcmSiteNodeList, List<BlockingQueue<Message>> qcmResponseQueues, int qcmAdapterThreadpoolSze, int qcmRmaThreadpoolsize, int processingTimeMs) {
        this.qcmSiteNodeList = qcmSiteNodeList;
        this.qcmResponseQueues = qcmResponseQueues;
        this.qcmNodes = initQcmNodes(qcmSiteNodeList, qcmAdapterThreadpoolSze, qcmRmaThreadpoolsize, processingTimeMs);

    }

    private List<QcmNode> initQcmNodes(List<BlockingQueue<Message>> qcmSiteNodeList, int qcmAdapterThreadpoolSze, int qcmRmaThreadpoolsize, int processingTimeMs) {
        List<QcmNode> qcmNodeList = new ArrayList<>(qcmSiteNodeList.size());
        int count = 0;
        for (BlockingQueue<Message> messages : qcmSiteNodeList) {
            QcmRmaReceiver qcmrmaReceiver = new QcmRmaReceiver(messages, qcmResponseQueues.get(count++), qcmAdapterThreadpoolSze, qcmRmaThreadpoolsize, processingTimeMs);
            if (count == qcmResponseQueues.size())
                count = 0;
            QcmNode qcmNode = new QcmNode(qcmrmaReceiver);
            qcmNodeList.add(qcmNode);
        }

        return qcmNodeList;
    }

    public void start() {
        qcmNodes.forEach(QcmNode::start);
    }

    public void stop(){
        for (QcmNode qcmNode: qcmNodes ) {
            qcmNode.getQcmRmaReceiver().stopQcmRmaReceiver();
        }
    }
}

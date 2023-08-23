package com.overload.threadpool;

public class QCMNode {

    QCMRMAReceiver qcmRmaReceiver;


    public QCMNode(QCMRMAReceiver qcmRmaReceiver) {
        this.qcmRmaReceiver = qcmRmaReceiver;

    }

    public void start(){
        qcmRmaReceiver.start();
    }
}

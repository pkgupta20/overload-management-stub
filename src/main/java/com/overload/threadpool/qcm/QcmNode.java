package com.overload.threadpool.qcm;

public class QcmNode {

    QcmRmaReceiver qcmRmaReceiver;


    public QcmNode(QcmRmaReceiver qcmRmaReceiver) {
        this.qcmRmaReceiver = qcmRmaReceiver;

    }

    public void start(){
        qcmRmaReceiver.start();
    }
}

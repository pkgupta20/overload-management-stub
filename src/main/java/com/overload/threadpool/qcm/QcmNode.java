package com.overload.threadpool.qcm;

public class QcmNode {


    private QcmRmaReceiver qcmRmaReceiver;

    public QcmRmaReceiver getQcmRmaReceiver() {
        return qcmRmaReceiver;
    }

    public QcmNode(QcmRmaReceiver qcmRmaReceiver) {
        this.qcmRmaReceiver = qcmRmaReceiver;

    }

    public void start(){
        qcmRmaReceiver.start();
    }
}

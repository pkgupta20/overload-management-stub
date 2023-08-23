package com.overload.threadpool;

import java.util.concurrent.BlockingQueue;

public class QCMRMAAdapterEmulator {
    QCMRMAReceiver qcmrmaReceiver;

    public QCMRMAAdapterEmulator(QCMRMAReceiver qcmrmaReceiver) {
        this.qcmrmaReceiver = qcmrmaReceiver;
    }
}

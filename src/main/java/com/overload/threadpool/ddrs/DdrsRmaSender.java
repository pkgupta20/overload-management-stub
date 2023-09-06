package com.overload.threadpool.ddrs;


import com.overload.threadpool.rma.RmaInputThreadPool;
import com.overload.threadpool.util.Message;

public class DdrsRmaSender {
    RmaInputThreadPool rmaInputThreadPool;

    public DdrsRmaSender(RmaInputThreadPool rmaInputThreadPool) {
        this.rmaInputThreadPool = rmaInputThreadPool;
    }
    public void submit(Message message){
        this.rmaInputThreadPool.execute(message);
    }
    public void shutdownRmaInputThreadPool() throws InterruptedException {
        this.rmaInputThreadPool.stop();
    }
}

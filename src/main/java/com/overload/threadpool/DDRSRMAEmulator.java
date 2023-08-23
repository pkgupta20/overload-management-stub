package com.overload.threadpool;

public class DDRSRMAEmulator {
    RMAInputThreadPool rmaInputThreadPool;

    public DDRSRMAEmulator(RMAInputThreadPool rmaInputThreadPool) {
        this.rmaInputThreadPool = rmaInputThreadPool;
    }
    public void submit(Message message){
        this.rmaInputThreadPool.execute(message);
    }
    public void shutdownRmaInputThreadPool() throws InterruptedException {
        this.rmaInputThreadPool.stop();
    }
}

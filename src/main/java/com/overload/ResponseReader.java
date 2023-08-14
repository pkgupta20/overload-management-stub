package com.overload;

import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.BlockingQueue;

public class ResponseReader {
    private final BlockingQueue<Message> messageQueue;
    private static final Logger LOGGER = Logger.getLogger(ResponseReader.class);

    private final String fileName;
    private int numberOfMessages;

    public ResponseReader(BlockingQueue<Message> mQueue,String fileName, int numberOfMessages){
        messageQueue = mQueue;
        this.fileName = fileName;
        this.numberOfMessages = numberOfMessages;
    }
    public void start() throws IOException, InterruptedException {
        long count = writeDataToFile(messageQueue);

        LOGGER.info("Received message: " + count);
    }

    private long writeDataToFile(BlockingQueue<Message> messageQueue) throws InterruptedException, IOException {
        FileWriter fileWriter = new FileWriter(fileName);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        long count = 0;
        while(numberOfMessages > count){
            Message m = messageQueue.take();
            //LOGGER.info("Message in Response Reader :: "+m);
            printWriter.printf(m.getType()+"\n");
            count++;
        }
        printWriter.flush();
        printWriter.close();
        return count;
    }

}
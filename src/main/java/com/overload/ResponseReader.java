package com.overload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class ResponseReader {
    private final BlockingQueue<Message> messageQueue;
    private static final Logger LOGGER = Logger.getLogger(ResponseReader.class);

    private final String fileName;
    private int numberOfMessages;
    private int numberOfThreads;
    private ExecutorService service;

    public ResponseReader(BlockingQueue<Message> mQueue,int numberOfThreads,String fileName){
        messageQueue = mQueue;
        this.fileName = fileName;
        this.numberOfMessages = numberOfMessages;
        this.numberOfThreads = numberOfThreads;
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("QCMProcessor-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        service = Executors.newFixedThreadPool(numberOfThreads, factory);

    }
    public void start() throws IOException, InterruptedException {
        List<Runnable> runnables = new ArrayList<>(numberOfThreads);

        final Queue<String> fileNames = new LinkedBlockingQueue<>();
        for (int i = 0; i < numberOfThreads; i++) {
            fileNames.offer(fileName+"-"+(i+1));
        }
        for (int i = 0; i < numberOfThreads; i++) {
            Runnable runnable = () -> {
                try(FileWriter fileWriter = new FileWriter(fileNames.remove(), true)) {
                    while (true) {
                        try {
                            Message message = messageQueue.take();
                            if (message.getType() == "TERM")
                                break;
                            consumeMessage(message,fileWriter);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }catch (IOException e) {
                    e.printStackTrace();
                }
                LOGGER.info("Write Done in " +Thread.currentThread().getName() );

            };
            runnables.add(runnable);

        }
        for (Runnable producer: runnables) {
            service.execute(producer);
        }

    }

    private void consumeMessage(Message message,FileWriter fileWriter) throws InterruptedException, IOException {
        Thread.sleep(5);
        synchronized (fileWriter){
            fileWriter.write(message.toString() + System.lineSeparator());
//            System.out.println(" wrote to file: " + message);

        }


    }

    public void shutdown() throws InterruptedException, IOException {
        service.shutdown();
        service.awaitTermination(1000, TimeUnit.SECONDS);
        mergeFiles(fileName,numberOfThreads);
    }

    private void mergeFiles(String fileName, int numberOfThreads) throws IOException {
        FileWriter fileWriter = new FileWriter(fileName,true);

        for (int i = 0; i < numberOfThreads; i++) {
            String sourceFile = fileName+"-"+(i+1);
            try(FileReader filereader = new FileReader(sourceFile)) {
                BufferedReader br = new BufferedReader(filereader);
                String str = br.readLine();
                while (str != null) {
                    fileWriter.write(str+System.lineSeparator());
                    str = br.readLine();
                }
            }

        }
        fileWriter.flush();
        fileWriter.close();

    }


}
package com.overload;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

public class ResponseReader {
    private final BlockingQueue<Message> responseQueue;
    private static final Logger LOGGER = Logger.getLogger(ResponseReader.class);

    private final String fileName;
    private int numberOfMessages;
    private int responseQueueConsumers;
    private ExecutorService service;

    public ResponseReader(BlockingQueue<Message> mQueue,int responseQueueConsumers,String fileName){
        responseQueue = mQueue;
        this.fileName = fileName;
        this.numberOfMessages = numberOfMessages;
        this.responseQueueConsumers = responseQueueConsumers;
        BasicThreadFactory factory = new BasicThreadFactory.Builder()
                .namingPattern("ResponseReader-%d")
                .priority(Thread.MAX_PRIORITY)
                .build();
        service = Executors.newFixedThreadPool(responseQueueConsumers, factory);

    }
    public void start() throws IOException, InterruptedException {
        List<Runnable> runnables = new ArrayList<>(responseQueueConsumers);

        final Queue<String> fileNames = new LinkedBlockingQueue<>();
        for (int i = 0; i < responseQueueConsumers; i++) {
            fileNames.offer(fileName+"-"+(i+1));
        }
        for (int i = 0; i < responseQueueConsumers; i++) {
            Runnable runnable = () -> {
                int count = 0;
                try(FileWriter fileWriter = new FileWriter(fileNames.remove(), true)) {
                    while (true) {
                        try {
                            Message message = responseQueue.take();
                            if (message.getType() == "TERM")
                                break;
                            consumeMessage(message,fileWriter);
                            count++;
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }catch (IOException e) {
                    e.printStackTrace();
                }
                LOGGER.info(count+" Messages Written Done in " +Thread.currentThread().getName() );

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
        while (!service.isTerminated()){
            Thread.sleep(100);
        }
        service.awaitTermination(1000, TimeUnit.SECONDS);
        System.out.println("Message consumption finished, now merging will be started...");
        long startTime = System.currentTimeMillis();
        mergeFiles(fileName,responseQueueConsumers);
        long endTime  = System.currentTimeMillis();
        System.out.println("Merging finished in "+(endTime - startTime)+" miliseconds");
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
package com.overload.threadpool.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MetricSampler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricSampler.class);
    private static final long NANO_TO_MILIS = 1000000;

    private final ScheduledExecutorService scheduledExecutorService;
    private final BlockingQueue<Message> marbenQueue;
    private final int pollTimeInSeconds;
    private final MeterRegistry registry;
    private final Timer timer;
    private AtomicInteger timedOutMessageCount;

    private long previousIterationCount;

    private final int timeoutMs;

    private final Map<UUID, Message> messageMap;
    public MetricSampler(MeterRegistry registry, Timer timer, int pollTimeInSeconds, BlockingQueue<Message> marbenQueue, Map<UUID, Message> messageMap, int timeoutMs){
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
        this.pollTimeInSeconds = pollTimeInSeconds;
        this.registry = registry;
        this.timer = timer;
        this.marbenQueue = marbenQueue;
        this.messageMap = messageMap;
        this.timeoutMs = timeoutMs;
        this.timedOutMessageCount = new AtomicInteger(0);
        this.previousIterationCount = 0;
    }
    public void start() {
        List<String> headers = Arrays.asList("count","delta","0.5","0.90","0.95","max","marbenQ","TimedOut");
        StringBuilder headerResult = new StringBuilder();
        for (String header: headers ) {
            headerResult.append(String.format("%12s",header));
        }

        LOGGER.info("{}",headerResult);
        Runnable runnable = () -> {
            StringBuilder resultValues = new StringBuilder();
            int timedOutInCurrentIteration;
            ValueAtPercentile[] valueAtPercentiles = timer.takeSnapshot().percentileValues();
            long currentIterationCount = timer.count();
            List<String> metricOutputParams = new ArrayList<>(8);
            metricOutputParams.add(String.valueOf(currentIterationCount));
            metricOutputParams.add(String.valueOf(currentIterationCount - previousIterationCount));
            metricOutputParams.add(new DecimalFormat("#").format(valueAtPercentiles[0].value(TimeUnit.MILLISECONDS)));
            metricOutputParams.add(new DecimalFormat("#").format(valueAtPercentiles[1].value(TimeUnit.MILLISECONDS)));
            metricOutputParams.add(new DecimalFormat("#").format(valueAtPercentiles[2].value(TimeUnit.MILLISECONDS)));
            metricOutputParams.add(new DecimalFormat("#").format(timer.max(TimeUnit.MILLISECONDS)));
            metricOutputParams.add(String.valueOf(marbenQueue.size()));

            timedOutInCurrentIteration = removeTimedOutMessages(this.messageMap);
            metricOutputParams.add(String.valueOf(timedOutInCurrentIteration));
            timedOutMessageCount.addAndGet(timedOutInCurrentIteration);
            previousIterationCount = currentIterationCount;

            for (String output: metricOutputParams ) {
                resultValues.append(String.format("%12s",output));
            }

            LOGGER.info("{}", resultValues);
        };
        scheduledExecutorService.scheduleAtFixedRate(runnable, 0, pollTimeInSeconds, TimeUnit.SECONDS);
    }

    private int removeTimedOutMessages(Map<UUID, Message> messageMap) {
        AtomicInteger timedOutMessageCount = new AtomicInteger();
        messageMap.keySet().forEach(uuid ->{
            long elapsedTime = System.nanoTime() - messageMap.get(uuid).getInitTime();
            long elapsedTimeMs = elapsedTime / NANO_TO_MILIS;
            if(elapsedTimeMs > timeoutMs){
                messageMap.remove(uuid);
                timedOutMessageCount.incrementAndGet();
            }
        });

        return timedOutMessageCount.get();
    }



    public void stop() throws InterruptedException {
        registry.close();
        scheduledExecutorService.shutdown();
        boolean result = scheduledExecutorService.awaitTermination(100, TimeUnit.MILLISECONDS);
        LOGGER.info("Metric Sampler is shutting down gracefully:"+result);
    }

    public int getTimedOutMessageCount() {
        return timedOutMessageCount.get();
    }
    public int incTimedOutMessageCount(){
        return timedOutMessageCount.incrementAndGet();
    }
}

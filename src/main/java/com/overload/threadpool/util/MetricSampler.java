package com.overload.threadpool.util;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricSampler {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricSampler.class);

    private final ScheduledExecutorService scheduledExecutorService;
    private final int pollTimeInSeconds;
    private final MeterRegistry registry;
    private Timer timer;
    public MetricSampler(MeterRegistry registry, Timer timer, int pollTimeInSeconds){
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
        this.pollTimeInSeconds = pollTimeInSeconds;
        this.registry = registry;
        this.timer = timer;
    }
    public void start() {
        Runnable runnable = () -> {
            ValueAtPercentile[] valueAtPercentiles = timer.takeSnapshot().percentileValues();
            LOGGER.info("{}th Percentile Value is:{}",(valueAtPercentiles[0].percentile() * 100.0), valueAtPercentiles[0].value(TimeUnit.MILLISECONDS));
            LOGGER.info("{}th Percentile Value is:{}",(valueAtPercentiles[1].percentile() * 100.0), valueAtPercentiles[1].value(TimeUnit.MILLISECONDS));
            LOGGER.info("{}th Percentile Value is:{}",(valueAtPercentiles[2].percentile() * 100.0), valueAtPercentiles[2].value(TimeUnit.MILLISECONDS));
            LOGGER.info("Total Sample count:{}"+ timer.count());
        };
        scheduledExecutorService.scheduleAtFixedRate(runnable, 0, pollTimeInSeconds, TimeUnit.SECONDS);
    }

    public void stop() {
        registry.close();
        scheduledExecutorService.shutdown();
        LOGGER.info("Metric Sampler is shutting down.");
    }

}

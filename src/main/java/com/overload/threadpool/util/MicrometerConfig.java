package com.overload.threadpool.util;

import io.micrometer.core.instrument.simple.CountingMode;
import io.micrometer.core.instrument.simple.SimpleConfig;

import java.time.Duration;

public class MicrometerConfig implements SimpleConfig {
    @Override
    public CountingMode mode() {
        return CountingMode.CUMULATIVE;
    }

    @Override
    public Duration step() {
        return Duration.ofSeconds(20);
    }

    @Override
    public String get(String s) {
        return null;
    }
}

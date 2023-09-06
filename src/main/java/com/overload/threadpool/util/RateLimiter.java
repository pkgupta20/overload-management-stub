package com.overload.threadpool.util;

public class RateLimiter {
    private static final long ONE_MILLISECOND = 1_000_000L;
    private static final long NS_IN_SECOND = 1_000_000_000L;

    private final int tps;

    private int count;
    private long startPointNs;

    public RateLimiter(int tps) {
        this.tps = tps;
    }

    public void start() {
        startPointNs = now();
    }

    public void delayIfNeeded() {
        if (tps <= 0) { // rate limiting is disabled
            return;
        }
        count++;
        long delta = count * NS_IN_SECOND / tps;
        long delay = startPointNs + delta - now();
        if (delay > ONE_MILLISECOND) {
            try {
                Thread.sleep(delay / ONE_MILLISECOND);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void reset() {
        count = 0;
    }

    private static long now() {
        return System.nanoTime();
    }
}


package kafka;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TimeStat {
    private static AtomicLong totalCost = new AtomicLong();
    private static AtomicInteger totalCount = new AtomicInteger();

    public static void record(long cost) {
        totalCost.addAndGet(cost);
        totalCount.incrementAndGet();
    }

    static {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Runnable task = () -> {
            long cost = totalCost.getAndSet(0);
            long count = totalCount.getAndSet(0);
            System.out.println("action ::: avg cost: " + (cost / count) + " ms");
        };
        scheduler.scheduleAtFixedRate(task, 0, 5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                try {
                    System.out.println("1");
                    Thread.sleep(1000);
                    System.out.println("2");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        executorService.shutdown();
        System.out.println("over");
    }
}

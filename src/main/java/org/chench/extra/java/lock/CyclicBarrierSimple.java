package org.chench.extra.java.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * CyclicBarrier示例：
 * 一组线程同时到达栅栏位置之后才开始执行
 *
 * @author chench
 * @desc org.chench.extra.java.lock.CyclicBarrierSimple
 * @date 2023.07.16
 */
public class CyclicBarrierSimple {
    public static void main(String[] args) {
        // 使用CyclicBarrier模拟多线程同时到达栅栏位置之后才开始执行
        int n = 10; // 线程数
        CyclicBarrier barrier = new CyclicBarrier(n);
        List resultList = new ArrayList(n);
        List<Thread> threadList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int index = i;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep((new Random().nextInt(10)) * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        System.out.println(String.format("线程%s准备好了", index));
                        barrier.await();
                        System.out.println(String.format("线程%s执行完毕!", index));
                        resultList.add(String.format("result%s", index));
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }
            });
            threadList.add(thread);
            thread.start();
        }
        threadList.forEach(thread -> {
            try {
                // 等待线程执行完毕
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        System.out.println(String.format("所有线程都执行完毕：%s", resultList));

        // 栅栏可以重复使用
        System.out.println("继续使用栅栏...");
        threadList.clear();
        resultList.clear();
        for (int i = 0; i < n; i++) {
            final int index = i;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep((new Random().nextInt(10)) * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(String.format("线程%s准备好了", index));
                    try {
                        barrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    System.out.println(String.format("线程%s又执行完毕了", index));
                    resultList.add(String.format("result%s", index));
                }
            });
            threadList.add(thread);
            thread.start();
        }

        threadList.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

        // 可以等所有线程都执行完毕之后再这里进行总的汇总
        System.out.println(String.format("所有线程都执行完毕了：%s", resultList));

    }
}

package org.chench.extra.java.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * CountDownLatch示例：
 * 等待一组线程执行完毕之后再继续执行后续操作
 *
 * @author chench
 * @desc org.chench.extra.java.lock.CountDownLatchSimple
 * @date 2023.07.16
 */
public class CountDownLatchSimple {
    public static void main(String[] args) throws InterruptedException {
        summary();
    }

    // 模拟同时调用多个接口获取数据，汇总后返回给客户端
    private static void summary() throws InterruptedException {
        // 模拟需要调用10个方法才能获取到数据
        int count = 10;
        // 每个方法的返回结果都保存到列表中
        List<Object> result = new ArrayList<>(count);
        // 使用CountDownLatch来控制10个方法都执行完毕之后再汇总数据
        CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep((new Random().nextInt(10)) * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    result.add(String.format("result%s", index));
                    // 每个线程在执行完毕之后都必须将计数器减1
                    latch.countDown();
                    System.out.println(String.format("线程%s结束了", Thread.currentThread().getName()));
                }
            }).start();
        }
        // 等待前面的所有方法都请求完毕并得到返回结果之后再进行处理
        System.out.println("等待所有方法执行完毕...");
        latch.await();

        // 从结果列表中获取返回信息
        System.out.println(String.format("最终的汇总结果：%s", result));
    }
}

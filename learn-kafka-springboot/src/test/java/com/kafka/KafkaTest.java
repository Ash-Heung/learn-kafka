package com.kafka;

import com.alibaba.fastjson.JSON;
import com.kafka.producer.Demo01Producer;
import com.kafka.producer.Demo02Producer;
import com.kafka.producer.Demo04Producer;
import com.kafka.producer.Demo05Producer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaApplication.class)
public class KafkaTest {

    int id = (int) (System.currentTimeMillis() / 1000);

    @Autowired
    Demo01Producer demo01Producer;

    @Autowired
    Demo02Producer demo02Producer;

    @Autowired
    private Demo04Producer demo04Producer;

    @Test
    public void testSend(){
        try {
            SendResult sendResult = demo01Producer.syncSend(id);
            log.info("[testSyncSend][发送编号：[{}] 发送结果：[{}]]", id, sendResult);
            // 阻塞等待，保证消费
            new CountDownLatch(1).await();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @SneakyThrows
    @Test
    public void testAsyncSend(){
        demo01Producer.asyncSend(id).addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {

            @Override
            public void onFailure(Throwable e) {
                log.info("[testASyncSend][发送编号：[{}] 发送异常]]", id, e);
            }

            @Override
            public void onSuccess(SendResult<Object, Object> result) {
                log.info("[testASyncSend][发送编号：[{}] 发送成功，结果为：[{}]]", id, result);
            }

        });

        // 阻塞等待，保证消费
        new CountDownLatch(1).await();
    }

    @Test
    public void testDemo02() throws InterruptedException {
        log.info("[testASyncSend][开始执行]");

        for (int i = 0; i < 5; i++) {
            int id = (int) (System.currentTimeMillis() / 1000);
            demo02Producer.asyncSend(id).addCallback(new ListenableFutureCallback<SendResult<Object, Object>>() {

                @Override
                public void onFailure(Throwable e) {
                    log.info("[testASyncSend][发送编号：[{}] 发送异常]]", id, e);
                }

                @Override
                public void onSuccess(SendResult<Object, Object> result) {
                    log.info("[testASyncSend][发送编号：[{}] 发送成功，结果为：[{}]]", id, result);
                }

            });

            // 故意每条消息之间，隔离 10 秒
            Thread.sleep(10 * 1000L);
        }

        // 阻塞等待，保证消费
        new CountDownLatch(1).await();
    }

    @SneakyThrows
    @Test
    public void testDemo04(){
        int id = (int) (System.currentTimeMillis() / 1000);
        SendResult result = demo04Producer.syncSend(id);
        log.info("[testSyncSend][发送编号：[{}] 发送结果：[{}]]", id, result);

        // 阻塞等待，保证消费
        new CountDownLatch(1).await();
    }

    @Autowired
    private Demo05Producer demo05Producer;

    @SneakyThrows
    @Test
    public void testDemo05(){
        int id = (int) (System.currentTimeMillis() / 1000);
        SendResult result = demo05Producer.syncSend(id);
        log.info("[testSyncSend][发送编号：[{}] 发送结果：[{}]]", id, result);

        // 阻塞等待，保证消费
        new CountDownLatch(1).await();
    }


    private void print(Object sendResult){
        System.out.println("********************************************");
        System.out.println(JSON.toJSONString(sendResult));
        System.out.println("********************************************");
    }

}

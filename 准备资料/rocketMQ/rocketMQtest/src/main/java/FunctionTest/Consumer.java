package FunctionTest;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by g on 2016/5/25.
 */
public class Consumer {
    public static void main(String[] args) {
        DefaultMQPushConsumer pushConsumer =
                new DefaultMQPushConsumer("PushConsumer");
        DefaultMQPushConsumer pullConsumer =
                new DefaultMQPushConsumer("PullConsumer");
        DefaultMQPushConsumer bothConsumer =
                new DefaultMQPushConsumer("BothConsumer");
        //设置nameserver
        pushConsumer.setNamesrvAddr("192.168.1.51:9876");
        pullConsumer.setNamesrvAddr("192.168.1.51:9876");
        bothConsumer.setNamesrvAddr("192.168.1.51:9876");
        try {
            //订阅PushTopic下Tag为push的消息
            pushConsumer.subscribe("lelesays", "tag1");
            pullConsumer.subscribe("lelesays", "tag2");
            bothConsumer.subscribe("lelesays", "*");
            //程序第一次启动从消息队列头取数据,push注册回调接口。
            pushConsumer.setConsumeFromWhere(
                    ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            pullConsumer.setConsumeFromWhere(
                    ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            bothConsumer.setConsumeFromWhere(
                    ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            pushConsumer.registerMessageListener(
                    new MessageListenerConcurrently() {
                        public ConsumeConcurrentlyStatus consumeMessage(
                                List<MessageExt> list,
                                ConsumeConcurrentlyContext Context) {
                            Message msg = list.get(0);
                       //     System.out.println(msg.toString());
                            System.out.println("tag1:"+new String(msg.getBody()));
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    }
            );
            pushConsumer.start();
            bothConsumer.registerMessageListener(
                    new MessageListenerConcurrently() {
                        public ConsumeConcurrentlyStatus consumeMessage(
                                List<MessageExt> list,
                                ConsumeConcurrentlyContext Context) {
                            Message msg = list.get(0);
                            //     System.out.println(msg.toString());
                            System.out.println("tag2:"+new String(msg.getBody()));
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    }
            );
            bothConsumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

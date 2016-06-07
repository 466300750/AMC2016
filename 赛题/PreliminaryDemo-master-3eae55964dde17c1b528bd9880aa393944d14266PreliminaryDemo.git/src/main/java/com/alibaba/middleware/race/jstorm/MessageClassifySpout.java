package com.alibaba.middleware.race.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MessageClassifySpout implements IRichSpout {
	private static Logger LOG = LoggerFactory.getLogger(MessageClassifySpout.class);
	SpoutOutputCollector _collector;
//	long startTime;
//	boolean isStatEnable;
//	int sendNumPerNexttuple;

	DefaultMQPushConsumer taobaoConsumer;
	DefaultMQPushConsumer tmallConsumer;
	DefaultMQPushConsumer paymentConsumer;
	Map<Long, Byte> orderIdTypeMap = new HashMap<Long, Byte>();
	Map<Long, OrderMessage> orderIdMessageMap = new HashMap<Long, OrderMessage>();


	// open�ǵ�task������ִ�еĳ�ʼ������
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
//		_rand = new Random();
//		sendingCount = 0;
//		startTime = System.currentTimeMillis();
//		sendNumPerNexttuple = JStormUtils.parseInt(conf.get("send.num.each.time"), 1);
//		isStatEnable = JStormUtils.parseBoolean(conf.get("is.stat.enable"), false);

		taobaoConsumer = new DefaultMQPushConsumer("taobaoConsumer");
		tmallConsumer = new DefaultMQPushConsumer("tmallConsumer");
		paymentConsumer = new DefaultMQPushConsumer("paymentConsumer");
		/**
		 * ����Consumer��һ�������ǴӶ���ͷ����ʼ���ѻ��Ƕ���β����ʼ����<br>
		 * ����ǵ�һ����������ô�����ϴ����ѵ�λ�ü�������
		 */
		taobaoConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		tmallConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		paymentConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

		// �ڱ��ش��broker��,�ǵ�ָ��nameServer�ĵ�ַ
		taobaoConsumer.setNamesrvAddr("192.168.1.51:9876");
		tmallConsumer.setNamesrvAddr("192.168.1.51:9876");
		paymentConsumer.setNamesrvAddr("192.168.1.51:9876");
		try {
			taobaoConsumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			tmallConsumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			paymentConsumer.subscribe(RaceConfig.MqPayTopic, "*");
		} catch (MQClientException e) {
			LOG.error("������Ϣ����");
			e.printStackTrace();
		}
		
		tmallConsumer.registerMessageListener(new MessageListenerConcurrently() {
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				for (MessageExt msg : msgs) {
					byte[] body = msg.getBody();
					if (body.length == 2 && body[0] == 0 && body[1] == 0) {
						// Info: ������ֹͣ��������, ������ζ�����Ͻ���
						System.out.println("Got the end signal");
						continue;
					}
					
					OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
					orderIdTypeMap.put(orderMessage.getOrderId(), (byte) 0x00);
					orderIdMessageMap.put(orderMessage.getOrderId(), new OrderMessage(orderMessage));
//					System.out.println("tmallConsumer"+orderMessage);
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		taobaoConsumer.registerMessageListener(new MessageListenerConcurrently() {
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				for (MessageExt msg : msgs) {
					byte[] body = msg.getBody();
					if (body.length == 2 && body[0] == 0 && body[1] == 0) {
						// Info: ������ֹͣ��������, ������ζ�����Ͻ���
						System.out.println("Got the end signal");
						continue;
					}
					OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);

					orderIdTypeMap.put(orderMessage.getOrderId(), (byte) 0x01);
//					System.out.println("taobaoConsumer"+orderMessage);
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		paymentConsumer.registerMessageListener(new MessageListenerConcurrently() {
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				for (MessageExt msg : msgs) {
					byte[] body = msg.getBody();
					if (body.length == 2 && body[0] == 0 && body[1] == 0) {
						// Info: ������ֹͣ��������, ������ζ�����Ͻ���
						_collector.emit("Taobao_Stream_Id", new Values("0x00"));
						_collector.emit("Tmall_Stream_Id", new Values("0x00"));
					}
					PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);

//					System.out.println("paymentMessage"+paymentMessage);
					// һ���ܱ�֤������Ϣ�ȵ�����
					long orderId = paymentMessage.getOrderId();
					byte type = orderIdTypeMap.get(orderId);
					if (type == 0x01) { // 01�����Ա���00������è����������
						_collector.emit("Taobao_Stream_Id", new Values(paymentMessage));		
					} else if (type == 0x00) {
						_collector.emit("Tmall_Stream_Id", new Values(paymentMessage));	
					} else {
						LOG.error("δ֪��Ϣ����");
					}
					Iterator<Map.Entry<Long, OrderMessage>> entries = orderIdMessageMap.entrySet().iterator();
					while (entries.hasNext()) {
						Map.Entry<Long, OrderMessage> entry = entries.next();
						if (entry.getKey() == paymentMessage.getOrderId()) {
							if (entry.getValue().getTotalPrice() != paymentMessage.getPayAmount()) {
								double oldPrice = entry.getValue().getTotalPrice();
								entry.getValue().setTotalPrice(oldPrice - paymentMessage.getPayAmount());
							} else { //���ǹ���ĳ�����������һ��������Ϣ
								orderIdTypeMap.remove(entry.getKey());
								entries.remove();
							}
						}
					}
				}
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});

		try {
			tmallConsumer.start();
			taobaoConsumer.start();
			paymentConsumer.start();
		} catch (MQClientException e) {
			LOG.error("������Ϣ����ʱ����");
			e.printStackTrace();
		}
	}

	// nextTuple ��spoutʵ�ֺ��ģ� nextuple����Լ����߼�����ÿһ��ȡ��Ϣ����collector ����Ϣemit��ȥ��
	@Override
	public void nextTuple() {
		
	}

	@Override
	public void ack(Object id) {
		// Ignored
	}

	@Override
	public void fail(Object id) {
		_collector.emit(new Values(id), id);
	}

	//����spout�������ݣ�ÿ���ֶεĺ���
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("Taobao_Stream_Id", new Fields("message"));
		declarer.declareStream("Tmall_Stream_Id", new Fields("message"));
	}

	// close�ǵ�task��shutdown��ִ�еĶ���
	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	// activate �ǵ�task������ʱ�������Ķ���
	@Override
	public void activate() {
		// TODO Auto-generated method stub

	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
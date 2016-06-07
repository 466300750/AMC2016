package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.middleware.race.model.PaymentMessage;

public class TmallStatistic implements IRichBolt {
    OutputCollector collector;
    Map<Long, Double> res = new HashMap<Long, Double>();
    long prePayTime = 0;

    @Override
    public void execute(Tuple tuple) {
    	if (tuple.getValue(0).equals("0x00")) {//�յ��������ı�־
			if(prePayTime == 0) {//��һ�ξ�ֱ���յ���������־
				return; 
			} else { //�����һ�����ڵ���Ϣ���ͳ�ȥ
				if (res.containsKey(prePayTime)) {
					collector.emit(new Values(prePayTime, res.get(prePayTime)));
					res.remove(prePayTime);
				}
			}			
		} else {
			PaymentMessage payment = (PaymentMessage) tuple.getValue(0);
			long createTime = (payment.getCreateTime() / 1000 / 60) * 60;
			if (!res.containsKey(createTime)) {
				res.put(createTime, payment.getPayAmount());
			} else {
				res.put(createTime, res.get(createTime) + payment.getPayAmount());
			}

			if (createTime != prePayTime) { // �Ѿ�������һ��һ����,�����ݴ�����ͬʱɾ��map�ж����ݵĴ洢
				if (res.containsKey(prePayTime)) {
					collector.emit(new Values(prePayTime, res.get(prePayTime)));
					res.remove(prePayTime);
				}
				prePayTime = createTime;
			}
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("minunet", "amount"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
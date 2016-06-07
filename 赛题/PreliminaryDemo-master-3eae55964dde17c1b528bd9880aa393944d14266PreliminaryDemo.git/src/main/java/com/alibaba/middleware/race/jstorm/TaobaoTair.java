package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TaobaoTair implements IRichBolt {		
	DefaultTairManager tairManager;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		tairManager = new DefaultTairManager();
		ArrayList<String> confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer);
		confServers.add(RaceConfig.TairSalveConfigServer);				
		tairManager.setConfigServerList(confServers);		
		tairManager.setGroupName(RaceConfig.TairGroup);	
		tairManager.init();
	}

	@Override
	public void execute(Tuple input) {
		long minuteTime = input.getLong(0);
		System.out.println("******************************"+minuteTime);
		double money = input.getDouble(1);
		ResultCode resultCode = tairManager.put(RaceConfig.TairNamespace, RaceConfig.prex_tmall + minuteTime, money);
		if(resultCode.isSuccess()) {
			System.out.println("insert success");
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

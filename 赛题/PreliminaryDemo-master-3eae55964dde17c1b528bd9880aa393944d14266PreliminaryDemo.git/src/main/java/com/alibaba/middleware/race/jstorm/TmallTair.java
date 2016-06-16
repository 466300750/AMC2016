package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class TmallTair implements IRichBolt {
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
		double money = input.getDouble(1);
		String key = RaceConfig.prex_tmall + minuteTime;
		/*ResultCode rc = tairManager.put(RaceConfig.TairNamespace, key, money);
		if(rc.isSuccess()) {
			System.out.println("############首次存取: "+ key +"  "+money);
		} else if(ResultCode.VERERROR.equals(rc)) {
			Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, key);
			int version = result.getValue().getVersion();
			double amount = (double) result.getValue().getValue();
			tairManager.put(RaceConfig.TairNamespace, key, money+amount, version);
			System.out.println("##再次存取: "+ key+"  "+(money+amount));
		}*/
		Result<DataEntry> result = tairManager.get(RaceConfig.TairNamespace, key);
		if(result.isSuccess()) {
			if(result.getValue() != null){
				double amount = (double) result.getValue().getValue();
				int version = result.getValue().getVersion();
				tairManager.put(RaceConfig.TairNamespace, key, money+amount/*, version*/);
				System.out.println("##再次存取: "+ key+"  "+(money+amount));
			}else {
				tairManager.put(RaceConfig.TairNamespace, key, money);
				System.out.println("############首次存取: "+ key +"  "+money);
			}
		}
		
//		System.out.println(RaceConfig.prex_tmall + minuteTime+": "+money);
//		if(resultCode.isSuccess()) {
//			System.out.println(tairManager.get(RaceConfig.TairNamespace, RaceConfig.prex_tmall + minuteTime));
//			System.out.println("tmall insert success");
//		}
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

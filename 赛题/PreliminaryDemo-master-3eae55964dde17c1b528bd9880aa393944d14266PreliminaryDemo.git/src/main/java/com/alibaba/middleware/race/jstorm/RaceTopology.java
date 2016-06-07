package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);
    public static int Interval=6000;

    public static void main(String[] args) throws Exception {
    	//topology所有自定义的配置均放入这个conf
        Config conf = new Config();
        int classify_Parallelism_hint = 1;
        int statistic_Parallelism_hint = 2;
        int tair_Parallelism_hint = 2;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("classify", new MessageClassifySpout(), classify_Parallelism_hint);
      //表示接收spout的数据，并且以shuffle方式，即每个spout随机轮询发送tuple到下一级bolt中
        builder.setBolt("taobao", new TaobaoStatistic(), statistic_Parallelism_hint).shuffleGrouping("classify", "Tmall_Stream_Id");
        builder.setBolt("tmall", new TmallStatistic(), statistic_Parallelism_hint).shuffleGrouping("classify", "Taobao_Stream_Id");
        builder.setBolt("taobaoTair", new TaobaoTair(), tair_Parallelism_hint).shuffleGrouping("taobao");
        builder.setBolt("tmallTair", new TmallTair(), tair_Parallelism_hint).shuffleGrouping("tmall");
        String topologyName = RaceConfig.JstormTopologyName;

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, conf, builder.createTopology());

        Thread.sleep(1000);

        Thread.sleep(20*20*1000);

        cluster.killTopology(topologyName);
        cluster.shutdown();
//        try {
//            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
//        } catch (Exception e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }
}
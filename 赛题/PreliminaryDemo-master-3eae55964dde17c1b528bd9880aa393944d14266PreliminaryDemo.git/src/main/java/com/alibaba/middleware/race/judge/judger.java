package com.alibaba.middleware.race.judge;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.impl.DefaultTairManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static com.alibaba.middleware.race.judge.MsgType.TaobaoOrderMsg;
import static com.alibaba.middleware.race.judge.MsgType.TmallOrderMsg;

/**
 * Created by g on 2016/6/7.
 * 由于在本地进行暴力求结果，适用于小规模测试，大规模的话，还是两个topo，一个暴力的，一个非暴力的，对比结果
 * 可以用在topo里，也可以用在producer里
 */
public class judger {
    public volatile static boolean open=false;
    static DefaultTairManager tairManager =new DefaultTairManager();
    //OrderId和MsgType的map
    static ConcurrentHashMap<Long,MsgType> idTypeMap=new ConcurrentHashMap<>();
    //PaymentMessage的BQ
    static LinkedBlockingQueue<PaymentMessage> paymentMessageBQ=new LinkedBlockingQueue<>();

    static HashMap<Long,Double> TaobaoCounter=new HashMap<>();
    static HashMap<Long,Double> TmallCounter=new HashMap<>();
    static HashMap<Long,Double> PcCounter=new HashMap<>();
    static HashMap<Long,Double> mobileCounter=new HashMap<>();
    //region Description :初始化tiarManager
    static {
        ArrayList<String> confServers = new ArrayList<String>();
        confServers.add(RaceConfig.TairConfigServer);
        confServers.add(RaceConfig.TairSalveConfigServer);
        tairManager.setConfigServerList(confServers);
        tairManager.setGroupName(RaceConfig.TairGroup);
        tairManager.init();
    }
    //endregion
    //获取msg钩子函数
    public static void getMsg(Object msg,MsgType type){
        if(!open)
            return;
        switch (type){
            case TaobaoOrderMsg:idTypeMap.put(((OrderMessage)msg).getOrderId(),TaobaoOrderMsg);break;
            case TmallOrderMsg:idTypeMap.put(((OrderMessage)msg).getOrderId(),TmallOrderMsg);break;
            case PaymentMsg:paymentMessageBQ.add((PaymentMessage)msg);break;
            default:break;
        }
    }
    public static void startJudge(){
        if(!open)
            return;
        //region Description
        //将数据整理进入四个Counter
        PaymentMessage msg;
        while (!paymentMessageBQ.isEmpty()){
            msg=paymentMessageBQ.poll();
            long orderId=msg.getOrderId();
            MsgType type=idTypeMap.get(orderId);
            double amount=msg.getPayAmount();
            long minite=getMinite(msg.getCreateTime());
            switch (type){
                case TaobaoOrderMsg:putInMap(TaobaoCounter,minite,amount);break;
                case TmallOrderMsg:putInMap(TmallCounter,minite,amount);break;
            }
            switch (msg.getPaySource()){
                case 0:putInMap(PcCounter,minite,amount);break;
                case 1:putInMap(mobileCounter,minite,amount);break;
            }
        }
        //endregion
        //录入完成，输入结果
        for(long time:TaobaoCounter.keySet()){
            double amount=TaobaoCounter.get(time);
            String tairKey="platformTaobao_"+time;
            System.out.println("   TaoBao time:"+time+" CheckAmount:"+amount);
            Result<DataEntry> data= tairManager.get(RaceConfig.TairNamespace,tairKey);
            double jstromRes=(double)data.getValue().getValue();
            boolean same=jstromRes==amount?true:false;
            System.out.println(same+"   TaoBao time:"+time+" CheckAmount:"+amount+"  jstromRes"+jstromRes);
        }
        for(long time:TmallCounter.keySet()){
            double amount=TmallCounter.get(time);
            String tairKey="platformTmall_"+time;
            System.out.println("   Tmall time:"+time+" CheckAmount:"+amount);
            Result<DataEntry> data= tairManager.get(RaceConfig.TairNamespace,tairKey);
            double jstromRes=(double)data.getValue().getValue();
            boolean same=jstromRes==amount?true:false;
            System.out.println(same+"   Tmall time:"+time+" CheckAmount:"+amount+"  jstromRes"+jstromRes);
        }
        for(long time:PcCounter.keySet()){
            double pcAmount=PcCounter.get(time);
            double mobileAmount=PcCounter.get(time);
            //假设每个分钟，既有pc，又有mobile
            Double ratio=mobileAmount/pcAmount;
            String res=String.format("%.2f",ratio);
            System.out.println("   Pay  time:"+time+" Ratio:"+res);
            String tairKey="ratio_"+time;
            Result<DataEntry> data= tairManager.get(RaceConfig.TairNamespace,tairKey);
            double jstromRes=(double)data.getValue().getValue();
            boolean same=jstromRes==ratio?true:false;
            System.out.println(same+"   Pay time:"+time+" CheckRatio:"+res+"  jstromRes"+jstromRes);
        }
        //清空
        clear();
    }
    public static void clear(){
        idTypeMap.clear();
        paymentMessageBQ.clear();
        TaobaoCounter.clear();
        TmallCounter.clear();
        PcCounter.clear();
        mobileCounter.clear();
    }
    public static long getMinite(long time){
        return time-time% RaceTopology.Interval;
    }
    public static void putInMap(HashMap<Long,Double> map,long time,double amount){
        Double alreadyAmout=map.get(time);
        if(alreadyAmout==null)
            map.put(time,amount);
        else
            map.put(time,amount+alreadyAmout);
    }
}

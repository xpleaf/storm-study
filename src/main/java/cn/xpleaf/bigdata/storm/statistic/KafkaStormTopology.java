package cn.xpleaf.bigdata.storm.statistic;

import cn.xpleaf.bigdata.storm.utils.JedisUtil;
import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Kafka和storm的整合，用于统计实时流量对应的pv和uv
 */
public class KafkaStormTopology {

    /**
     * 日志数据提取
     */
    static class MyKafkaBolt extends BaseBasicBolt {

        /**
         * kafkaSpout发送的字段名为bytes
         */
        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            byte[] binary = input.getBinary(0); // 跨jvm传输数据，接收到的是字节数据
            // byte[] bytes = input.getBinaryByField("bytes");   // 这种方式也行
            String line = new String(binary);
            String[] fields = line.split("\t");

            if(fields == null || fields.length < 10) {
                return;
            }
            String userId = fields[0];
            String ip = fields[1];
            String mid = fields[2];
            String request = fields[5];
            String timestamp = fields[9];

            collector.emit(new Values(userId, ip, mid, request, timestamp));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("user_id", "ip", "mid", "request", "timestamp"));
        }
    }

    /**
     * 统计pv & uv
     */
    static class StatisticBolt extends BaseBasicBolt {

        long pv = 0L;
        Set<String> mids = new HashSet<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {

            if(!input.getSourceComponent().equalsIgnoreCase(Constants.SYSTEM_COMPONENT_ID)) {   // 如果这不是系统发送过来的tuple，则执行我们的业务逻辑
                // String ip = input.getStringByField("ip");
                String mid = input.getStringByField("mid");
                pv += 1;
                mids.add(mid);
            } else {    // 如果是系统发送过来的tuple，执行我们的定时任务
                /**
                 * 一般统计的pv和uv都是每一天的数据
                 * 所以要把pv和uv存放在redis要按天来存放
                 * 2018-04-15
                 *  | 1 pv uv
                 *  | 2 pv uv
                 * pv--->string
                 * uv--->set
                 */
                Jedis jedis = JedisUtil.getJedis();
                String prefix = sdf.format(new Date());
                String keyPV = prefix + "_pv";
                if(pv > 0) {
                    String redisPv = jedis.get(keyPV);
                    if (redisPv == null) {
                        redisPv = "0";
                    }
                    pv += Long.valueOf(redisPv);
                    jedis.set(keyPV, pv + "");
                }
                // uv
                if(!mids.isEmpty()) {
                    String keyUV = prefix + "_uv";
                    jedis.sadd(keyUV, mids.toArray(new String[mids.size()]));
                }
                // 结束之后要做数据的释放
                pv = 0;
                mids.clear();
                JedisUtil.returnJedis(jedis);

                System.out.println("系统级别的消息--->");
                System.out.println("此时的pv变量：" + pv);
                System.out.println("此时的mids大小：" + mids.size());
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

        /**
         * Storm局部定时任务，只对当前bolt生效
         */
        @Override
        public Map<String, Object> getComponentConfiguration() {
            Map<String, Object> config = new HashMap<>();
            config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
            return config;
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        /**
         * 设置spout和bolt的dag（有向无环图）
         */
        KafkaSpout kafkaSpout = createKafkaSpout();
        builder.setSpout("id_kafka_spout", kafkaSpout);
        builder.setBolt("id_kafka_bolt", new MyKafkaBolt()).shuffleGrouping("id_kafka_spout"); // 通过不同的数据流转方式，来指定数据的上游组件
        builder.setBolt("id_statistic_bolt", new StatisticBolt()).shuffleGrouping("id_kafka_bolt");
        // 使用builder构建topology
        StormTopology topology = builder.createTopology();
        String topologyName = KafkaStormTopology.class.getSimpleName();  // 拓扑的名称
        Config config = new Config();   // Config()对象继承自HashMap，但本身封装了一些基本的配置

        // 启动topology，本地启动使用LocalCluster，集群启动使用StormSubmitter
        if (args == null || args.length < 1) {  // 没有参数时使用本地模式，有参数时使用集群模式
            LocalCluster localCluster = new LocalCluster(); // 本地开发模式，创建的对象为LocalCluster
            localCluster.submitTopology(topologyName, config, topology);
        } else {
            StormSubmitter.submitTopology(topologyName, config, topology);
        }
    }

    /**
     * BrokerHosts hosts  kafka集群列表
     * String topic       要消费的topic主题
     * String zkRoot      kafka在zk中的目录（会在该节点目录下记录读取kafka消息的偏移量）
     * String id          当前操作的标识id
     */
    private static KafkaSpout createKafkaSpout() {
        String brokerZkStr = "uplooking01:2181,uplooking02:2181,uplooking03:2181";
        BrokerHosts hosts = new ZkHosts(brokerZkStr);   // 通过zookeeper中的/brokers即可找到kafka的地址
        String topic = "f-k-s";
        String zkRoot = "/" + topic;
        String id = "consumer-id";
        SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, id);
        // 本地环境设置之后，也可以在zk中建立/f-k-s节点，在集群环境中，不用配置也可以在zk中建立/f-k-s节点
        //spoutConf.zkServers = Arrays.asList(new String[]{"uplooking01", "uplooking02", "uplooking03"});
        //spoutConf.zkPort = 2181;
        spoutConf.startOffsetTime = OffsetRequest.LatestTime(); // 设置之后，刚启动时就不会把之前的消费也进行读取，会从最新的偏移量开始读取
        return new KafkaSpout(spoutConf);
    }
}

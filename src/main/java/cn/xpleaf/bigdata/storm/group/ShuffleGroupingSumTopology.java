package cn.xpleaf.bigdata.storm.group;

import cn.xpleaf.bigdata.storm.utils.StormUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Date;
import java.util.Map;

/**
 * 1°、实现数字累加求和的案例：数据源不断产生递增数字，对产生的数字累加求和。
 * <p>
 * Storm组件：Spout、Bolt、数据是Tuple，使用main中的Topology将spout和bolt进行关联
 * MapReduce的组件：Mapper和Reducer、数据是Writable，通过一个main中的job将二者关联
 * <p>
 * 适配器模式（Adapter）：BaseRichSpout，其对继承接口中一些没必要的方法进行了重写，但其重写的代码没有实现任何功能。
 * 我们称这为适配器模式
 */
public class ShuffleGroupingSumTopology {

    /**
     * 数据源
     */
    static class OrderSpout extends BaseRichSpout {

        private Map conf;   // 当前组件配置信息
        private TopologyContext context;    // 当前组件上下文对象
        private SpoutOutputCollector collector; // 发送tuple的组件

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        /**
         * 接收数据的核心方法
         */
        @Override
        public void nextTuple() {
            long num = 0;
            while (true) {
                num++;
                StormUtil.sleep(1000);
                System.out.println("当前时间" + StormUtil.df_yyyyMMddHHmmss.format(new Date()) + "产生的订单金额：" + num);
                this.collector.emit(new Values(num));
            }
        }

        /**
         * 是对发送出去的数据的描述schema
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("order_cost"));
        }
    }

    /**
     * 计算和的Bolt节点
     */
    static class SumBolt extends BaseRichBolt {

        private Map conf;   // 当前组件配置信息
        private TopologyContext context;    // 当前组件上下文对象
        private OutputCollector collector; // 发送tuple的组件

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        private Long sumOrderCost = 0L;

        /**
         * 处理数据的核心方法
         */
        @Override
        public void execute(Tuple input) {
            Long orderCost = input.getLongByField("order_cost");
            sumOrderCost += orderCost;

            System.out.println("线程ID：" + Thread.currentThread().getId() + " ,商城网站到目前" + StormUtil.df_yyyyMMddHHmmss.format(new Date()) + "的商品总交易额" + sumOrderCost);
            StormUtil.sleep(1000);
        }

        /**
         * 如果当前bolt为最后一个处理单元，该方法可以不用管
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    /**
     * 构建拓扑，相当于在MapReduce中构建Job
     */
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        /**
         * 设置spout和bolt的dag（有向无环图）
         */
        builder.setSpout("id_order_spout", new OrderSpout());
        builder.setBolt("id_sum_bolt", new SumBolt(), 3)
                .shuffleGrouping("id_order_spout"); // 通过不同的数据流转方式，来指定数据的上游组件
        // 使用builder构建topology
        StormTopology topology = builder.createTopology();
        String topologyName = ShuffleGroupingSumTopology.class.getSimpleName();  // 拓扑的名称
        Config config = new Config();   // Config()对象继承自HashMap，但本身封装了一些基本的配置
        // 启动topology，本地启动使用LocalCluster，集群启动使用StormSubmitter
        if (args == null || args.length < 1) {  // 没有参数时使用本地模式，有参数时使用集群模式
            LocalCluster localCluster = new LocalCluster(); // 本地开发模式，创建的对象为LocalCluster
            localCluster.submitTopology(topologyName, config, topology);
        } else {
            StormSubmitter.submitTopology(topologyName, config, topology);
        }
    }
}

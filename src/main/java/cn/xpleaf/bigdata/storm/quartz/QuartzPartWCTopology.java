package cn.xpleaf.bigdata.storm.quartz;

import clojure.lang.Obj;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
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

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 2°、单词计数：监控一个目录下的文件，当发现有新文件的时候，
        把文件读取过来，解析文件中的内容，统计单词出现的总次数
        E:\data\storm

        研究storm的定时任务
        有两种方式：
            1.main中设置，全局有效
            2.在特定bolt中设置，bolt中有效
 */
public class QuartzPartWCTopology {

    /**
     * Spout，获取数据源，这里是持续读取某一目录下的文件，并将每一行输出到下一个Bolt中
     */
    static class FileSpout extends BaseRichSpout {
        private Map conf;   // 当前组件配置信息
        private TopologyContext context;    // 当前组件上下文对象
        private SpoutOutputCollector collector; // 发送tuple的组件

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            File directory = new File("D:/data/storm");
            // 第二个参数extensions的意思就是，只采集某些后缀名的文件
            Collection<File> files = FileUtils.listFiles(directory, new String[]{"txt"}, true);
            for (File file : files) {
                try {
                    List<String> lines = FileUtils.readLines(file, "utf-8");
                    for(String line : lines) {
                        this.collector.emit(new Values(line));
                    }
                    // 当前文件被消费之后，需要重命名，同时为了防止相同文件的加入，重命名后的文件加了一个随机的UUID，或者加入时间戳也可以的
                    File destFile = new File(file.getAbsolutePath() + "_" + UUID.randomUUID().toString() + ".completed");
                    FileUtils.moveFile(file, destFile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * Bolt节点，将接收到的每一行数据切割为一个个单词并发送到下一个节点
     */
    static class SplitBolt extends BaseRichBolt {

        private Map conf;   // 当前组件配置信息
        private TopologyContext context;    // 当前组件上下文对象
        private OutputCollector collector; // 发送tuple的组件

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            String line = input.getStringByField("line");
            String[] words = line.split(" ");
            for (String word : words) {
                this.collector.emit(new Values(word, 1));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    /**
     * Bolt节点，执行单词统计计算
     */
    static class WCBolt extends BaseRichBolt {

        private Map conf;   // 当前组件配置信息
        private TopologyContext context;    // 当前组件上下文对象
        private OutputCollector collector; // 发送tuple的组件

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        private Map<String, Integer> map = new HashMap<>();

        @Override
        public void execute(Tuple input) {
            if (!input.getSourceComponent().equalsIgnoreCase(Constants.SYSTEM_COMPONENT_ID) ) { // 确保不是系统发送的tuple，才使用我们的业务逻辑
                String word = input.getStringByField("word");
                Integer count = input.getIntegerByField("count");
            /*if (map.containsKey(word)) {
                map.put(word, map.get(word) + 1);
            } else {
                map.put(word, 1);
            }*/
                map.put(word, map.getOrDefault(word, 0) + 1);

                System.out.println("====================================");
                map.forEach((k, v) -> {
                    System.out.println(k + ":::" + v);
                });
            } else {
                System.out.println("sumBolt: " + input.getSourceComponent().toString() + "---" + System.currentTimeMillis());
            }
        }

        @Override
        public Map<String, Object> getComponentConfiguration() { // 修改局部bolt的配置信息
            Map<String, Object> config = new HashMap<>();
            config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
            return config;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    /**
     * 构建拓扑，组装Spout和Bolt节点，相当于在MapReduce中构建Job
     */
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        // dag
        builder.setSpout("id_file_spout", new FileSpout());
        builder.setBolt("id_split_bolt", new SplitBolt()).shuffleGrouping("id_file_spout");
        builder.setBolt("id_wc_bolt", new WCBolt()).shuffleGrouping("id_split_bolt");

        StormTopology stormTopology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        String topologyName = QuartzPartWCTopology.class.getSimpleName();
        Config config = new Config();
        cluster.submitTopology(topologyName, config, stormTopology);
    }
}

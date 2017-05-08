/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalCluster.LocalTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.scheduler.adaptive.TaskMonitor;
import org.apache.storm.scheduler.adaptive.WorkerMonitor;
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
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * This is a basic example of a Storm topology.
 */
public class AdaptiveExclamationTopology {

  public static class TestWordSpoutAdaptive extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(org.apache.storm.testing.TestWordSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    //region monitoring
    private TaskMonitor taskMonitor;
    //endregion monitoring

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
      //region monitoring
      //register this spout instance (task) to the java process monitor
      WorkerMonitor.getInstance().setContextInfo(context);
      //create the object required to notify relevant events (also notify thread ID)
      taskMonitor = new TaskMonitor(context.getThisTaskId());
      //endregion
    }

    public void close() {

    }

    public void nextTuple() {
      Utils.sleep(100);
      final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
      final Random rand = new Random();
      final String word = words[rand.nextInt(words.length)];
      //region monitoring
      taskMonitor.checkThreadId();
      //endregion
      _collector.emit(new Values(word));
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      if (!_isDistributed) {
        Map<String, Object> ret = new HashMap<String, Object>();
        ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 5);
        return ret;
      } else {
        return null;
      }
    }
  }

  public static class ExclamationBolt extends BaseRichBolt {
    OutputCollector _collector;
    //region monitoring
    private TaskMonitor taskMonitor;
    //endregion monitoring

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      //region monitoring
      // register this spout instance (task) to the java process monitor
      WorkerMonitor.getInstance().setContextInfo(context);
      // create the object for notifying relevant events (received tuple, which in turn notifies thread ID)
      taskMonitor = new TaskMonitor(context.getThisTaskId());
      //endregion monitoring
    }

    @Override
    public void execute(Tuple tuple) {
      //region monitoring
      taskMonitor.notifyTupleReceived(tuple);
      //endregion monitoring
      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }


  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("a", new TestWordSpoutAdaptive(), 1);
//    builder.setBolt("b", new ExclamationBolt(), 2).fieldsGrouping("a", new Fields("word"));
    builder.setBolt("b", new ExclamationBolt(), 2).fieldsGrouping("a", new Fields("word"));
    builder.setBolt("c", new ExclamationBolt(), 2).allGrouping("b");

    Config conf = new Config();
    conf.setDebug(true);
    conf.setNumWorkers(2);
    //conf.setTopologyStrategy(org.apache.storm.scheduler.resource.strategies.scheduling.myResourceAwareStrategy.class);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    } else {

      try (LocalCluster cluster = new LocalCluster();
           LocalTopology topo = cluster.submitTopology("adaptive-exclamation-topology", conf, builder.createTopology());) {
        Utils.sleep(100000);
      }
    }
  }
}

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

package org.apache.storm.scheduler.adaptive;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.resource.TestUtilsFormyScheduler;
import org.apache.storm.scheduler.resource.strategies.scheduling.myStrategy;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestAdaptiveOnlineScheduler {

  private final String TOPOLOGY_SUBMITTER = "amir";

  private static final Logger LOG = LoggerFactory.getLogger(TestAdaptiveOnlineScheduler.class);

  private static int currentTime = 1450418597;

  private static final Config defaultTopologyConf = new Config();

  @Test
  public static void testAdaptiveOnlineScheduler() {
    int spoutParallelism = 1;
    int boltParallelism = 2;
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("a", new TestUtilsForAdaptiveOnlineScheduler.TestSpout(),
        spoutParallelism + 1);
    builder.setBolt("b", new TestUtilsForAdaptiveOnlineScheduler.TestBolt(),
        boltParallelism + 1).fieldsGrouping("a", new Fields("word"));
    builder.setBolt("c", new TestUtilsForAdaptiveOnlineScheduler.TestBolt(),
        boltParallelism).shuffleGrouping("b");


    StormTopology stormToplogy = builder.createTopology();

    Config conf = new Config();
    INimbus iNimbus = new TestUtilsForAdaptiveOnlineScheduler.INimbusTest();
    Map<String, Number> resourceMap = new HashMap<String, Number>();

    resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 150.0);
    resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1500.0);
//        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genHeterogeneousSupervisors(4, 4, resourceMap);
    Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genHeterogeneousSupervisors(4, 4);
    conf.putAll(Utils.readDefaultConfig());
    conf.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
    conf.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
    conf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, myStrategy.class.getName());
    conf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 50.0);
    conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 250);
    conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 250);
    conf.put(Config.TOPOLOGY_PRIORITY, 0);
    conf.put(Config.TOPOLOGY_NAME, "testTopology");
    conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);

    TopologyDetails topo = new TopologyDetails("testTopology-id", conf, stormToplogy, 0,
        TestUtilsForAdaptiveOnlineScheduler.genExecsAndComps(stormToplogy)
        , currentTime);

    Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
    topoMap.put(topo.getId(), topo);
    Topologies topologies = new Topologies(topoMap);
    Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), conf);

    OnlineScheduler rs = new OnlineScheduler();

    rs.prepare(conf);
    rs.schedule(topologies, cluster);

    Map<String, List<String>> nodeToComps = new HashMap<String, List<String>>();
    for (Map.Entry<ExecutorDetails, WorkerSlot> entry : cluster.getAssignments().get("testTopology-id").getExecutorToSlot().entrySet()) {
      WorkerSlot ws = entry.getValue();
      ExecutorDetails exec = entry.getKey();
      if (!nodeToComps.containsKey(ws.getNodeId())) {
        nodeToComps.put(ws.getNodeId(), new LinkedList<String>());
      }
      nodeToComps.get(ws.getNodeId()).add(topo.getExecutorToComponent().get(exec));
    }
  }
}

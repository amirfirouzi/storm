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

package org.apache.storm.scheduler.resource;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestMyScheduler {

  private final String TOPOLOGY_SUBMITTER = "jerry";

  private static final Logger LOG = LoggerFactory.getLogger(TestMyScheduler.class);

  private static int currentTime = 1450418597;

  private static final Config defaultTopologyConf = new Config();

  @BeforeClass
  public static void initConf() {
    defaultTopologyConf.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
    defaultTopologyConf.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
    defaultTopologyConf.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());

    defaultTopologyConf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.myStrategy.class.getName());
    defaultTopologyConf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10.0);
    defaultTopologyConf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 128.0);
    defaultTopologyConf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);
    defaultTopologyConf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8192.0);
    defaultTopologyConf.put(Config.TOPOLOGY_PRIORITY, 0);
    defaultTopologyConf.put(Config.TOPOLOGY_SUBMITTER_USER, "amir");
  }

  @Test
  public void testHeterogeneousCluster() {
    INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
    Map<String, Number> resourceMap1 = new HashMap<>(); // strong supervisor node
    resourceMap1.put(Config.SUPERVISOR_CPU_CAPACITY, 900.0);
    resourceMap1.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 4096.0);
    Map<String, Number> resourceMap2 = new HashMap<>(); // weak supervisor node
    resourceMap2.put(Config.SUPERVISOR_CPU_CAPACITY, 500.0);
    resourceMap2.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);

    Map<String, SupervisorDetails> supMap = new HashMap<String, SupervisorDetails>();
    for (int i = 0; i < 2; i++) {
      List<Number> ports = new LinkedList<Number>();
      for (int j = 0; j < 4; j++) {
        ports.add(j);
      }
      SupervisorDetails sup = new SupervisorDetails("sup-" + i, "host-" + i, null, ports, (Map) (i == 0 ? resourceMap1 : resourceMap2));
      supMap.put(sup.getId(), sup);
    }

    // topo1 has one single huge task that can not be handled by the small-super
    TopologyBuilder builder1 = new TopologyBuilder();
    builder1.setSpout("a", new TestWordSpout(), 1)
        .setCPULoad(150)
        .setMemoryLoad(1024);
    builder1.setBolt("b", new TestWordCounter(), 2).fieldsGrouping("a", new Fields("word"))
        .setCPULoad(200)
        .setMemoryLoad(768);
    builder1.setBolt("c", new TestWordCounter(), 2).allGrouping("b")
        .setCPULoad(300)
        .setMemoryLoad(256);

    StormTopology stormTopology1 = builder1.createTopology();

    Config config1 = new Config();
    config1.putAll(defaultTopologyConf);
    Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
    TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 1, executorMap1, 0);
//
//        // topo2 has 4 large tasks
//        TopologyBuilder builder2 = new TopologyBuilder();
//        builder2.setSpout("wordSpout2", new TestWordSpout(), 4).setCPULoad(100.0).setMemoryLoad(500.0, 12.0);
//        StormTopology stormTopology2 = builder2.createTopology();
//        Config config2 = new Config();
//        config2.putAll(defaultTopologyConf);
//        Map<ExecutorDetails, String> executorMap2 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology2);
//        TopologyDetails topology2 = new TopologyDetails("topology2", config2, stormTopology2, 1, executorMap2, 0);
//
//        // topo3 has 4 large tasks
//        TopologyBuilder builder3 = new TopologyBuilder();
//        builder3.setSpout("wordSpout3", new TestWordSpout(), 4).setCPULoad(20.0).setMemoryLoad(200.0, 56.0);
//        StormTopology stormTopology3 = builder3.createTopology();
//        Config config3 = new Config();
//        config3.putAll(defaultTopologyConf);
//        Map<ExecutorDetails, String> executorMap3 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology3);
//        TopologyDetails topology3 = new TopologyDetails("topology3", config2, stormTopology3, 1, executorMap3, 0);
//
//        // topo4 has 12 small tasks, whose mem usage does not exactly divide a node's mem capacity
//        TopologyBuilder builder4 = new TopologyBuilder();
//        builder4.setSpout("wordSpout4", new TestWordSpout(), 12).setCPULoad(30.0).setMemoryLoad(100.0, 0.0);
//        StormTopology stormTopology4 = builder4.createTopology();
//        Config config4 = new Config();
//        config4.putAll(defaultTopologyConf);
//        Map<ExecutorDetails, String> executorMap4 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology4);
//        TopologyDetails topology4 = new TopologyDetails("topology4", config4, stormTopology4, 1, executorMap4, 0);
//
//        // topo5 has 40 small tasks, it should be able to exactly use up both the cpu and mem in the cluster
//        TopologyBuilder builder5 = new TopologyBuilder();
//        builder5.setSpout("wordSpout5", new TestWordSpout(), 40).setCPULoad(25.0).setMemoryLoad(100.0, 28.0);
//        StormTopology stormTopology5 = builder5.createTopology();
//        Config config5 = new Config();
//        config5.putAll(defaultTopologyConf);
//        Map<ExecutorDetails, String> executorMap5 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology5);
//        TopologyDetails topology5 = new TopologyDetails("topology5", config5, stormTopology5, 1, executorMap5, 0);

    // Test1: Launch topo 1-3 together, it should be able to use up either mem or cpu resource due to exact division
    Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config1);
    myScheduler rs = new myScheduler();
    Map<String, TopologyDetails> topoMap = new HashMap<>();
    topoMap.put(topology1.getId(), topology1);
//        topoMap.put(topology2.getId(), topology2);
//        topoMap.put(topology3.getId(), topology3);
    Topologies topologies = new Topologies(topoMap);
    rs.prepare(config1);
    rs.schedule(topologies, cluster);

    Assert.assertEquals("Running - Fully Scheduled by myStrategy", cluster.getStatusMap().get(topology1.getId()));
//        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology2.getId()));
//        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology3.getId()));

    Map<SupervisorDetails, Double> superToCpu = TestUtilsFormyScheduler.getSupervisorToCpuUsage(cluster, topologies);
    Map<SupervisorDetails, Double> superToMem = TestUtilsFormyScheduler.getSupervisorToMemoryUsage(cluster, topologies);

    SchedulerAssignment assignment1 = cluster.getAssignmentById(topology1.getId());
    Set<WorkerSlot> assignedSlots1 = assignment1.getSlots();

    for (WorkerSlot slot : assignedSlots1) {
      System.out.println(slot);
    }
    // end of Test1

  }
}

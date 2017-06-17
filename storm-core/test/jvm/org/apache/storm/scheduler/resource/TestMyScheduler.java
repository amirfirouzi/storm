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
import org.apache.storm.graph.Graph;
import org.apache.storm.graph.TopologyGraphBuilder;
import org.apache.storm.graph.partitioner.CostFunction;
import org.apache.storm.graph.partitioner.Partitioner;
import org.apache.storm.graph.partitioner.PartitioningResult;
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

import static org.apache.storm.scheduler.resource.monitoring.Utils.RESCHEDULE_TIMEOUT;

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
        defaultTopologyConf.put(RESCHEDULE_TIMEOUT, 10);

        defaultTopologyConf.put("jdbc.driver", "com.mysql.jdbc.Driver");
        defaultTopologyConf.put("data.connection.uri", "jdbc:mysql://localhost/stormscheduler?user=stormuser&password=123456");
        defaultTopologyConf.put("validation.query", "SELECT");
        defaultTopologyConf.put("node-name", "amir-pc");

    }

    @Test
    public void testHeterogeneousCluster() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap1 = new HashMap<>(); // strong supervisor node
        resourceMap1.put(Config.SUPERVISOR_CPU_CAPACITY, 900.0);
        resourceMap1.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 4096.0);
        Map<String, Number> resourceMap2 = new HashMap<>(); // weak supervisor node
        resourceMap2.put(Config.SUPERVISOR_CPU_CAPACITY, 700.0);
        resourceMap2.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);

        Map<String, SupervisorDetails> supMap = new HashMap<String, SupervisorDetails>();
        for (int i = 0; i < 2; i++) {
            int numberOfPorts = 4;
            List<Number> ports = new LinkedList<Number>();
            for (int j = 0; j < numberOfPorts - i; j++) {
                ports.add(j);
            }
            SupervisorDetails sup = new SupervisorDetails("sup-" + i, "host-" + i, null, ports, (Map) (i == 0 ? resourceMap1 : resourceMap2));
            supMap.put(sup.getId(), sup);
        }

        // topo1 has one single huge task that can not be handled by the small-super
        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("a", new TestWordSpout(), 1)
                .setCPULoad(150)
                .setMemoryLoad(1000);
        builder1.setBolt("b", new TestWordCounter(), 2).fieldsGrouping("a", new Fields("word"))
                .setCPULoad(200)
                .setMemoryLoad(500);
        builder1.setBolt("c", new TestWordCounter(), 2).allGrouping("b")
                .setCPULoad(300)
                .setMemoryLoad(600);


        StormTopology stormTopology1 = builder1.createTopology();

        Config config1 = new Config();
        config1.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 1, executorMap1, 0);

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


        //region prepare for reScheduling
        Graph graph = TopologyGraphBuilder.buildGraph(topology1);

        graph.getEdgeFromExecutor(generateExecName(0), generateExecName(1)).setWeight(100);
        graph.getEdgeFromExecutor(generateExecName(0), generateExecName(2)).setWeight(100);
        graph.getEdgeFromExecutor(generateExecName(1), generateExecName(3)).setWeight(10);
        graph.getEdgeFromExecutor(generateExecName(1), generateExecName(4)).setWeight(10);
        graph.getEdgeFromExecutor(generateExecName(2), generateExecName(3)).setWeight(10);
        graph.getEdgeFromExecutor(generateExecName(2), generateExecName(4)).setWeight(10);
        //TODO: update vertex weights from monitoring


        SchedulingState schedulingState = rs.getSchedulingState();
        PartitioningResult partitioning = Partitioner.doPartition(CostFunction.costMode.Both, true, graph, schedulingState);
        boolean isPartitioningImproved = rs.isPartitioningImproved(topology1.getId(), partitioning);
        rs.scheduleTopology(topology1, partitioning, isPartitioningImproved);
        //end region


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

    private String generateExecName(int number) {
        return "[" + number + ", " + number + "]";
    }
}

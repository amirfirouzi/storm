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
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.*;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.apache.storm.validation.ConfigValidation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.storm.scheduler.resource.TestUtilsForResourceAwareScheduler.genExecsAndComps;

public class TestmyResourceAwareScheduler {

    private final String TOPOLOGY_SUBMITTER = "jerry";

    private static final Logger LOG = LoggerFactory.getLogger(TestmyResourceAwareScheduler.class);

    private static int currentTime = 1450418597;

    private static final Config defaultTopologyConf = new Config();

    @BeforeClass
    public static void initConf() {
        defaultTopologyConf.put(Config.STORM_NETWORK_TOPOGRAPHY_PLUGIN, "org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping");
        defaultTopologyConf.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        defaultTopologyConf.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());

        defaultTopologyConf.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        defaultTopologyConf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10.0);
        defaultTopologyConf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 128.0);
        defaultTopologyConf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 0.0);
        defaultTopologyConf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 8192.0);
        defaultTopologyConf.put(Config.TOPOLOGY_PRIORITY, 0);
        defaultTopologyConf.put(Config.TOPOLOGY_SUBMITTER_USER, "zhuo");
    }

    @Test
    public void testRASNodeSlotAssign() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(5, 4, resourceMap);
        Topologies topologies = new Topologies(new HashMap<String, TopologyDetails>());
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), new HashMap());
        Map<String, RAS_Node> nodes = RAS_Nodes.getAllNodesFrom(cluster, topologies);
        Assert.assertEquals(5, nodes.size());
        RAS_Node node = nodes.get("sup-0");

        Assert.assertEquals("sup-0", node.getId());
        Assert.assertTrue(node.isAlive());
        Assert.assertEquals(0, node.getRunningTopologies().size());
        Assert.assertTrue(node.isTotallyFree());
        Assert.assertEquals(4, node.totalSlotsFree());
        Assert.assertEquals(0, node.totalSlotsUsed());
        Assert.assertEquals(4, node.totalSlots());

        TopologyDetails topology1 = TestUtilsFormyScheduler.getTopology("topology1", new HashMap(), 1, 0, 2, 0, 0, 0);

        List<ExecutorDetails> executors11 = new ArrayList<>();
        executors11.add(new ExecutorDetails(1, 1));
        node.assign(node.getFreeSlots().iterator().next(), topology1, executors11);
        Assert.assertEquals(1, node.getRunningTopologies().size());
        Assert.assertFalse(node.isTotallyFree());
        Assert.assertEquals(3, node.totalSlotsFree());
        Assert.assertEquals(1, node.totalSlotsUsed());
        Assert.assertEquals(4, node.totalSlots());

        List<ExecutorDetails> executors12 = new ArrayList<>();
        executors12.add(new ExecutorDetails(2, 2));
        node.assign(node.getFreeSlots().iterator().next(), topology1, executors12);
        Assert.assertEquals(1, node.getRunningTopologies().size());
        Assert.assertFalse(node.isTotallyFree());
        Assert.assertEquals(2, node.totalSlotsFree());
        Assert.assertEquals(2, node.totalSlotsUsed());
        Assert.assertEquals(4, node.totalSlots());

        TopologyDetails topology2 = TestUtilsFormyScheduler.getTopology("topology2", new HashMap(), 1, 0, 2, 0, 0, 0);

        List<ExecutorDetails> executors21 = new ArrayList<>();
        executors21.add(new ExecutorDetails(1, 1));
        node.assign(node.getFreeSlots().iterator().next(), topology2, executors21);
        Assert.assertEquals(2, node.getRunningTopologies().size());
        Assert.assertFalse(node.isTotallyFree());
        Assert.assertEquals(1, node.totalSlotsFree());
        Assert.assertEquals(3, node.totalSlotsUsed());
        Assert.assertEquals(4, node.totalSlots());

        List<ExecutorDetails> executors22 = new ArrayList<>();
        executors22.add(new ExecutorDetails(2, 2));
        node.assign(node.getFreeSlots().iterator().next(), topology2, executors22);
        Assert.assertEquals(2, node.getRunningTopologies().size());
        Assert.assertFalse(node.isTotallyFree());
        Assert.assertEquals(0, node.totalSlotsFree());
        Assert.assertEquals(4, node.totalSlotsUsed());
        Assert.assertEquals(4, node.totalSlots());

        node.freeAllSlots();
        Assert.assertEquals(0, node.getRunningTopologies().size());
        Assert.assertTrue(node.isTotallyFree());
        Assert.assertEquals(4, node.totalSlotsFree());
        Assert.assertEquals(0, node.totalSlotsUsed());
        Assert.assertEquals(4, node.totalSlots());
    }

    @Test
    public void sanityTestOfScheduling() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(1, 2, resourceMap);

        Config config = new Config();
        config.putAll(defaultTopologyConf);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        TopologyDetails topology1 = TestUtilsFormyScheduler.getTopology("topology1", config, 1, 1, 1, 1, 0, 0);
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        Topologies topologies = new Topologies(topoMap);

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        SchedulerAssignment assignment = cluster.getAssignmentById(topology1.getId());
        Set<WorkerSlot> assignedSlots = assignment.getSlots();
        Set<String> nodesIDs = new HashSet<>();
        for (WorkerSlot slot : assignedSlots) {
            nodesIDs.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors = assignment.getExecutors();

        Assert.assertEquals(1, assignedSlots.size());
        Assert.assertEquals(1, nodesIDs.size());
        Assert.assertEquals(2, executors.size());
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology1.getId()));
    }

    @Test
    public void testTopologyWithMultipleSpouts() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(2, 4, resourceMap);

        TopologyBuilder builder1 = new TopologyBuilder(); // a topology with multiple spouts
        builder1.setSpout("wordSpout1", new TestWordSpout(), 1);
        builder1.setSpout("wordSpout2", new TestWordSpout(), 1);
        builder1.setBolt("wordCountBolt1", new TestWordCounter(), 1).shuffleGrouping("wordSpout1").shuffleGrouping("wordSpout2");
        builder1.setBolt("wordCountBolt2", new TestWordCounter(), 1).shuffleGrouping("wordCountBolt1");
        builder1.setBolt("wordCountBolt3", new TestWordCounter(), 1).shuffleGrouping("wordCountBolt1");
        builder1.setBolt("wordCountBolt4", new TestWordCounter(), 1).shuffleGrouping("wordCountBolt2");
        builder1.setBolt("wordCountBolt5", new TestWordCounter(), 1).shuffleGrouping("wordSpout2");
        StormTopology stormTopology1 = builder1.createTopology();

        Config config = new Config();
        config.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config, stormTopology1, 0, executorMap1, 0);

        TopologyBuilder builder2 = new TopologyBuilder(); // a topology with two unconnected partitions
        builder2.setSpout("wordSpoutX", new TestWordSpout(), 1);
        builder2.setSpout("wordSpoutY", new TestWordSpout(), 1);
        StormTopology stormTopology2 = builder2.createTopology();
        Map<ExecutorDetails, String> executorMap2 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config, stormTopology2, 0, executorMap2, 0);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        topoMap.put(topology2.getId(), topology2);
        Topologies topologies = new Topologies(topoMap);

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        SchedulerAssignment assignment1 = cluster.getAssignmentById(topology1.getId());
        Set<WorkerSlot> assignedSlots1 = assignment1.getSlots();
        Set<String> nodesIDs1 = new HashSet<>();
        for (WorkerSlot slot : assignedSlots1) {
            nodesIDs1.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors1 = assignment1.getExecutors();

        Assert.assertEquals(1, assignedSlots1.size());
        Assert.assertEquals(1, nodesIDs1.size());
        Assert.assertEquals(7, executors1.size());
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology1.getId()));

        SchedulerAssignment assignment2 = cluster.getAssignmentById(topology2.getId());
        Set<WorkerSlot> assignedSlots2 = assignment2.getSlots();
        Set<String> nodesIDs2 = new HashSet<>();
        for (WorkerSlot slot : assignedSlots2) {
            nodesIDs2.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors2 = assignment2.getExecutors();

        Assert.assertEquals(1, assignedSlots2.size());
        Assert.assertEquals(1, nodesIDs2.size());
        Assert.assertEquals(2, executors2.size());
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology2.getId()));
    }

    @Test
    public void testTopologySetCpuAndMemLoad() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(2, 2, resourceMap);

        TopologyBuilder builder1 = new TopologyBuilder(); // a topology with multiple spouts
        builder1.setSpout("wordSpout", new TestWordSpout(), 1).setCPULoad(20.0).setMemoryLoad(200.0);
        builder1.setBolt("wordCountBolt", new TestWordCounter(), 1).shuffleGrouping("wordSpout").setCPULoad(20.0).setMemoryLoad(200.0);
        StormTopology stormTopology1 = builder1.createTopology();

        Config config = new Config();
        config.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config, stormTopology1, 0, executorMap1, 0);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        Topologies topologies = new Topologies(topoMap);

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        SchedulerAssignment assignment1 = cluster.getAssignmentById(topology1.getId());
        Set<WorkerSlot> assignedSlots1 = assignment1.getSlots();
        double assignedMemory = 0.0;
        double assignedCpu = 0.0;
        Set<String> nodesIDs1 = new HashSet<>();
        for (WorkerSlot slot : assignedSlots1) {
            nodesIDs1.add(slot.getNodeId());
            assignedMemory += slot.getAllocatedMemOnHeap() + slot.getAllocatedMemOffHeap();
            assignedCpu += slot.getAllocatedCpu();

        }
        Collection<ExecutorDetails> executors1 = assignment1.getExecutors();

        Assert.assertEquals(1, assignedSlots1.size());
        Assert.assertEquals(1, nodesIDs1.size());
        Assert.assertEquals(2, executors1.size());
        Assert.assertEquals(400.0, assignedMemory, 0.001);
        Assert.assertEquals(40.0, assignedCpu, 0.001);
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology1.getId()));
    }

    @Test
    public void testResourceLimitation() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(2, 2, resourceMap);

        TopologyBuilder builder1 = new TopologyBuilder(); // a topology with multiple spouts
        builder1.setSpout("wordSpout", new TestWordSpout(), 2).setCPULoad(250.0).setMemoryLoad(1000.0, 200.0);
        builder1.setBolt("wordCountBolt", new TestWordCounter(), 1).shuffleGrouping("wordSpout").setCPULoad(100.0).setMemoryLoad(500.0, 100.0);
        StormTopology stormTopology1 = builder1.createTopology();

        Config config = new Config();
        config.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config, stormTopology1, 2, executorMap1, 0);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        Topologies topologies = new Topologies(topoMap);

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        SchedulerAssignment assignment1 = cluster.getAssignmentById(topology1.getId());
        Set<WorkerSlot> assignedSlots1 = assignment1.getSlots();
        Set<String> nodesIDs1 = new HashSet<>();
        for (WorkerSlot slot : assignedSlots1) {
            nodesIDs1.add(slot.getNodeId());
        }
        Collection<ExecutorDetails> executors1 = assignment1.getExecutors();
        List<Double> assignedExecutorMemory = new ArrayList<>();
        List<Double> assignedExecutorCpu = new ArrayList<>();
        for (ExecutorDetails executor : executors1) {
            assignedExecutorMemory.add(topology1.getTotalMemReqTask(executor));
            assignedExecutorCpu.add(topology1.getTotalCpuReqTask(executor));
        }
        Collections.sort(assignedExecutorCpu);
        Collections.sort(assignedExecutorMemory);

        Map<ExecutorDetails, SupervisorDetails> executorToSupervisor = new HashMap<>();
        Map<SupervisorDetails, List<ExecutorDetails>> supervisorToExecutors = new HashMap<>();
        Map<Double, Double> cpuAvailableToUsed = new HashMap();
        Map<Double, Double> memoryAvailableToUsed = new HashMap();

        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : assignment1.getExecutorToSlot().entrySet()) {
            executorToSupervisor.put(entry.getKey(), cluster.getSupervisorById(entry.getValue().getNodeId()));
        }
        for (Map.Entry<ExecutorDetails, SupervisorDetails> entry : executorToSupervisor.entrySet()) {
            List<ExecutorDetails> executorsOnSupervisor = supervisorToExecutors.get(entry.getValue());
            if (executorsOnSupervisor == null) {
                executorsOnSupervisor = new ArrayList<>();
                supervisorToExecutors.put(entry.getValue(), executorsOnSupervisor);
            }
            executorsOnSupervisor.add(entry.getKey());
        }
        for (Map.Entry<SupervisorDetails, List<ExecutorDetails>> entry : supervisorToExecutors.entrySet()) {
            Double supervisorTotalCpu = entry.getKey().getTotalCPU();
            Double supervisorTotalMemory = entry.getKey().getTotalMemory();
            Double supervisorUsedCpu = 0.0;
            Double supervisorUsedMemory = 0.0;
            for (ExecutorDetails executor: entry.getValue()) {
                supervisorUsedMemory += topology1.getTotalCpuReqTask(executor);
                supervisorTotalCpu += topology1.getTotalMemReqTask(executor);
            }
            cpuAvailableToUsed.put(supervisorTotalCpu, supervisorUsedCpu);
            memoryAvailableToUsed.put(supervisorTotalMemory, supervisorUsedMemory);
        }
        // executor0 resides one one worker (on one), executor1 and executor2 on another worker (on the other node)
        Assert.assertEquals(2, assignedSlots1.size());
        Assert.assertEquals(2, nodesIDs1.size());
        Assert.assertEquals(3, executors1.size());

        Assert.assertEquals(100.0, assignedExecutorCpu.get(0), 0.001);
        Assert.assertEquals(250.0, assignedExecutorCpu.get(1), 0.001);
        Assert.assertEquals(250.0, assignedExecutorCpu.get(2), 0.001);
        Assert.assertEquals(600.0, assignedExecutorMemory.get(0), 0.001);
        Assert.assertEquals(1200.0, assignedExecutorMemory.get(1), 0.001);
        Assert.assertEquals(1200.0, assignedExecutorMemory.get(2), 0.001);

        for (Map.Entry<Double, Double> entry : memoryAvailableToUsed.entrySet()) {
            Assert.assertTrue(entry.getKey()- entry.getValue() >= 0);
        }
        for (Map.Entry<Double, Double> entry : cpuAvailableToUsed.entrySet()) {
            Assert.assertTrue(entry.getKey()- entry.getValue() >= 0);
        }
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology1.getId()));
    }

    @Test
    public void testScheduleResilience() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(2, 2, resourceMap);

        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 3);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 3, executorMap1, 0);

        TopologyBuilder builder2 = new TopologyBuilder();
        builder2.setSpout("wordSpout2", new TestWordSpout(), 2);
        StormTopology stormTopology2 = builder2.createTopology();
        Config config2 = new Config();
        config2.putAll(defaultTopologyConf);
        // memory requirement is large enough so that two executors can not be fully assigned to one node
        config2.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 1280.0);
        Map<ExecutorDetails, String> executorMap2 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config2, stormTopology2, 2, executorMap2, 0);

        // Test1: When a worker fails, RAS does not alter existing assignments on healthy workers
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config1);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topology2.getId(), topology2);
        Topologies topologies = new Topologies(topoMap);

        rs.prepare(config1);
        rs.schedule(topologies, cluster);

        SchedulerAssignmentImpl assignment = (SchedulerAssignmentImpl)cluster.getAssignmentById(topology2.getId());
        // pick a worker to mock as failed
        WorkerSlot failedWorker = new ArrayList<WorkerSlot>(assignment.getSlots()).get(0);
        Map<ExecutorDetails, WorkerSlot> executorToSlot = assignment.getExecutorToSlot();
        List<ExecutorDetails> failedExecutors = new ArrayList<>();
        for (Map.Entry<ExecutorDetails, WorkerSlot> entry : executorToSlot.entrySet()) {
            if (entry.getValue().equals(failedWorker)) {
                failedExecutors.add(entry.getKey());
            }
        }
        for (ExecutorDetails executor : failedExecutors) {
            executorToSlot.remove(executor); // remove executor details assigned to the failed worker
        }
        Map<ExecutorDetails, WorkerSlot> copyOfOldMapping = new HashMap<>(executorToSlot);
        Set<ExecutorDetails> healthyExecutors = copyOfOldMapping.keySet();

        rs.schedule(topologies, cluster);
        SchedulerAssignment newAssignment = cluster.getAssignmentById(topology2.getId());
        Map<ExecutorDetails, WorkerSlot> newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : healthyExecutors) {
            Assert.assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology2.getId()));
        // end of Test1

        // Test2: When a supervisor fails, RAS does not alter existing assignments
        executorToSlot = new HashMap<>();
        executorToSlot.put(new ExecutorDetails(0, 0), new WorkerSlot("sup-0", 0));
        executorToSlot.put(new ExecutorDetails(1, 1), new WorkerSlot("sup-0", 1));
        executorToSlot.put(new ExecutorDetails(2, 2), new WorkerSlot("sup-1", 1));
        Map<String, SchedulerAssignmentImpl> existingAssignments = new HashMap<>();
        assignment = new SchedulerAssignmentImpl(topology1.getId(), executorToSlot);
        existingAssignments.put(topology1.getId(), assignment);
        copyOfOldMapping = new HashMap<>(executorToSlot);
        Set<ExecutorDetails> existingExecutors = copyOfOldMapping.keySet();
        Map<String, SupervisorDetails> supMap1 = new HashMap<>(supMap);
        supMap1.remove("sup-0"); // mock the supervisor sup-0 as a failed supervisor
        Cluster cluster1 = new Cluster(iNimbus, supMap1, existingAssignments, config1);

        topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster1);

        newAssignment = cluster1.getAssignmentById(topology1.getId());
        newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : existingExecutors) {
            Assert.assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        Assert.assertEquals("Fully Scheduled", cluster1.getStatusMap().get(topology1.getId()));
        // end of Test2

        // Test3: When a supervisor and a worker on it fails, RAS does not alter existing assignments
        executorToSlot = new HashMap<>();
        executorToSlot.put(new ExecutorDetails(0, 0), new WorkerSlot("sup-0", 1)); // the worker to orphan
        executorToSlot.put(new ExecutorDetails(1, 1), new WorkerSlot("sup-0", 2)); // the worker that fails
        executorToSlot.put(new ExecutorDetails(2, 2), new WorkerSlot("sup-1", 1)); // the healthy worker
        existingAssignments = new HashMap<>();
        assignment = new SchedulerAssignmentImpl(topology1.getId(), executorToSlot);
        existingAssignments.put(topology1.getId(), assignment);
        // delete one worker of sup-0 (failed) from topo1 assignment to enable actual schedule for testing
        executorToSlot.remove(new ExecutorDetails(1, 1));

        copyOfOldMapping = new HashMap<>(executorToSlot);
        existingExecutors = copyOfOldMapping.keySet(); // namely the two eds on the orphaned worker and the healthy worker
        supMap1 = new HashMap<>(supMap);
        supMap1.remove("sup-0"); // mock the supervisor sup-0 as a failed supervisor
        cluster1 = new Cluster(iNimbus, supMap1, existingAssignments, config1);

        topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster1);

        newAssignment = cluster1.getAssignmentById(topology1.getId());
        newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : existingExecutors) {
            Assert.assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        Assert.assertEquals("Fully Scheduled", cluster1.getStatusMap().get(topology1.getId()));
        // end of Test3

        // Test4: Scheduling a new topology does not disturb other assignments unnecessarily
        cluster1 = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config1);
        topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster1);
        assignment = (SchedulerAssignmentImpl)cluster1.getAssignmentById(topology1.getId());
        executorToSlot = assignment.getExecutorToSlot();
        copyOfOldMapping = new HashMap<>(executorToSlot);

        topoMap.put(topology2.getId(), topology2);
        topologies = new Topologies(topoMap);
        rs.schedule(topologies, cluster1);

        newAssignment = (SchedulerAssignmentImpl)cluster1.getAssignmentById(topology1.getId());
        newExecutorToSlot = newAssignment.getExecutorToSlot();

        for (ExecutorDetails executor : copyOfOldMapping.keySet()) {
            Assert.assertEquals(copyOfOldMapping.get(executor), newExecutorToSlot.get(executor));
        }
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster1.getStatusMap().get(topology1.getId()));
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster1.getStatusMap().get(topology2.getId()));
    }

    @Test
    public void testHeterogeneousCluster() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap1 = new HashMap<>(); // strong supervisor node
        resourceMap1.put(Config.SUPERVISOR_CPU_CAPACITY, 800.0);
        resourceMap1.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 4096.0);
        Map<String, Number> resourceMap2 = new HashMap<>(); // weak supervisor node
        resourceMap2.put(Config.SUPERVISOR_CPU_CAPACITY, 200.0);
        resourceMap2.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0);

        Map<String, SupervisorDetails> supMap = new HashMap<String, SupervisorDetails>();
        for (int i = 0; i < 2; i++) {
            List<Number> ports = new LinkedList<Number>();
            for (int j = 0; j < 4; j++) {
                ports.add(j);
            }
            SupervisorDetails sup = new SupervisorDetails("sup-" + i, "host-" + i, null, ports, (Map)(i == 0 ? resourceMap1 : resourceMap2));
            supMap.put(sup.getId(), sup);
        }

        // topo1 has one single huge task that can not be handled by the small-super
        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 1).setCPULoad(300.0).setMemoryLoad(2000.0, 48.0);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 1, executorMap1, 0);

        // topo2 has 4 large tasks
        TopologyBuilder builder2 = new TopologyBuilder();
        builder2.setSpout("wordSpout2", new TestWordSpout(), 4).setCPULoad(100.0).setMemoryLoad(500.0, 12.0);
        StormTopology stormTopology2 = builder2.createTopology();
        Config config2 = new Config();
        config2.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap2 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config2, stormTopology2, 1, executorMap2, 0);

        // topo3 has 4 large tasks
        TopologyBuilder builder3 = new TopologyBuilder();
        builder3.setSpout("wordSpout3", new TestWordSpout(), 4).setCPULoad(20.0).setMemoryLoad(200.0, 56.0);
        StormTopology stormTopology3 = builder3.createTopology();
        Config config3 = new Config();
        config3.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap3 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology3);
        TopologyDetails topology3 = new TopologyDetails("topology3", config2, stormTopology3, 1, executorMap3, 0);

        // topo4 has 12 small tasks, whose mem usage does not exactly divide a node's mem capacity
        TopologyBuilder builder4 = new TopologyBuilder();
        builder4.setSpout("wordSpout4", new TestWordSpout(), 12).setCPULoad(30.0).setMemoryLoad(100.0, 0.0);
        StormTopology stormTopology4 = builder4.createTopology();
        Config config4 = new Config();
        config4.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap4 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology4);
        TopologyDetails topology4 = new TopologyDetails("topology4", config4, stormTopology4, 1, executorMap4, 0);

        // topo5 has 40 small tasks, it should be able to exactly use up both the cpu and mem in the cluster
        TopologyBuilder builder5 = new TopologyBuilder();
        builder5.setSpout("wordSpout5", new TestWordSpout(), 40).setCPULoad(25.0).setMemoryLoad(100.0, 28.0);
        StormTopology stormTopology5 = builder5.createTopology();
        Config config5 = new Config();
        config5.putAll(defaultTopologyConf);
        Map<ExecutorDetails, String> executorMap5 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology5);
        TopologyDetails topology5 = new TopologyDetails("topology5", config5, stormTopology5, 1, executorMap5, 0);

        // Test1: Launch topo 1-3 together, it should be able to use up either mem or cpu resource due to exact division
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config1);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        topoMap.put(topology2.getId(), topology2);
        topoMap.put(topology3.getId(), topology3);
        Topologies topologies = new Topologies(topoMap);
        rs.prepare(config1);
        rs.schedule(topologies, cluster);

        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology1.getId()));
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology2.getId()));
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology3.getId()));

        Map<SupervisorDetails, Double> superToCpu = TestUtilsFormyScheduler.getSupervisorToCpuUsage(cluster, topologies);
        Map<SupervisorDetails, Double> superToMem = TestUtilsFormyScheduler.getSupervisorToMemoryUsage(cluster, topologies);

        final Double EPSILON = 0.0001;
        for (SupervisorDetails supervisor : supMap.values()) {
            Double cpuAvailable = supervisor.getTotalCPU();
            Double memAvailable = supervisor.getTotalMemory();
            Double cpuUsed = superToCpu.get(supervisor);
            Double memUsed = superToMem.get(supervisor);
            Assert.assertTrue((Math.abs(memAvailable - memUsed) < EPSILON) || (Math.abs(cpuAvailable - cpuUsed) < EPSILON));
        }
        // end of Test1

        // Test2: Launch topo 1, 2 and 4, they together request a little more mem than available, so one of the 3 topos will not be scheduled
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config1);
        topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        topoMap.put(topology2.getId(), topology2);
        topoMap.put(topology4.getId(), topology4);
        topologies = new Topologies(topoMap);
        rs.prepare(config1);
        rs.schedule(topologies, cluster);
        int numTopologiesAssigned = 0;
        if (cluster.getStatusMap().get(topology1.getId()).equals("Running - Fully Scheduled by DefaultResourceAwareStrategy")) {
            numTopologiesAssigned++;
        }
        if (cluster.getStatusMap().get(topology2.getId()).equals("Running - Fully Scheduled by DefaultResourceAwareStrategy")) {
            numTopologiesAssigned++;
        }
        if (cluster.getStatusMap().get(topology4.getId()).equals("Running - Fully Scheduled by DefaultResourceAwareStrategy")) {
            numTopologiesAssigned++;
        }
        Assert.assertEquals(2, numTopologiesAssigned);
        //end of Test2

        //Test3: "Launch topo5 only, both mem and cpu should be exactly used up"
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config1);
        topoMap = new HashMap<>();
        topoMap.put(topology5.getId(), topology5);
        topologies = new Topologies(topoMap);
        rs.prepare(config1);
        rs.schedule(topologies, cluster);
        superToCpu = TestUtilsFormyScheduler.getSupervisorToCpuUsage(cluster, topologies);
        superToMem = TestUtilsFormyScheduler.getSupervisorToMemoryUsage(cluster, topologies);
        for (SupervisorDetails supervisor : supMap.values()) {
            Double cpuAvailable = supervisor.getTotalCPU();
            Double memAvailable = supervisor.getTotalMemory();
            Double cpuUsed = superToCpu.get(supervisor);
            Double memUsed = superToMem.get(supervisor);
            Assert.assertEquals(cpuAvailable, cpuUsed, 0.0001);
            Assert.assertEquals(memAvailable, memUsed, 0.0001);
        }
        //end of Test3
    }

    @Test
    public void testTopologyWorkerMaxHeapSize() {
        // Test1: If RAS spreads executors across multiple workers based on the set limit for a worker used by the topology
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 400.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 2000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(2, 2, resourceMap);

        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 4);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.putAll(defaultTopologyConf);
        config1.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        Map<ExecutorDetails, String> executorMap1 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology1);
        TopologyDetails topology1 = new TopologyDetails("topology1", config1, stormTopology1, 1, executorMap1, 0);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config1);
        ResourceAwareScheduler rs = new ResourceAwareScheduler();
        Map<String, TopologyDetails> topoMap = new HashMap<>();
        topoMap.put(topology1.getId(), topology1);
        Topologies topologies = new Topologies(topoMap);
        rs.prepare(config1);
        rs.schedule(topologies, cluster);
        Assert.assertEquals("Running - Fully Scheduled by DefaultResourceAwareStrategy", cluster.getStatusMap().get(topology1.getId()));
        Assert.assertEquals(4, cluster.getAssignedNumWorkers(topology1));

        // Test2: test when no more workers are available due to topology worker max heap size limit but there is memory is still available
        // wordSpout2 is going to contain 5 executors that needs scheduling. Each of those executors has a memory requirement of 128.0 MB
        // The cluster contains 4 free WorkerSlots. For this topolology each worker is limited to a max heap size of 128.0
        // Thus, one executor not going to be able to get scheduled thus failing the scheduling of this topology and no executors of this topology will be scheduleded
        TopologyBuilder builder2 = new TopologyBuilder();
        builder2.setSpout("wordSpout2", new TestWordSpout(), 5);
        StormTopology stormTopology2 = builder2.createTopology();
        Config config2 = new Config();
        config2.putAll(defaultTopologyConf);
        config2.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        Map<ExecutorDetails, String> executorMap2 = TestUtilsFormyScheduler.genExecsAndComps(stormTopology2);
        TopologyDetails topology2 = new TopologyDetails("topology2", config2, stormTopology2, 1, executorMap2, 0);
        cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config2);
        topoMap = new HashMap<>();
        topoMap.put(topology2.getId(), topology2);
        topologies = new Topologies(topoMap);
        rs.prepare(config2);
        rs.schedule(topologies, cluster);
        Assert.assertEquals("Not enough resources to schedule - 0/5 executors scheduled", cluster.getStatusMap().get(topology2.getId()));
        Assert.assertEquals(5, cluster.getUnassignedExecutors(topology2).size());
    }

    @Test(expected=IllegalArgumentException.class)
    public void testMemoryLoadLargerThanMaxHeapSize() throws Exception {
        // Topology will not be able to be successfully scheduled: Config TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB=128.0 < 129.0,
        // Largest memory requirement of a component in the topology).
        TopologyBuilder builder1 = new TopologyBuilder();
        builder1.setSpout("wordSpout1", new TestWordSpout(), 4);
        StormTopology stormTopology1 = builder1.createTopology();
        Config config1 = new Config();
        config1.putAll(defaultTopologyConf);
        config1.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 128.0);
        config1.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 129.0);
        StormSubmitter.submitTopologyWithProgressBar("test", config1, stormTopology1);
    }

    @Test
    public void TestReadInResourceAwareSchedulerUserPools() {
        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        LOG.info("fromFile: {}", fromFile);
        ConfigValidation.validateFields(fromFile);
    }

    @Test
    public void TestSubmitUsersWithNoGuarantees() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);

        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 200.0);
        resourceUserPool.get("jerry").put("memory", 2000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsFormyScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo2 = TestUtilsFormyScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo3 = TestUtilsFormyScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo4 = TestUtilsFormyScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo5 = TestUtilsFormyScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 20);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 3, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("assert topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("bobby").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
    }

    @Test
    public void TestTopologySortedInCorrectOrder() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(20, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());

        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 10.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 128.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 0.0);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, TOPOLOGY_SUBMITTER);

        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 1000);
        resourceUserPool.get("jerry").put("memory", 8192.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 10000.0);
        resourceUserPool.get("bobby").put("memory", 32768);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 5000.0);
        resourceUserPool.get("derek").put("memory", 16384.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);

        TopologyDetails topo1 = TestUtilsFormyScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsFormyScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 30);
        TopologyDetails topo3 = TestUtilsFormyScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, 30);
        TopologyDetails topo4 = TestUtilsFormyScheduler.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo5 = TestUtilsFormyScheduler.getTopology("topo-5", config, 5, 15, 1, 1, currentTime - 24, 30);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        Set<TopologyDetails> queue = rs.getUser("jerry").getTopologiesPending();
        Assert.assertEquals("check size", queue.size(), 0);

        queue = rs.getUser("jerry").getTopologiesRunning();

        Iterator<TopologyDetails> itr = queue.iterator();

        TopologyDetails topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-4");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-1");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-5");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-3");

        topo = itr.next();
        LOG.info("{} - {}", topo.getName(), queue);
        Assert.assertEquals("check order", topo.getName(), "topo-2");

        TopologyDetails topo6 = TestUtilsFormyScheduler.getTopology("topo-6", config, 5, 15, 1, 1, currentTime - 30, 10);
        topoMap.put(topo6.getId(), topo6);

        topologies = new Topologies(topoMap);
        rs.prepare(config);
        rs.schedule(topologies, cluster);

        queue = rs.getUser("jerry").getTopologiesRunning();
        itr = queue.iterator();

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-6");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-4");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-1");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-5");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-3");

        topo = itr.next();
        Assert.assertEquals("check order", topo.getName(), "topo-2");

        queue = rs.getUser("jerry").getTopologiesPending();
        Assert.assertEquals("check size", queue.size(), 0);
    }

    @Test
    public void TestMultipleUsers() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 1000.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 10);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(20, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 1000);
        resourceUserPool.get("jerry").put("memory", 8192.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 10000.0);
        resourceUserPool.get("bobby").put("memory", 32768);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 5000.0);
        resourceUserPool.get("derek").put("memory", 16384.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsFormyScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsFormyScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 29);
        TopologyDetails topo3 = TestUtilsFormyScheduler.getTopology("topo-3", config, 5, 15, 1, 1, currentTime - 16, 29);
        TopologyDetails topo4 = TestUtilsFormyScheduler.getTopology("topo-4", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo5 = TestUtilsFormyScheduler.getTopology("topo-5", config, 5, 15, 1, 1, currentTime - 24, 29);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo6 = TestUtilsFormyScheduler.getTopology("topo-6", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo7 = TestUtilsFormyScheduler.getTopology("topo-7", config, 5, 15, 1, 1, currentTime - 8, 29);
        TopologyDetails topo8 = TestUtilsFormyScheduler.getTopology("topo-8", config, 5, 15, 1, 1, currentTime - 16, 29);
        TopologyDetails topo9 = TestUtilsFormyScheduler.getTopology("topo-9", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo10 = TestUtilsFormyScheduler.getTopology("topo-10", config, 5, 15, 1, 1, currentTime - 24, 29);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo11 = TestUtilsFormyScheduler.getTopology("topo-11", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo12 = TestUtilsFormyScheduler.getTopology("topo-12", config, 5, 15, 1, 1, currentTime - 8, 29);
        TopologyDetails topo13 = TestUtilsFormyScheduler.getTopology("topo-13", config, 5, 15, 1, 1, currentTime - 16, 29);
        TopologyDetails topo14 = TestUtilsFormyScheduler.getTopology("topo-14", config, 5, 15, 1, 1, currentTime - 16, 20);
        TopologyDetails topo15 = TestUtilsFormyScheduler.getTopology("topo-15", config, 5, 15, 1, 1, currentTime - 24, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);
        topoMap.put(topo6.getId(), topo6);
        topoMap.put(topo7.getId(), topo7);
        topoMap.put(topo8.getId(), topo8);
        topoMap.put(topo9.getId(), topo9);
        topoMap.put(topo10.getId(), topo10);
        topoMap.put(topo11.getId(), topo11);
        topoMap.put(topo12.getId(), topo12);
        topoMap.put(topo13.getId(), topo13);
        topoMap.put(topo14.getId(), topo14);
        topoMap.put(topo15.getId(), topo15);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : topoMap.values()) {
            Assert.assertTrue(TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }

        for (User user : rs.getUserMap().values()) {
            Assert.assertEquals(user.getTopologiesPending().size(), 0);
            Assert.assertEquals(user.getTopologiesRunning().size(), 5);
        }
    }

    @Test
    public void testHandlingClusterSubscription() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 200.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1024.0 * 10);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(1, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 1000);
        resourceUserPool.get("jerry").put("memory", 8192.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 10000.0);
        resourceUserPool.get("bobby").put("memory", 32768);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 5000.0);
        resourceUserPool.get("derek").put("memory", 16384.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsFormyScheduler.getTopology("topo-1", config, 5, 15, 1, 1, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsFormyScheduler.getTopology("topo-2", config, 5, 15, 1, 1, currentTime - 8, 29);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        int fullyScheduled = 0;
        for (TopologyDetails topo : topoMap.values()) {
            if (TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId()))) {
                fullyScheduled++;
            }
        }
        Assert.assertEquals("# of Fully scheduled", 1, fullyScheduled);
        Assert.assertEquals("# of topologies schedule attempted", 1, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of topologies running", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of topologies schedule pending", 0, rs.getUser("jerry").getTopologiesPending().size());
    }

    /**
     * Test correct behaviour when a supervisor dies.  Check if the scheduler handles it correctly and evicts the correct
     * topology when rescheduling the executors from the died supervisor
     */
    @Test
    public void TestFaultTolerance() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(6, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        Map<String, Map<String, Number>> resourceUserPool = new HashMap<String, Map<String, Number>>();
        resourceUserPool.put("jerry", new HashMap<String, Number>());
        resourceUserPool.get("jerry").put("cpu", 50.0);
        resourceUserPool.get("jerry").put("memory", 500.0);

        resourceUserPool.put("bobby", new HashMap<String, Number>());
        resourceUserPool.get("bobby").put("cpu", 200.0);
        resourceUserPool.get("bobby").put("memory", 2000.0);

        resourceUserPool.put("derek", new HashMap<String, Number>());
        resourceUserPool.get("derek").put("cpu", 100.0);
        resourceUserPool.get("derek").put("memory", 1000.0);

        config.put(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS, resourceUserPool);
        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsFormyScheduler.getTopology("topo-1", config, 1, 0, 1, 0, currentTime - 2, 20);
        TopologyDetails topo2 = TestUtilsFormyScheduler.getTopology("topo-2", config, 1, 0, 1, 0, currentTime - 2, 20);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "bobby");

        TopologyDetails topo3 = TestUtilsFormyScheduler.getTopology("topo-3", config, 1, 0, 1, 0, currentTime - 2, 10);
        TopologyDetails topo4 = TestUtilsFormyScheduler.getTopology("topo-4", config, 1, 0, 1, 0, currentTime - 2, 10);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "derek");

        TopologyDetails topo5 = TestUtilsFormyScheduler.getTopology("topo-5", config, 1, 0, 1, 0, currentTime - 2, 29);
        TopologyDetails topo6 = TestUtilsFormyScheduler.getTopology("topo-6", config, 1, 0, 1, 0, currentTime - 2, 10);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);
        topoMap.put(topo4.getId(), topo4);
        topoMap.put(topo5.getId(), topo5);
        topoMap.put(topo6.getId(), topo6);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());

        //fail supervisor
        SupervisorDetails supFailed = cluster.getSupervisors().values().iterator().next();
        LOG.info("/***** failing supervisor: {} ****/", supFailed.getHost());
        supMap.remove(supFailed.getId());
        Map<String, SchedulerAssignmentImpl> newAssignments = new HashMap<String, SchedulerAssignmentImpl>();
        for (Map.Entry<String, SchedulerAssignment> topoToAssignment : cluster.getAssignments().entrySet()) {
            String topoId = topoToAssignment.getKey();
            SchedulerAssignment assignment = topoToAssignment.getValue();
            Map<ExecutorDetails, WorkerSlot> executorToSlots = new HashMap<ExecutorDetails, WorkerSlot>();
            for (Map.Entry<ExecutorDetails, WorkerSlot> execToWorker : assignment.getExecutorToSlot().entrySet()) {
                ExecutorDetails exec = execToWorker.getKey();
                WorkerSlot ws = execToWorker.getValue();
                if (!ws.getNodeId().equals(supFailed.getId())) {
                    executorToSlots.put(exec, ws);
                }
            }
            newAssignments.put(topoId, new SchedulerAssignmentImpl(topoId, executorToSlots));
        }
        Map<String, String> statusMap = cluster.getStatusMap();
        cluster = new Cluster(iNimbus, supMap, newAssignments, config);
        cluster.setStatusMap(statusMap);

        rs.schedule(topologies, cluster);

        //Supervisor failed contains a executor from topo-6 of user derek.  Should evict a topology from user jerry since user will be above resource guarantee more so than user derek
        for (TopologyDetails topo : rs.getUser("jerry").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 1, rs.getUser("jerry").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("jerry").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 1, rs.getUser("jerry").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("jerry").getTopologiesInvalid().size());


        for (TopologyDetails topo : rs.getUser("derek").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("derek").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("derek").getTopologiesPending().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("derek").getTopologiesAttempted().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("derek").getTopologiesInvalid().size());

        for (TopologyDetails topo : rs.getUser("bobby").getTopologiesRunning()) {
            Assert.assertTrue("Assert scheduling topology success", TestUtilsFormyScheduler.assertStatusSuccess(cluster.getStatusMap().get(topo.getId())));
        }
        Assert.assertEquals("# of running topologies", 2, rs.getUser("bobby").getTopologiesRunning().size());
        Assert.assertEquals("# of pending topologies", 0, rs.getUser("bobby").getTopologiesPending().size());
        Assert.assertEquals("# of invalid topologies", 0, rs.getUser("bobby").getTopologiesInvalid().size());
        Assert.assertEquals("# of attempted topologies", 0, rs.getUser("bobby").getTopologiesAttempted().size());
    }

    /**
     * test if free slots on nodes work correctly
     */
    @Test
    public void TestNodeFreeSlot() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(4, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);
        TopologyDetails topo1 = TestUtilsFormyScheduler.getTopology("topo-1", config, 1, 0, 2, 0, currentTime - 2, 29);
        TopologyDetails topo2 = TestUtilsFormyScheduler.getTopology("topo-2", config, 1, 0, 2, 0, currentTime - 2, 10);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        Map<String, RAS_Node> nodes = RAS_Nodes.getAllNodesFrom(cluster, topologies);

        for (SchedulerAssignment entry : cluster.getAssignments().values()) {
            for (WorkerSlot ws : entry.getSlots()) {
                double memoryBefore = nodes.get(ws.getNodeId()).getAvailableMemoryResources();
                double cpuBefore = nodes.get(ws.getNodeId()).getAvailableCpuResources();
                double memoryUsedByWorker = nodes.get(ws.getNodeId()).getMemoryUsedByWorker(ws);
                Assert.assertEquals("Check if memory used by worker is calculated correctly", 1000.0, memoryUsedByWorker, 0.001);
                double cpuUsedByWorker = nodes.get(ws.getNodeId()).getCpuUsedByWorker(ws);
                Assert.assertEquals("Check if CPU used by worker is calculated correctly", 100.0, cpuUsedByWorker, 0.001);
                nodes.get(ws.getNodeId()).free(ws);
                double memoryAfter = nodes.get(ws.getNodeId()).getAvailableMemoryResources();
                double cpuAfter = nodes.get(ws.getNodeId()).getAvailableCpuResources();
                Assert.assertEquals("Check if free correctly frees amount of memory", memoryBefore + memoryUsedByWorker,  memoryAfter, 0.001);
                Assert.assertEquals("Check if free correctly frees amount of memory", cpuBefore + cpuUsedByWorker,  cpuAfter, 0.001);
                Assert.assertFalse("Check if worker was removed from assignments", entry.getSlotToExecutors().containsKey(ws));
            }
        }
    }

    /**
     * When the first topology failed to be scheduled make sure subsequent schedulings can still succeed
     */
    @Test
    public void TestSchedulingAfterFailedScheduling() {
        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(8, 4, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        TopologyDetails topo1 = TestUtilsFormyScheduler.getTopology("topo-1", config, 8, 0, 2, 0, currentTime - 2, 10);
        TopologyDetails topo2 = TestUtilsFormyScheduler.getTopology("topo-2", config, 2, 0, 2, 0, currentTime - 2, 20);
        TopologyDetails topo3 = TestUtilsFormyScheduler.getTopology("topo-3", config, 1, 2, 1, 1, currentTime - 2, 20);

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo1.getId(), topo1);
        topoMap.put(topo2.getId(), topo2);
        topoMap.put(topo3.getId(), topo3);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        Assert.assertTrue("Topo-2 scheduled?", cluster.getAssignmentById(topo2.getId()) != null);
        Assert.assertEquals("Topo-2 all executors scheduled?", 4, cluster.getAssignmentById(topo2.getId()).getExecutorToSlot().size());
        Assert.assertTrue("Topo-3 scheduled?", cluster.getAssignmentById(topo3.getId()) != null);
        Assert.assertEquals("Topo-3 all executors scheduled?", 3, cluster.getAssignmentById(topo3.getId()).getExecutorToSlot().size());
    }

    /**
     * Test multiple spouts and cyclic topologies
     */
    @Test
    public void TestMultipleSpoutsAndCyclicTopologies() {

        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer s1 = builder.setSpout("spout-1", new TestUtilsFormyScheduler.TestSpout(),
                5);
        SpoutDeclarer s2 = builder.setSpout("spout-2", new TestUtilsFormyScheduler.TestSpout(),
                5);
        BoltDeclarer b1 = builder.setBolt("bolt-1", new TestUtilsFormyScheduler.TestBolt(),
                5).shuffleGrouping("spout-1").shuffleGrouping("bolt-3");
        BoltDeclarer b2 = builder.setBolt("bolt-2", new TestUtilsFormyScheduler.TestBolt(),
                5).shuffleGrouping("bolt-1");
        BoltDeclarer b3 = builder.setBolt("bolt-3", new TestUtilsFormyScheduler.TestBolt(),
                5).shuffleGrouping("bolt-2").shuffleGrouping("spout-2");

        INimbus iNimbus = new TestUtilsFormyScheduler.INimbusTest();
        Map<String, Number> resourceMap = new HashMap<String, Number>();
        resourceMap.put(Config.SUPERVISOR_CPU_CAPACITY, 100.0);
        resourceMap.put(Config.SUPERVISOR_MEMORY_CAPACITY_MB, 1000.0);
        Map<String, SupervisorDetails> supMap = TestUtilsFormyScheduler.genSupervisors(25, 1, resourceMap);
        Config config = new Config();
        config.putAll(Utils.readDefaultConfig());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY, org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy.class.getName());
        config.put(Config.RESOURCE_AWARE_SCHEDULER_PRIORITY_STRATEGY, org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy.class.getName());
        config.put(Config.TOPOLOGY_SCHEDULER_STRATEGY, org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy.class.getName());
        config.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, 100.0);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, 500);
        config.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, Double.MAX_VALUE);

        StormTopology stormTopology = builder.createTopology();
        TopologyDetails topo = new TopologyDetails("topo-1", config, stormTopology,
                0,
                genExecsAndComps(stormTopology), 0);

        Cluster cluster = new Cluster(iNimbus, supMap, new HashMap<String, SchedulerAssignmentImpl>(), config);

        config.put(Config.TOPOLOGY_SUBMITTER_USER, "jerry");

        Map<String, TopologyDetails> topoMap = new HashMap<String, TopologyDetails>();
        topoMap.put(topo.getId(), topo);

        Topologies topologies = new Topologies(topoMap);

        ResourceAwareScheduler rs = new ResourceAwareScheduler();

        rs.prepare(config);
        rs.schedule(topologies, cluster);

        Assert.assertTrue("Topo scheduled?", cluster.getAssignmentById(topo.getId()) != null);
        Assert.assertEquals("Topo all executors scheduled?", 25, cluster.getAssignmentById(topo.getId()).getExecutorToSlot().size());
    }
}

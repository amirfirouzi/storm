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
import org.apache.storm.graph.Graph;
import org.apache.storm.graph.TopologyGraphBuilder;
import org.apache.storm.graph.partitioner.CostFunction;
import org.apache.storm.graph.partitioner.Partitioner;
import org.apache.storm.graph.partitioner.PartitioningResult;
import org.apache.storm.scheduler.*;
import org.apache.storm.scheduler.resource.monitoring.DataManager;
import org.apache.storm.scheduler.resource.monitoring.ExecutorPair;
import org.apache.storm.scheduler.resource.strategies.eviction.IEvictionStrategy;
import org.apache.storm.scheduler.resource.strategies.scheduling.IStrategy;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.storm.scheduler.resource.monitoring.Utils.RESCHEDULE_TIMEOUT;
import static org.apache.storm.scheduler.resource.monitoring.Utils.collectionToString;

public class myScheduler implements IScheduler {

    // Object that holds the current scheduling state
    private SchedulingState schedulingState;
    private static final int DEFAULT_RESCHEDULE_TIMEOUT = 180;
    //    private Map<String, Long> lastRescheduledTopologies;
    private Map<String, PartitioningResult> lastPartitioning;
    private long lastRescheduling;
    private Map<String, Graph> topoGraphs;
    private boolean firstRescheduling;

    @SuppressWarnings("rawtypes")
    private Map conf;

    private static final Logger LOG = LoggerFactory
            .getLogger(myScheduler.class);

    @Override
    public void prepare(Map conf) {
        this.conf = conf;
        if (lastPartitioning == null)
            lastPartitioning = new LinkedHashMap<>();
        topoGraphs = new HashMap<>();
        firstRescheduling = true;

    }

    private void initialize(Topologies topologies, Cluster cluster) {
        Map<String, User> userMap = getUsers(topologies, cluster);
        this.schedulingState = new SchedulingState(userMap, cluster, topologies, this.conf);
        for (TopologyDetails td :
                topologies.getTopologies()) {
            Graph graph = TopologyGraphBuilder.buildGraph(td);
            if (!topoGraphs.containsKey(td.getId())) {
                topoGraphs.put(td.getId(), graph);
            }
        }
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        LOG.debug("\n\n\nRerunning myResourceAwareScheduler...");
        //initialize data structures
        initialize(topologies, cluster);
        //logs everything that is currently scheduled and the location at which they are scheduled
        LOG.info("Cluster scheduling:\n{}", ResourceUtils.printScheduling(cluster, topologies));
        //logs the resources available/used for every node
        LOG.info("Nodes:\n{}", this.schedulingState.nodes);
        //logs the detailed info about each user
        for (User user : getUserMap().values()) {
            LOG.info(user.getDetailedInfo());
        }

        if (!topologies.getTopologies().isEmpty()) {
            int rescheduleTimeout = DEFAULT_RESCHEDULE_TIMEOUT;
            for (TopologyDetails td : topologies.getTopologies()) {
                rescheduleTimeout = Integer.parseInt(td.getConf().get(RESCHEDULE_TIMEOUT).toString());
//                lastRescheduledTopologies.put(td.getId(), 0L);

            }
            long now = System.currentTimeMillis();
            long elapsedTime = (now - lastRescheduling) / 1000; // s
            if (lastRescheduling == 0 || elapsedTime >= rescheduleTimeout)
                chooseSchedulingPlan(topologies, cluster);
            else
                LOG.info("It's not time to reschedule yet, " + elapsedTime + " seconds have passed, other " + (rescheduleTimeout - elapsedTime) + " seconds have to pass");
        }
        LOG.info("Nodes after scheduling:\n{}", this.schedulingState.nodes);

        updateChanges(cluster, topologies);
    }

    private void chooseSchedulingPlan(Topologies topologies, Cluster cluster) {
        List<String> dbTopologies = null;
        try {
            dbTopologies = DataManager.getInstance(conf).getTopologies();
        } catch (Exception e) {
            dbTopologies = new ArrayList<>();
            e.printStackTrace();
        }
        LOG.info("DB Topologies: " + collectionToString(dbTopologies));

        // get topologies from Storm and identify topologies to be deleted
        List<String> topologiesToBeRemoved = new ArrayList<String>(dbTopologies);
//            int trafficImprovement = 0;
        for (TopologyDetails td : topologies.getTopologies()) {

            if (dbTopologies.contains(td.getId())) {// ReScheduling this topology(RePartitioning)
                reScheduleTopology(td);
            } else {// First Time To Schedule this topology(initial Partitioning)
                initialScheduleTopology(td);
            }
            lastRescheduling = System.currentTimeMillis();
            topologiesToBeRemoved.remove(td.getId());
            LOG.debug("Configuration of topology " + td.getId());
            for (Object key : td.getConf().keySet())
                LOG.debug("- " + key + ": " + td.getConf().get(key));
        }

        dbTopologies.removeAll(topologiesToBeRemoved);
        LOG.info("Topologies to be removed from DB: " + collectionToString(topologiesToBeRemoved));

        // remove topologies from DB
        if (!topologiesToBeRemoved.isEmpty()) {
            try {
                DataManager.getInstance(conf).removeTopologies(topologiesToBeRemoved);
            } catch (Exception e) {
                e.printStackTrace();
            }
            LOG.info("Topologies succesfully removed from DB");
        }

    }

    private void updateChanges(Cluster cluster, Topologies topologies) {
        //Cannot simply set this.cluster=schedulingState.cluster since clojure is immutable
        cluster.setAssignments(schedulingState.cluster.getAssignments());
        cluster.setBlacklistedHosts(schedulingState.cluster.getBlacklistedHosts());
        cluster.setStatusMap(schedulingState.cluster.getStatusMap());
        cluster.setSupervisorsResourcesMap(schedulingState.cluster.getSupervisorsResourcesMap());
        cluster.setTopologyResourcesMap(schedulingState.cluster.getTopologyResourcesMap());
        cluster.setWorkerResourcesMap(schedulingState.cluster.getWorkerResourcesMap());
        //updating resources used by supervisor
        updateSupervisorsResources(cluster, topologies);
    }

    public void initialScheduleTopology(TopologyDetails td) {

        Graph graph = topoGraphs.get(td.getId());
        LOG.info("Generated Graph:\n {}", graph);
        PartitioningResult partitioning = Partitioner.doPartition(CostFunction.costMode.Both, true, graph, schedulingState);
        lastPartitioning.put(td.getId(), partitioning);
        scheduleTopology(td, partitioning, false);

    }

    public void reScheduleTopology(TopologyDetails td) {

        Graph graph = topoGraphs.get(td.getId());
        LOG.info("Generated Graph:\n {}", graph);
        List<ExecutorPair> traffic = new ArrayList<>();
        try {
            traffic = DataManager.getInstance(conf).getInterExecutorTrafficList(td.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (ExecutorPair exec :
                traffic) {
            String from = exec.getSource().getFullExecutorName();
            String to = exec.getDestination().getFullExecutorName();
            int trafficValue = exec.getTraffic();
            graph.getEdgeFromExecutor(from, to).setWeight(trafficValue);
            //TODO: update vertex weights from monitoring
        }

        PartitioningResult partitioning = Partitioner.doPartition(CostFunction.costMode.Both, true, graph, schedulingState);
        boolean isPartitioningImproved = isPartitioningImproved(td.getId(), partitioning);
        scheduleTopology(td, partitioning, isPartitioningImproved);
        if (isPartitioningImproved)
            lastPartitioning.put(td.getId(), partitioning);
    }

    public boolean isPartitioningImproved(String td, PartitioningResult newPartitioning) {
        //TODO: now only considers crosscut, ADD loads and other factors too
        int newCut = newPartitioning.getBestCut();
        int lastCut = lastPartitioning.get(td).getBestCut();
        boolean weightedGraph = lastPartitioning.get(td).getGraph().doesEdgesHaveWeight();
        if ((newCut < lastCut) || (firstRescheduling && weightedGraph)) {
            LOG.info("\n****************Partitioning is Improved: lastCut={}, newCut={}, firstRescheduling:{}\n",
                    lastCut, newCut, firstRescheduling);
            if (firstRescheduling)
                firstRescheduling = false;
            return true;
        } else {
            LOG.info("\n****************Partitioning is NOT Improved: lastCut={}, newCut={}, firstRescheduling:{}\n",
                    lastCut, newCut, firstRescheduling);
            return false;
        }

    }

    public void scheduleTopology(TopologyDetails td, PartitioningResult partitioning, boolean rePartitioning) {
        User topologySubmitter = this.schedulingState.userMap.get(td.getTopologySubmitter());
        if (this.schedulingState.cluster.getUnassignedExecutors(td).size() > 0 || rePartitioning) {
            LOG.debug("/********Scheduling topology {} from User {}************/", td.getName(), topologySubmitter);

            SchedulingState schedulingState = checkpointSchedulingState();
            IStrategy rasStrategy = null;
            try {

                rasStrategy = (IStrategy) Utils.newInstance((String) td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY));

            } catch (RuntimeException e) {
                LOG.error("failed to create instance of IStrategy: {} with error: {}! Topology {} will not be scheduled.",
                        td.getName(), td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY), e.getMessage());
                topologySubmitter = cleanup(schedulingState, td);
                topologySubmitter.moveTopoFromPendingToInvalid(td);
                this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - failed to create instance of topology strategy "
                        + td.getConf().get(Config.TOPOLOGY_SCHEDULER_STRATEGY) + ". Please check logs for details");
                return;
            }
            IEvictionStrategy evictionStrategy = null;
            while (true) {
                SchedulingResult result = null;
                try {
                    // Need to re prepare scheduling strategy with cluster and topologies in case scheduling state was restored
                    // Pass in a copy of scheduling state since the scheduling strategy should not be able to be able to make modifications to
                    // the state of cluster directly
                    rasStrategy.prepare(new SchedulingState(this.schedulingState));
                    if (rePartitioning)
                        result = rasStrategy.reSchedule(td, lastPartitioning.get(td.getId()), partitioning);
                    else
                        result = rasStrategy.schedule(td, partitioning);
                } catch (Exception ex) {
                    LOG.error(String.format("Exception thrown when running strategy %s to schedule topology %s. Topology will not be scheduled!"
                            , rasStrategy.getClass().getName(), td.getName()), ex);
                    topologySubmitter = cleanup(schedulingState, td);
                    topologySubmitter.moveTopoFromPendingToInvalid(td);
                    this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - Exception thrown when running strategy {}"
                            + rasStrategy.getClass().getName() + ". Please check logs for details");
                }
                LOG.debug("scheduling result: {}", result);
                if (result != null && result.isValid()) {
                    if (result.isSuccess()) {
                        try {
                            if (mkAssignment(td, result.getSchedulingResultMap())) {
                                topologySubmitter.moveTopoFromPendingToRunning(td);
                                this.schedulingState.cluster.setStatus(td.getId(), "Running - " + result.getMessage());
                            } else {
                                topologySubmitter = this.cleanup(schedulingState, td);
                                topologySubmitter.moveTopoFromPendingToAttempted(td);
                                this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - Unable to assign executors to nodes. Please check logs for details");
                            }
                        } catch (IllegalStateException ex) {
                            LOG.error("Unsuccessful in scheduling - IllegalStateException thrown when attempting to assign executors to nodes.", ex);
                            topologySubmitter = cleanup(schedulingState, td);
                            topologySubmitter.moveTopoFromPendingToAttempted(td);
                            this.schedulingState.cluster.setStatus(td.getId(), "Unsuccessful in scheduling - IllegalStateException thrown when attempting to assign executors to nodes. Please check log for details.");
                        }
                        break;
                    } else {
                        if (result.getStatus() == SchedulingStatus.FAIL_NOT_ENOUGH_RESOURCES) {
                            if (evictionStrategy == null) {
                                try {
                                    evictionStrategy = (IEvictionStrategy) Utils.newInstance((String) this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY));
                                } catch (RuntimeException e) {
                                    LOG.error("failed to create instance of eviction strategy: {} with error: {}! No topology eviction will be done.",
                                            this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_EVICTION_STRATEGY), e.getMessage());
                                    topologySubmitter.moveTopoFromPendingToAttempted(td);
                                    break;
                                }
                            }
                            boolean madeSpace = false;
                            try {
                                //need to re prepare since scheduling state might have been restored
                                evictionStrategy.prepare(this.schedulingState);
                                madeSpace = evictionStrategy.makeSpaceForTopo(td);
                            } catch (Exception ex) {
                                LOG.error(String.format("Exception thrown when running eviction strategy %s to schedule topology %s. No evictions will be done! Error: %s"
                                        , evictionStrategy.getClass().getName(), td.getName(), ex.getClass().getName()), ex);
                                topologySubmitter = cleanup(schedulingState, td);
                                topologySubmitter.moveTopoFromPendingToAttempted(td);
                                break;
                            }
                            if (!madeSpace) {
                                LOG.debug("Could not make space for topo {} will move to attempted", td);
                                topologySubmitter = cleanup(schedulingState, td);
                                topologySubmitter.moveTopoFromPendingToAttempted(td);
                                this.schedulingState.cluster.setStatus(td.getId(), "Not enough resources to schedule - " + result.getErrorMessage());
                                break;
                            }
                            continue;
                        } else if (result.getStatus() == SchedulingStatus.FAIL_INVALID_TOPOLOGY) {
                            topologySubmitter = cleanup(schedulingState, td);
                            topologySubmitter.moveTopoFromPendingToInvalid(td, this.schedulingState.cluster);
                            break;
                        } else {
                            topologySubmitter = cleanup(schedulingState, td);
                            topologySubmitter.moveTopoFromPendingToAttempted(td, this.schedulingState.cluster);
                            break;
                        }
                    }
                } else {
                    LOG.warn("Scheduling results returned from topology {} is not vaild! Topology with be ignored.", td.getName());
                    topologySubmitter = cleanup(schedulingState, td);
                    topologySubmitter.moveTopoFromPendingToInvalid(td, this.schedulingState.cluster);
                    break;
                }
            }
        } else {
            LOG.warn("Topology {} is already fully scheduled!", td.getName());
            topologySubmitter.moveTopoFromPendingToRunning(td);
            if (this.schedulingState.cluster.getStatusMap().get(td.getId()) == null || this.schedulingState.cluster.getStatusMap().get(td.getId()).equals("")) {
                this.schedulingState.cluster.setStatus(td.getId(), "Fully Scheduled");
            }
        }
    }

    private User cleanup(SchedulingState schedulingState, TopologyDetails td) {
        restoreCheckpointSchedulingState(schedulingState);
        //since state is restored need the update User topologySubmitter to the new User object in userMap
        return this.schedulingState.userMap.get(td.getTopologySubmitter());
    }

    private boolean mkAssignment(TopologyDetails td, Map<WorkerSlot, Collection<ExecutorDetails>> schedulerAssignmentMap) {
        if (schedulerAssignmentMap != null) {
            double requestedMemOnHeap = td.getTotalRequestedMemOnHeap();
            double requestedMemOffHeap = td.getTotalRequestedMemOffHeap();
            double requestedCpu = td.getTotalRequestedCpu();
            double assignedMemOnHeap = 0.0;
            double assignedMemOffHeap = 0.0;
            double assignedCpu = 0.0;

            Map<WorkerSlot, Double[]> workerResources = new HashMap<WorkerSlot, Double[]>();


            //region diff Assignments

            //initialize newAssignmentExecutorToWorker a Map of Execs to Workers of new Assignment
            // and remove Null Slots from assignment
            List<WorkerSlot> nullAssignments = new ArrayList<>();
            Map<ExecutorDetails, WorkerSlot> newAssignmentExecutorToWorker = new LinkedHashMap<>();
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> wsExecs :
                    schedulerAssignmentMap.entrySet()) {
                if (wsExecs.getValue().isEmpty()) {
                    nullAssignments.add(wsExecs.getKey());
                } else {
                    for (ExecutorDetails exec :
                            wsExecs.getValue()) {
                        if (!newAssignmentExecutorToWorker.containsKey(exec)) {
                            newAssignmentExecutorToWorker.put(exec, wsExecs.getKey());
                        }
                    }
                }
            }

            //compare New Assignment with latest cluster assignment
            Map<String, SchedulerAssignment> clusterSchedulingState = schedulingState.cluster.getAssignments();
            Map<WorkerSlot, Collection<ExecutorDetails>> clusterSlotToExecutors;
            if ((!clusterSchedulingState.isEmpty()) &&
                    (!clusterSchedulingState.get(td.getId()).getExecutorToSlot().isEmpty())) {
                clusterSlotToExecutors = clusterSchedulingState.get(td.getId()).getSlotToExecutors();

                LOG.info(SchedulingUtils.assignmentToString(clusterSlotToExecutors, "Cluster Assignments(Before)"));

                WorkerSlot ws;
                for (Map.Entry<ExecutorDetails, WorkerSlot> assignment :
                        newAssignmentExecutorToWorker.entrySet()) {
                    //find exact worker and exec object instances in cluster assignments(past assignment)
                    WorkerSlot clusterWorker = findWorkerInAssignment(assignment.getKey(), clusterSlotToExecutors);
                    ExecutorDetails clusterExecutor = findExecuterInAssignment(assignment.getKey(),
                            clusterSlotToExecutors.get(clusterWorker));
                    if (clusterSlotToExecutors.get(clusterWorker).isEmpty()) {
                        clusterSlotToExecutors.remove(clusterWorker);
                        schedulingState.cluster.freeSlot(clusterWorker);
                    }
                    // it means that current exec(execToWorker.getKey()) is moved to another ws,
                    // so it should be removed from last assignment and also from cluster
                    if (!clusterWorker.equals(assignment.getValue())) {
                        clusterSlotToExecutors.get(clusterWorker).remove(clusterExecutor);
                        clusterSchedulingState.get(td.getId()).getExecutorToSlot().remove(clusterExecutor, clusterWorker);
                    }
                }
                LOG.info(SchedulingUtils.assignmentToString(clusterSlotToExecutors, "Cluster Assignments(After)"));
                LOG.info(SchedulingUtils.assignmentToString(schedulerAssignmentMap, "New Assignments(Before)"));

                for (WorkerSlot worker :
                        nullAssignments) {
                    schedulerAssignmentMap.remove(worker);
                }
                for (WorkerSlot worker :
                        clusterSlotToExecutors.keySet()) {
                    schedulerAssignmentMap.remove(worker);
                }
            }

            LOG.info(SchedulingUtils.assignmentToString(schedulerAssignmentMap, "New Assignments(After)"));
            //endregion

            Set<String> nodesUsed = new HashSet<String>();
            for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> workerToTasksEntry : schedulerAssignmentMap.entrySet()) {
                WorkerSlot targetSlot = workerToTasksEntry.getKey();
                Collection<ExecutorDetails> execsNeedScheduling = workerToTasksEntry.getValue();
                RAS_Node targetNode = this.schedulingState.nodes.getNodeById(targetSlot.getNodeId());

                targetSlot = allocateResourceToSlot(td, execsNeedScheduling, targetSlot);

                targetNode.assign(targetSlot, td, execsNeedScheduling);

                LOG.debug("ASSIGNMENT    TOPOLOGY: {}  TASKS: {} To Node: {} on Slot: {}",
                        td.getName(), execsNeedScheduling, targetNode.getHostname(), targetSlot.getPort());

                for (ExecutorDetails exec : execsNeedScheduling) {
                    targetNode.consumeResourcesforTask(exec, td);
                }
                if (!nodesUsed.contains(targetNode.getId())) {
                    nodesUsed.add(targetNode.getId());
                }
                assignedMemOnHeap += targetSlot.getAllocatedMemOnHeap();
                assignedMemOffHeap += targetSlot.getAllocatedMemOffHeap();
                assignedCpu += targetSlot.getAllocatedCpu();

                Double[] worker_resources = {
                        requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                        targetSlot.getAllocatedMemOnHeap(), targetSlot.getAllocatedMemOffHeap(), targetSlot.getAllocatedCpu()};
                workerResources.put(targetSlot, worker_resources);
            }

            Double[] resources = {requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                    assignedMemOnHeap, assignedMemOffHeap, assignedCpu};
            LOG.debug("setTopologyResources for {}: requested on-heap mem, off-heap mem, cpu: {} {} {} " +
                            "assigned on-heap mem, off-heap mem, cpu: {} {} {}",
                    td.getId(), requestedMemOnHeap, requestedMemOffHeap, requestedCpu,
                    assignedMemOnHeap, assignedMemOffHeap, assignedCpu);
            //updating resources used for a topology
            this.schedulingState.cluster.setTopologyResources(td.getId(), resources);
            this.schedulingState.cluster.setWorkerResources(td.getId(), workerResources);
            return true;
        } else {
            LOG.warn("schedulerAssignmentMap for topo {} is null. This shouldn't happen!", td.getName());
            return false;
        }
    }

    private ExecutorDetails findExecuterInAssignment(ExecutorDetails exec, Collection<ExecutorDetails> executers) {
        if (exec != null && executers != null) {
            for (ExecutorDetails executor :
                    executers) {
                if (exec.toString().equals(executor.toString()))
                    return executor;
            }
        }
        return null;
    }

    private WorkerSlot findWorkerInAssignment(ExecutorDetails exec, Map<WorkerSlot, Collection<ExecutorDetails>> assignments) {
        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> assignment :
                assignments.entrySet()) {
            for (ExecutorDetails executor :
                    assignment.getValue()) {
                if (exec.toString().equals(executor.toString()))
                    return assignment.getKey();
            }
        }
        return null;
    }

    private WorkerSlot allocateResourceToSlot(TopologyDetails td, Collection<ExecutorDetails> executors, WorkerSlot slot) {
        double onHeapMem = 0.0;
        double offHeapMem = 0.0;
        double cpu = 0.0;
        for (ExecutorDetails exec : executors) {
            Double onHeapMemForExec = td.getOnHeapMemoryRequirement(exec);
            if (onHeapMemForExec != null) {
                onHeapMem += onHeapMemForExec;
            }
            Double offHeapMemForExec = td.getOffHeapMemoryRequirement(exec);
            if (offHeapMemForExec != null) {
                offHeapMem += offHeapMemForExec;
            }
            Double cpuForExec = td.getTotalCpuReqTask(exec);
            if (cpuForExec != null) {
                cpu += cpuForExec;
            }
        }
        return new WorkerSlot(slot.getNodeId(), slot.getPort(), onHeapMem, offHeapMem, cpu);
    }

    private void updateSupervisorsResources(Cluster cluster, Topologies topologies) {
        Map<String, Double[]> supervisors_resources = new HashMap<String, Double[]>();
        Map<String, RAS_Node> nodes = RAS_Nodes.getAllNodesFrom(cluster, topologies);
        for (Map.Entry<String, RAS_Node> entry : nodes.entrySet()) {
            RAS_Node node = entry.getValue();
            Double totalMem = node.getTotalMemoryResources();
            Double totalCpu = node.getTotalCpuResources();
            Double usedMem = totalMem - node.getAvailableMemoryResources();
            Double usedCpu = totalCpu - node.getAvailableCpuResources();
            Double[] resources = {totalMem, totalCpu, usedMem, usedCpu};
            supervisors_resources.put(entry.getKey(), resources);
        }
        cluster.setSupervisorsResourcesMap(supervisors_resources);
    }

    public User getUser(String user) {
        return this.schedulingState.userMap.get(user);
    }

    public Map<String, User> getUserMap() {
        return this.schedulingState.userMap;
    }

    /**
     * Intialize scheduling and running queues
     *
     * @param topologies
     * @param cluster
     */
    private Map<String, User> getUsers(Topologies topologies, Cluster cluster) {
        Map<String, User> userMap = new HashMap<String, User>();
        Map<String, Map<String, Double>> userResourcePools = getUserResourcePools();
        LOG.debug("userResourcePools: {}", userResourcePools);

        for (TopologyDetails td : topologies.getTopologies()) {

            String topologySubmitter = td.getTopologySubmitter();
            //additional safety check to make sure that topologySubmitter is going to be a valid value
            if (topologySubmitter == null || topologySubmitter.equals("")) {
                LOG.error("Cannot determine user for topology {}.  Will skip scheduling this topology", td.getName());
                continue;
            }
            if (!userMap.containsKey(topologySubmitter)) {
                userMap.put(topologySubmitter, new User(topologySubmitter, userResourcePools.get(topologySubmitter)));
            }
            if (cluster.getUnassignedExecutors(td).size() > 0) {
                LOG.debug("adding td: {} to pending queue", td.getName());
                userMap.get(topologySubmitter).addTopologyToPendingQueue(td);
            } else {
                LOG.debug("adding td: {} to running queue with existing status: {}", td.getName(), cluster.getStatusMap().get(td.getId()));
                userMap.get(topologySubmitter).addTopologyToRunningQueue(td);
                if (cluster.getStatusMap().get(td.getId()) == null || cluster.getStatusMap().get(td.getId()).equals("")) {
                    cluster.setStatus(td.getId(), "Fully Scheduled");
                }
            }
        }
        return userMap;
    }

    /**
     * Get resource guarantee configs
     *
     * @return a map that contains resource guarantees of every user of the following format
     * {userid->{resourceType->amountGuaranteed}}
     */
    private Map<String, Map<String, Double>> getUserResourcePools() {
        Object raw = this.conf.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        Map<String, Map<String, Double>> ret = new HashMap<String, Map<String, Double>>();

        if (raw != null) {
            for (Map.Entry<String, Map<String, Number>> userPoolEntry : ((Map<String, Map<String, Number>>) raw).entrySet()) {
                String user = userPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for (Map.Entry<String, Number> resourceEntry : userPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }

        Map fromFile = Utils.findAndReadConfigFile("user-resource-pools.yaml", false);
        Map<String, Map<String, Number>> tmp = (Map<String, Map<String, Number>>) fromFile.get(Config.RESOURCE_AWARE_SCHEDULER_USER_POOLS);
        if (tmp != null) {
            for (Map.Entry<String, Map<String, Number>> userPoolEntry : tmp.entrySet()) {
                String user = userPoolEntry.getKey();
                ret.put(user, new HashMap<String, Double>());
                for (Map.Entry<String, Number> resourceEntry : userPoolEntry.getValue().entrySet()) {
                    ret.get(user).put(resourceEntry.getKey(), resourceEntry.getValue().doubleValue());
                }
            }
        }
        return ret;
    }

    public SchedulingState getSchedulingState() {
        return this.schedulingState;
    }

    private SchedulingState checkpointSchedulingState() {
        LOG.debug("/*********Checkpoint scheduling state************/");
        for (User user : this.schedulingState.userMap.values()) {
            LOG.debug(user.getDetailedInfo());
        }
        LOG.debug(ResourceUtils.printScheduling(this.schedulingState.cluster, this.schedulingState.topologies));
        LOG.debug("nodes:\n{}", this.schedulingState.nodes);
        LOG.debug("/*********End************/");
        return new SchedulingState(this.schedulingState);
    }

    private void restoreCheckpointSchedulingState(SchedulingState schedulingState) {
        LOG.debug("/*********restoring scheduling state************/");
        //reseting cluster
        this.schedulingState = schedulingState;
        for (User user : this.schedulingState.userMap.values()) {
            LOG.debug(user.getDetailedInfo());
        }
        LOG.debug(ResourceUtils.printScheduling(this.schedulingState.cluster, this.schedulingState.topologies));
        LOG.debug("nodes:\n{}", this.schedulingState.nodes);
        LOG.debug("/*********End************/");
    }
}

package org.apache.storm.scheduler.resource;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.junit.Test;

import java.util.*;

/**
 * Created by amir on 7/1/17.
 */
public class TestDiffMap {
    @Test
    public void doDiffMaps() {
        Map<WorkerSlot, Collection<ExecutorDetails>> oldSchedulerAssignmentMap = new HashMap<>();
        Map<WorkerSlot, Collection<ExecutorDetails>> newSchedulerAssignmentMap = new HashMap<>();
        Map<Integer, ExecutorDetails> execsMap = new HashMap<>();
        Map<String, List<Integer>> oldassignmentBlueprint = new HashMap<>();
        Map<String, List<Integer>> newassignmentBlueprint = new HashMap<>();
        int numberOfExecs = 9;


        oldassignmentBlueprint.put("node1:1027", new ArrayList<Integer>(Arrays.asList(7, 8)));
        oldassignmentBlueprint.put("node2:1030", new ArrayList<Integer>(Arrays.asList(5, 1, 2, 3, 4)));
        oldassignmentBlueprint.put("node0:1024", new ArrayList<Integer>(Arrays.asList(6, 9)));

        newassignmentBlueprint.put("node1:1027", new ArrayList<Integer>(Arrays.asList(7, 8)));
        newassignmentBlueprint.put("node2:1030", new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4)));
        newassignmentBlueprint.put("node2:1031", new ArrayList<Integer>(Arrays.asList(6)));
        newassignmentBlueprint.put("node0:1025", new ArrayList<Integer>(Arrays.asList(5)));
        newassignmentBlueprint.put("node1:1028", new ArrayList<Integer>(Arrays.asList(9)));

        fillAssignmentMap(oldassignmentBlueprint, oldSchedulerAssignmentMap);
        fillAssignmentMap(newassignmentBlueprint, newSchedulerAssignmentMap);

        Map<WorkerSlot, Collection<ExecutorDetails>> tempSchedulerAssignmentMap = new HashMap<>(newSchedulerAssignmentMap);

        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> assignment :
                oldSchedulerAssignmentMap.entrySet()) {
            //get ws of cluster not the one in assignmentMap
            WorkerSlot clusterWorker = assignment.getKey();
            if (!newSchedulerAssignmentMap.containsKey(clusterWorker))
                tempSchedulerAssignmentMap.put(clusterWorker, assignment.getValue());

        }

        for (Map.Entry<WorkerSlot, Collection<ExecutorDetails>> assignment :
                tempSchedulerAssignmentMap.entrySet()) {
            WorkerSlot clusterWorker = assignment.getKey();
            boolean newAssignment=false;
            if (newSchedulerAssignmentMap.containsKey(clusterWorker)){
                for (ExecutorDetails exec :
                        newSchedulerAssignmentMap.get(clusterWorker)) {
                    if (oldSchedulerAssignmentMap.get(clusterWorker).size() != assignment.getValue().size()){

                    }
                }
            }
        }
    }

    private void fillAssignmentMap(Map<String, List<Integer>> assignmentBlueprint, Map<WorkerSlot,
            Collection<ExecutorDetails>> schedulerAssignmentMap) {
        for (Map.Entry<String, List<Integer>> entry :
                assignmentBlueprint.entrySet()) {
            String node = entry.getKey().split(":")[0];
            int port = Integer.parseInt(entry.getKey().split(":")[1]);
            WorkerSlot ws = new WorkerSlot(node, port);
            Collection<ExecutorDetails> workerExecs = new ArrayList<>();

            for (Integer execNumber :
                    entry.getValue()) {
                workerExecs.add(new ExecutorDetails(execNumber, execNumber));
            }
            schedulerAssignmentMap.put(ws, workerExecs);

        }
    }
}

package org.apache.storm.graph.partitioner;

import org.apache.storm.graph.Graph;
import org.apache.storm.graph.Vertex;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by amir on 5/7/17.
 */
public class PartitioningResult {
    private long elapsedTime;
    private float usedMemory;
    private int iteration, bestCut;
    private List<Integer> bestLoadCPU, bestLoadMEM;
    private int[] bestSelection;
    private double bestCost;
    private Graph graph;
    private Map<Integer, Partition> partitions;
    private Map<String, Integer> execToPartitionId;

    public PartitioningResult(Graph graph, long elapsedTime, float usedMemory, int iteration,
                              List<Integer> bestLoadCPU, List<Integer> bestLoadMEM,
                              int bestCut, int[] bestSelection, double bestCost) {
        this.elapsedTime = elapsedTime;
        this.usedMemory = usedMemory;
        this.iteration = iteration;
        this.bestLoadCPU = bestLoadCPU;
        this.bestLoadMEM = bestLoadMEM;
        this.bestCut = bestCut;
        this.bestSelection = bestSelection;
        this.bestCost = bestCost;
        this.partitions = new LinkedHashMap<>();
        this.graph = graph;
        execToPartitionId = new LinkedHashMap<>();
    }

    public Graph getGraph() {
        return graph;
    }

    public void setGraph(Graph graph) {
        this.graph = graph;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    public float getUsedMemory() {
        return usedMemory;
    }

    public void setUsedMemory(float usedMemory) {
        this.usedMemory = usedMemory;
    }

    public int getIteration() {
        return iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

    public List<Integer> getBestLoadCPU() {
        return bestLoadCPU;
    }

    public void setBestLoadCPU(List<Integer> bestLoad) {
        this.bestLoadCPU = bestLoad;
    }

    public List<Integer> getBestLoadMEM() {
        return bestLoadMEM;
    }

    public void setBestLoadMEM(List<Integer> bestLoadMEM) {
        this.bestLoadMEM = bestLoadMEM;
    }

    public int getBestCut() {
        return bestCut;
    }

    public void setBestCut(int bestCut) {
        this.bestCut = bestCut;
    }

    public int[] getBestSelection() {
        return bestSelection;
    }

    public void setBestSelection(int[] bestSelection) {
        this.bestSelection = bestSelection;
    }

    public double getBestCost() {
        return bestCost;
    }

    public void setBestCost(double bestCost) {
        this.bestCost = bestCost;
    }

    public Map<Integer, Partition> getPartitions() {
        return partitions;
    }

    public Partition getPartition(Integer nodeId) {
        return partitions.get(nodeId);
    }

    public void setPartitions(Map<Integer, Partition> partitions) {
        this.partitions = partitions;
    }

    public void addVertexToPartition(int partitionId, Vertex vertex) {
        partitions.get(partitionId).getVertices().add(vertex);
        execToPartitionId.put(vertex.getExecutor().toString(), partitionId);
    }

    public void addPartition(Integer nodeId, Partition partition) {
        this.partitions.put(nodeId, partition);
        for (Vertex vertex :
                partition.getVertices()) {
            execToPartitionId.put(vertex.getExecutor().toString(), partition.getId());
        }
    }

    public int getExecuterPartitionId(String execName) {
        return execToPartitionId.get(execName);
    }
}

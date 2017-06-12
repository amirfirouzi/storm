package org.apache.storm.graph.partitioner;

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
    private List<Integer> bestLoadR1, bestLoadR2;
    private int[] bestSelection;
    private double bestCost;
    private Map<Integer, Partition> partitions;

    public PartitioningResult(long elapsedTime, float usedMemory, int iteration, List<Integer> bestLoadR1, List<Integer> bestLoadR2, int bestCut, int[] bestSelection, double bestCost) {
        this.elapsedTime = elapsedTime;
        this.usedMemory = usedMemory;
        this.iteration = iteration;
        this.bestLoadR1 = bestLoadR1;
        this.bestLoadR2 = bestLoadR2;
        this.bestCut = bestCut;
        this.bestSelection = bestSelection;
        this.bestCost = bestCost;
        this.partitions = new LinkedHashMap<>();
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

    public List<Integer> getBestLoadR1() {
        return bestLoadR1;
    }

    public void setBestLoadR1(List<Integer> bestLoad) {
        this.bestLoadR1 = bestLoad;
    }

    public List<Integer> getBestLoadR2() {
        return bestLoadR2;
    }

    public void setBestLoadR2(List<Integer> bestLoadR2) {
        this.bestLoadR2 = bestLoadR2;
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

    public void addPartition(Integer nodeId, Partition partition) {
        this.partitions.put(nodeId, partition);
    }
}

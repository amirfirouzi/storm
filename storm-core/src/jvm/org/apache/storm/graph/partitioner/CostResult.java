package org.apache.storm.graph.partitioner;

import java.util.List;

/**
 * Created by amir on 5/7/17.
 */
public class CostResult {
    private List<Integer> loadCPU;
    private List<Integer> loadMEM;
    private int crosscut = 0;
    private int[] selection;
    private double cost;

    public CostResult(List<Integer> loadCPU, List<Integer> loadMEM, int crosscut, int[] selection, double cost) {
        this.loadCPU = loadCPU;
        this.loadMEM = loadMEM;
        this.crosscut = crosscut;
        this.selection = selection;
        this.cost = cost;
    }

    public int[] getSelection() {
        return selection;
    }

    public List<Integer> getLoadCPU() {
        return loadCPU;
    }

    public List<Integer> getLoadMEM() {
        return loadMEM;
    }

    public int getCrosscut() {
        return crosscut;
    }

    public double getCost() {
        return cost;
    }
}

package org.apache.storm.graph.partitioner;

import java.util.List;

/**
 * Created by amir on 5/7/17.
 */
public class CostResult {
    private List<Integer> loadR1;
    private List<Integer> loadR2;
    private int crosscut = 0;
    private int[] selection;
    private double cost;

    public CostResult(List<Integer> loadR1, List<Integer> loadR2, int crosscut, int[] selection, double cost) {
        this.loadR1 = loadR1;
        this.loadR2 = loadR2;
        this.crosscut = crosscut;
        this.selection = selection;
        this.cost = cost;
    }

    public int[] getSelection() {
        return selection;
    }

    public List<Integer> getLoadR1() {
        return loadR1;
    }

    public List<Integer> getLoadR2() {
        return loadR2;
    }

    public int getCrosscut() {
        return crosscut;
    }

    public double getCost() {
        return cost;
    }
}

package org.apache.storm.graph.partitioner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by amir on 4/15/17.
 */
public class CostFunction {
    Map<Integer, List<Integer>> partitions;
    Model model;
    int[] selection;

    public CostFunction(Model model) {
        this.model = model;
    }

    public enum costMode {
        //ToDo: add more modes and write code to effect the behaviour of CostFunction
        BestCut, LoadBalanced;
    }

    public CostResult CalculateCost(int[] selection, costMode mode) {
        //Load Threshold for Resources type 1(CPU) & 2(RAM)
        float LTR1 = 1.0f;
        float LTR2 = 0.9f;

        int crosscut = 0;
        float violation = 0;
        this.selection = selection;
        this.partitions = new HashMap();
        //List<Integer> internal = new ArrayList();
        List<Integer> loadR1 = new ArrayList();
        List<Integer> loadR2 = new ArrayList();
        int[] capacityR1 = model.getM1();
        int[] capacityR2 = model.getM2();

        for (int i = 0; i < model.getnMachines(); i++) {
            List<Integer> partitionTasks = find(selection, i);
            partitions.put(i, partitionTasks);
            if (partitionTasks.size() != 0) {
                //internal.add(InternalCommunication(partitionTasks, model.getAdjacency()));
                loadR1.add(InternalLoad(partitionTasks, model.getR1()));
                loadR2.add(InternalLoad(partitionTasks, model.getR2()));
                crosscut += ExternalCommunication(partitionTasks, model.getAdjacency());

                //Add to Cost if selection violates the usage of Resources(more that capacity)
                if (loadR1.get(i) > capacityR1[i] * LTR1) {
                    violation += ((loadR1.get(i) / (capacityR1[i] * LTR1)) - 1) * 100;
                }
                if (loadR2.get(i) > capacityR2[i] * LTR2) {
                    violation += ((loadR2.get(i) / (capacityR2[i] * LTR2)) - 1) * 100;
                }
            } else {
                //internal.add(0);
                loadR1.add(0);
                loadR2.add(0);
            }
        }

        for (int i = 0; i < model.getnMachines(); i++) {
            List<Integer> loadDiffR1 = new ArrayList();
            List<Integer> loadDiffR2 = new ArrayList();

        }

        //ToDo: Effect of Load Imbalance between different partitions
        //compare the ratio of load between all partitions and if its balanced improve violation
        // but if it's imbalanced make some negative effects on violation

        //alpha: effect of load on cost
        float alpha = 1.5f;
        //beta: effect of crosscut on cost
        float beta = 2f;

        //ToDo: effect of InternalCommunication(calculated but not used)
        double z = (alpha * violation) + (beta * crosscut);

        return new CostResult(loadR1, loadR2, crosscut, selection, z);
    }

    private int InternalCommunication(List<Integer> partitionTasks, int[][] Adjacency) {
        int sum = 0;
//    ToDo: Can be More Optimized
        for (int i = 0; i < partitionTasks.size(); i++) {
            for (int j = i + 1; j < partitionTasks.size(); j++) {
                if (i < j) {
                    int from = partitionTasks.get(i);
                    int to = partitionTasks.get(j);
                    sum = sum + Adjacency[from][to];
                }
            }
        }
        return sum;
    }

    private int InternalLoad(List<Integer> partitionTasks, int[] Resource) {
        int load = 0;
        for (int i = 0; i < partitionTasks.size(); i++) {
            load += Resource[partitionTasks.get(i)];
        }
        return load;
    }

    private int ExternalCommunication(List<Integer> partitionTasks, int[][] Adjacency) {
        int crosscut = 0;
        //List<Integer> externalLinks = new ArrayList<>();
        //Map<Integer, Integer> nonZeros = new HashMap<>();
        for (int i = 0; i < partitionTasks.size(); i++) {
            int rowNumber = partitionTasks.get(i);
            int[] row = Adjacency[rowNumber];

            for (int j = 0; j < row.length; j++) {
                int currentElement = Adjacency[rowNumber][j];
                if (currentElement != 0 && (!partitionTasks.contains(j))) {
                    //externalLinks.add(currentElement);
                    crosscut += currentElement;
                }
            }
        }
        return crosscut;
    }

    //gets an array of ints
    //returns a collection containing nonzero elements with its position in the array(column number)
    private Map<Integer, Integer> nonZeros(int[] array) {
        Map<Integer, Integer> result = new HashMap();
        for (int i = 0; i < array.length; i++) {
            if (array[i] != 0)
                result.put(array[i], i);
        }
        return result;
    }

    private List<Integer> find(int[] array, int value) {
        List<Integer> results = new ArrayList<Integer>();
        for (int i = 0; i < array.length; i++)
            if (array[i] == value)
                results.add(i);
        return results;
    }
}

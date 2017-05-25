package org.apache.storm.graph.partitioner;

import java.util.*;

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
        BestCut, LoadBalanced, Both
    }

    public CostResult CalculateCost(int[] selection, costMode mode) {
        //Load Threshold for Resources type 1(CPU) & 2(RAM)
        float LTR1 = 1.0f;
        float LTR2 = 0.9f;

        int crosscut = 0;
        float capacityViolation = 0;
        this.selection = selection;
        this.partitions = new HashMap();
        //List<Integer> internal = new ArrayList();
        List<Integer> loadR1 = new ArrayList();
        List<Integer> loadR2 = new ArrayList();
        int[] capacityR1 = model.getM1();
        int[] capacityR2 = model.getM2();
        int disconnectedPartitions = 0;
        for (int i = 0; i < model.getnMachines(); i++) {
            List<Integer> partitionTasks = find(selection, i);
//            partitionTasks.clear();
//            partitionTasks.add(0);
//            partitionTasks.add(2);
//            partitionTasks.add(4);
//            partitionTasks.add(7);
//            partitionTasks.add(5);
//            partitionTasks.add(7);
//            partitionTasks.add(8);

            partitions.put(i, partitionTasks);
            if (partitionTasks.size() != 0) {
                boolean connected = isConnected(model.getAdjacency(), partitionTasks);
                disconnectedPartitions += (!connected ? 1 : 0);
                //internal.add(InternalCommunication(partitionTasks, model.getAdjacency()));
                loadR1.add(InternalLoad(partitionTasks, model.getR1()));
                loadR2.add(InternalLoad(partitionTasks, model.getR2()));
                if (mode != costMode.LoadBalanced)
                    crosscut += ExternalCommunication(partitionTasks, model.getAdjacency());

                //Add to Cost if selection violates the usage of Resources(more that capacity)
                if (loadR1.get(i) > capacityR1[i] * LTR1) {
                    capacityViolation += ((loadR1.get(i) / (capacityR1[i] * LTR1)) - 1) * 100;
                }
                if (loadR2.get(i) > capacityR2[i] * LTR2) {
                    capacityViolation += ((loadR2.get(i) / (capacityR2[i] * LTR2)) - 1) * 100;
                }
            } else {
                //internal.add(0);
                loadR1.add(0);
                loadR2.add(0);
            }
        }

        //ToDo: Consider effect of disconnected Vertices in partitions

        //Calculate load balancing violations
        double balancingViolationR1 = 0;
        double balancingViolationR2 = 0;

        if (mode != costMode.BestCut) {
            double avgLoadR1 = 0;
            double avgLoadR2 = 0;
            for (int i = 0; i < model.getnMachines(); i++) {
                avgLoadR1 += loadR1.get(i);
                avgLoadR2 += loadR2.get(i);
            }
            avgLoadR1 = avgLoadR1 / model.getnMachines();
            avgLoadR2 = avgLoadR2 / model.getnMachines();


            for (int i = 0; i < model.getnMachines(); i++) {
                balancingViolationR1 += Math.pow(loadR1.get(i) - avgLoadR1, 2);
                balancingViolationR2 += Math.pow(loadR2.get(i) - avgLoadR2, 2);
            }
            balancingViolationR1 = Math.sqrt(balancingViolationR1) / model.getnMachines();
            balancingViolationR2 = Math.sqrt(balancingViolationR2) / model.getnMachines();
        }
        //loadEffectRatio: effect of load (loadViolation & capacityViolation) on cost
        float loadEffectRatio = 1.5f;
        float crosscutEffectRatio = 2f;
        float disconnectivityEffect = 100f;


        if (mode == costMode.BestCut) {
            loadEffectRatio *= 0.6;
            crosscutEffectRatio *= 10;
        } else if (mode == costMode.LoadBalanced) {
            loadEffectRatio *= 3;
            crosscutEffectRatio *= 0.6;
        }

        //ToDo: effect of InternalCommunication(calculated but not used)
        double z = (loadEffectRatio * capacityViolation) + //Capacity Violation Cost
                (crosscutEffectRatio * crosscut) + //crosscut Cost
                (loadEffectRatio * ((balancingViolationR1) + (balancingViolationR2)) / 2) + //load balancing cost
                (disconnectedPartitions * disconnectivityEffect);

        return new CostResult(loadR1, loadR2, crosscut, selection, z);
    }


    public boolean isConnected(int[][] adj, List<Integer> partitionTasks) {
        Set<Integer> covered = new HashSet<>();
        List<Integer> visited = new ArrayList<>();

        covered.add(partitionTasks.get(0));
        visited.add(partitionTasks.get(0));
        for (int i = 1; i < partitionTasks.size(); i++) {
            List<Integer> neighbours = neighboursOf(adj, visited);
            if (neighbours.contains(partitionTasks.get(i))) {
                covered.addAll(neighboursOf(adj, visited));
                visited.add(partitionTasks.get(i));
            }
        }
        if (visited.size() == partitionTasks.size())
            return true;
        else
            return false;


//        Integer currentElement = partitionTasks.get(0);
//        List<Integer> neighbours = null;
//        Stack<Integer> stack = new Stack<>();
//        stack.push(currentElement);
//        int i = 0, visited = 1;
//        while ((!stack.isEmpty()) && (visited < partitionTasks.size())) {
//            neighbours = neighboursOf(adj, currentElement);
//            if (neighbours.contains(partitionTasks.get(++i))) {
//                currentElement = partitionTasks.get(i);
//                stack.push(currentElement);
//                visited++;
//            } else {
//                //stack.pop();
//                i -= 1;
//                currentElement = stack.pop();
//            }
//        }
//        if (!stack.isEmpty())
//            return true;
//        else
//        return false;
    }

    private List<Integer> neighboursOf(int[][] adj, List<Integer> elements) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < elements.size(); i++) {
            for (int j = 0; j < adj[elements.get(i)].length; j++) {
                if (elements.get(i) > j) {
                    if (adj[j][elements.get(i)] != 0)
                        result.add(j);
                } else {
                    if (adj[elements.get(i)][j] != 0)
                        result.add(j);
                }
            }
        }

        return result;
    }

    private List<Integer> neighboursOf(int[][] adj, int[] elements) {
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < elements.length; i++) {
            for (int j = 0; j < adj[elements[i]].length; j++) {
                if (elements[i] > j) {
                    if (adj[j][elements[i]] != 0)
                        result.add(j);
                } else {
                    if (adj[elements[i]][j] != 0)
                        result.add(j);
                }
            }
        }

        return result;
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

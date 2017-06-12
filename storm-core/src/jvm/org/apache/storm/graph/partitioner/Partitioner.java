package org.apache.storm.graph.partitioner;

import org.apache.storm.graph.Graph;
import org.apache.storm.graph.Vertex;
import org.apache.storm.scheduler.resource.RAS_Node;
import org.apache.storm.scheduler.resource.RAS_Nodes;
import org.apache.storm.scheduler.resource.SchedulingState;


import java.util.*;

/**
 * Created by amir on 4/15/17.
 */
public class Partitioner {
    public static PartitioningResult doPartition(CostFunction.costMode mode, boolean verbose, Graph graph, SchedulingState schedulingState) {
        long startTime = System.currentTimeMillis();

        //region get Model
        RAS_Nodes nodes = schedulingState.nodes;
        Map<Integer, RAS_Node> nodeWithIds = new LinkedHashMap<>();
        int i = 0;
        for (RAS_Node node :
                nodes.getNodes()) {
            nodeWithIds.put(i++, node);
        }
        ModelGenerator mg = new ModelGenerator();
        Model model = null;
        try {
            model = mg.generateModel(graph, schedulingState);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //endregion

        //region ACO Parameters
        int maxIt = 10;      // Maximum Number of Iterations
        int nAnt = 100;        // Number of Ants (Population Size)
        int Q = 1;

        int tau0 = 1;         // Initial Pheromone
        float alpha = 1;        // Pheromone Exponential Weight
        float beta = 0.2f;       // Heuristic Exponential Weight
        float rho = 0.05f;       // Evaporation Rate
        //endregion

        //region Initialization
        //ToDo: Heuristic Information
        // eta
        double[][] tau = new double[model.getnMachines()][model.getnTasks()];
        ones(tau);
//        INDArray tau = Nd4j.ones(model.getnMachines(), model.getnTasks());

        List<CostResult> bestResults = new ArrayList<CostResult>();
//        int[][] bestSelections = new int[maxIt][];

        CostResult costResult;
        CostResult bestCost;
        int bestAnt = -1;
        int[] ants = new int[nAnt];
        double[] antCosts = new double[nAnt];
        int[][] antSelections = new int[nAnt][model.getnTasks()];
        CostFunction costObject = new CostFunction(model);

        //endregion

        //region ACO Main Loop

        // iteration loop
        for (int it = 0; it < maxIt; it++) {
            // Move Ants
            bestCost = null;
            bestAnt = -1;
            for (int ant = 0; ant < nAnt; ant++) {
                //ToDo: empty current ant's Selection
                antSelections[ant] = new int[model.getnTasks()];
                for (int level = 0; level < model.getnTasks(); level++) {
                    // Probabilities
//                    INDArray p = Transforms.pow(tau.getColumn(level), alpha);
                    double[] p = powColumn(tau, alpha, level);
//                    p = p.div(p.sumNumber());
                    divi(p, sumOfElements(p));

                    int selectionCandidate = RouletteWheelSelection(p);
                    antSelections[ant][level] = selectionCandidate;
                }//end level Loop

                // Calculate the cost
                costResult = costObject.CalculateCost(antSelections[ant], mode);
                double cost = costResult.getCost();

                // Update the records if it improves the solution
                if (bestCost == null)
                    bestCost = new CostResult(costResult.getLoadR1(), costResult.getLoadR2(), costResult.getCrosscut(), antSelections[ant], cost);

                if (bestAnt == -1)
                    bestAnt = ant;
                antCosts[ant] = cost;
                if (cost <= bestCost.getCost()) {
                    bestAnt = ant;
                    bestCost = costResult;
                }
            }//end Ant Loop

            // Update Phromones
            // Move Ants
            int selectedMachine;
            for (int ant = 0; ant < nAnt; ant++) {
                for (int level = 0; level < model.getnTasks(); level++) {
                    selectedMachine = antSelections[ant][level];
//                    double pheremone = tau.getRow(antSelections[ant][level]).getDouble(level) + (Q / antCosts[ant]);
//                    tau.putScalar(antSelections[ant][level], level, pheremone);
                    tau[selectedMachine][level] += (Q / antCosts[ant]);
                }
            }

            // Evaporation
//            tau.muli((1 - rho));
            muli(tau, (1 - rho));

            // Store Best Cost & Best Selection
            bestResults.add(bestCost);

            // Show Iteration Information
            if (verbose)
                System.out.println("Iteration: " + it + " Selection= " + arrayToString(bestResults.get(it).getSelection())
                        + " Cost= " + bestResults.get(it).getCost());

        }//end Iteration

        //find index of bestResult(Generally)
        int index = findLatestMinimum(bestResults);

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        float usedMemory = ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (float) (1024 * 1024));
        CostResult bestOfTheBest = bestResults.get(index);

        if (verbose) {
            System.out.println("\n-------------\n");
            System.out.println("Best Answer:\n Iteration: " + index + " Selection= " + arrayToString(bestOfTheBest.getSelection()) + " Cost= " + bestOfTheBest.getCost());
            System.out.println("time: " + (totalTime));
            System.out.println("used Mem: " + usedMemory);

        }
        //endregion

        //region Create PartitioningResult Object
        PartitioningResult answer = new PartitioningResult(totalTime, usedMemory, index, bestOfTheBest.getLoadR1(), bestOfTheBest.getLoadR2(), bestOfTheBest.getCrosscut(), bestOfTheBest.getSelection(), bestOfTheBest.getCost());
        for (i = 0; i < answer.getBestSelection().length; i++) {
            int nodeId = answer.getBestSelection()[i];
            int taskId = i + 1;
            if (answer.getPartitions().get(nodeId) != null) {
                answer.getPartitions().get(nodeId).getVertices().add(graph.getVertex(taskId));
            } else {
                List<Vertex> partitionVertices = new ArrayList<>();
                partitionVertices.add(graph.getVertex(taskId));
                Partition p = new Partition(nodeWithIds.get(nodeId), partitionVertices, answer.getBestLoadR1().get(nodeId), answer.getBestLoadR2().get(nodeId));
                answer.addPartition(nodeId, p);
            }
        }
        //endregion

        return answer;
    }

//    public static int RouletteWheelSelection(INDArray p) {
//        Random rand = new Random();
//
//        float n = rand.nextFloat();
//        INDArray c = p.cumsum(0);
//        int result = p.length() - 1;
//        for (int i = 0; i < p.length(); i++) {
//            if (c.getFloat(i) >= n) {
//                result = i;
//                break;
//            }
//        }
//        return result;
//    }

    public static int RouletteWheelSelection(double[] p) {
        Random rand = new Random();

        float n = rand.nextFloat();
        double[] c = cumsum(p);
        int result = p.length - 1;
        for (int i = 0; i < p.length; i++) {
            if (c[i] >= n) {
                result = i;
                break;
            }
        }
        return result;
    }

    public static String arrayToString(int[] array) {
        String str = "";
        for (int i = 0; i < array.length; i++) {
            str += array[i] + ",";
        }
        if (str != "")
            str = str.substring(0, str.length() - 1);
        return str;
    }

    public static int[] stringToArray(String str) {
        String[] strArray = str.split(",");
        int[] intArray = new int[strArray.length];
        for (int i = 0; i < strArray.length; i++) {
            intArray[i] = Integer.parseInt(strArray[i]);
        }
        return intArray;
    }

    public static void fillArray(float[][] array, float value) {
        for (int i = 0; i < array.length; i++) {
            Arrays.fill(array[i], value);
        }
    }

    public static int findLatestMinimum(List<CostResult> array) {
        double min = -1;
        int index = -1;
        for (int i = array.size() - 1; i >= 0; i--) {
            if (array.get(i).getCost() < min || (min == -1 && index == -1)) {
                min = array.get(i).getCost();
                index = i;
            }
        }
        return index;
    }

    //region Array Operations
    public static void ones(double[][] array) {
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[0].length; j++) {
                array[i][j] = 2;
            }
        }
    }

    public static double sumOfElements(double[] array) {
        double sum = 0;
        for (int i = 0; i < array.length; i++) {
            sum += array[i];
        }
        return sum;
    }

    public static double[] powColumn(double[][] array, double value, int column) {
        double[] result = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = Math.pow(array[i][column], value);
        }
        return result;
    }

    public static void divi(double[] array, double value) {
        for (int i = 0; i < array.length; i++) {
            array[i] /= value;
        }
    }

    public static void addi(double[][] array, double value) {
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[0].length; j++) {
                array[i][j] += value;
            }
        }
    }

    public static void muli(double[][] array, double value) {
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array[0].length; j++) {
                array[i][j] *= value;
            }
        }
    }

    public static double[] cumsum(double[] array) {
        double[] result = new double[array.length];
        for (int i = 0; i < array.length; i++) {
            if ((i - 1) >= 0) {
                result[i] = array[i] + array[i - 1];
            } else {
                result[i] = array[i];
            }
        }
        return result;
    }
    //endregion

}

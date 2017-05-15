package org.apache.storm.graph.partitioner;

import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.ops.transforms.Transforms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by amir on 4/15/17.
 */
public class Partitioner {
    public static PartitioningResult doPartition(Model model,CostFunction.costMode mode, boolean verbose) {
        long startTime = System.currentTimeMillis();

        //region ACO Parameters
        int maxIt = 20;      // Maximum Number of Iterations
        int nAnt = 50;        // Number of Ants (Population Size)
        int Q = 1;

        int tau0 = 1;         // Initial Pheromone
        float alpha = 1;        // Pheromone Exponential Weight
        float beta = 0.2f;       // Heuristic Exponential Weight
        float rho = 0.05f;       // Evaporation Rate
        //endregion

        //region Initialization
        //ToDo: Heuristic Information
        // eta
//    float[][] tau = new float[model.getnMachines()][model.getnTasks()];
        INDArray tau = Nd4j.ones(model.getnMachines(), model.getnTasks());
//    fillArray(tau, 1);
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
                    INDArray p = Transforms.pow(tau.getColumn(level), alpha);
                    p = p.div(p.sumNumber());

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
            for (int ant = 0; ant < nAnt; ant++) {
                for (int level = 0; level < model.getnTasks(); level++) {
                    double pheremone = tau.getRow(antSelections[ant][level]).getDouble(level) + (Q / antCosts[ant]);
                    tau.putScalar(antSelections[ant][level], level, pheremone);
                }
            }

            // Evaporation
            tau.muli((1 - rho));

            // Store Best Cost & Best Selection
            bestResults.add(bestCost);
//            bestSelections[it] = antSelections[bestAnt];

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
        PartitioningResult answer = new PartitioningResult(totalTime,usedMemory, index, bestOfTheBest.getLoadR1(), bestOfTheBest.getLoadR2(), bestOfTheBest.getCrosscut(), bestOfTheBest.getSelection(), bestOfTheBest.getCost());
        if (verbose) {
            System.out.println("\n-------------\n");
            System.out.println("Best Answer:\n Iteration: " + index + " Selection= " + arrayToString(bestOfTheBest.getSelection()) + " Cost= " + bestOfTheBest.getCost());
            System.out.println("time: " + (totalTime));
            System.out.println("used Mem: " + usedMemory);

        }
        //endregion
        return answer;
    }

    public static int RouletteWheelSelection(INDArray p) {
        Random rand = new Random();

        float n = rand.nextFloat();
        INDArray c = p.cumsum(0);
        int result = p.length() - 1;
        for (int i = 0; i < p.length(); i++) {
            if (c.getFloat(i) >= n) {
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
}

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
package org.apache.storm.graph;


import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.TopologyDetails;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by amir on 11/21/16.
 */
public final class TopologyGraphBuilder {

    private static Map<String, Integer> parallelismMap = new HashMap<>();

    private TopologyGraphBuilder() {
        //not called
    }

    public static Graph buildGraph(TopologyDetails td) {
        calculateParallelismMap(td);

        Map<String, List<ExecutorDetails>> compToExecsMap = reverseExecstoCompMap(td);
        Graph g = new Graph();
        //Add Vertices for Spouts
        ExecutorEntity currentExecutor;
        ExecutorDetails exec;
        int spoutParallelism;
        Resource resource = new Resource();
        for (Map.Entry<String, SpoutSpec> spout
                : td.getTopology().get_spouts().entrySet()) {
            spoutParallelism = spout.getValue().get_common().get_parallelism_hint();
            for (int i = 1; i <= spoutParallelism; i++) {
                exec = compToExecsMap.get(spout.getKey()).get(i - 1);
                currentExecutor = new ExecutorEntity(spout.getKey(), Integer.toString(i), exec);

                int reqCPU = td.getTotalCpuReqTask(exec).intValue();
                int reqMEM = td.getTotalMemReqTask(exec).intValue();

                resource.setCpu(reqCPU);
                resource.setMemory(reqMEM);

                g.addVertex(currentExecutor, resource);
                g.addExecutor(exec);
            }
        }

        ExecutorEntity fromExecutor;
        int boltParallelism;
        List<String> compExecs = new ArrayList<>();
        //Add Vertices for Bolts, then Add Edges Between Components(spouts->bolts and bolts->bolts)
        //and their sub-tasks(considering number of instances)
        for (Map.Entry<String, Bolt> bolt
                : td.getTopology().get_bolts().entrySet()) {
            boltParallelism = bolt.getValue().get_common().get_parallelism_hint();


            for (int i = 1; i <= boltParallelism; i++) {
                exec = compToExecsMap.get(bolt.getKey()).get(i - 1);
                currentExecutor = new ExecutorEntity(bolt.getKey(), Integer.toString(i), exec);

                resource = new Resource();
                int reqCPU = td.getTotalCpuReqTask(exec).intValue();
                int reqMEM = td.getTotalMemReqTask(exec).intValue();
                resource.setCpu(reqCPU);
                resource.setMemory(reqMEM);
                g.addVertex(currentExecutor, resource);
                g.addExecutor(exec);
                for (GlobalStreamId input
                        : bolt.getValue().get_common().get_inputs().keySet()) {

                    int sourceParallelism = getComponentParallelism(input.get_componentId());
                    for (int j = 1; j <= sourceParallelism; j++) {
                        fromExecutor = new ExecutorEntity(input.get_componentId(),
                                Integer.toString(j),
                                compToExecsMap.get(input.get_componentId()).get(j - 1));
                        g.addEdge(fromExecutor, currentExecutor);
                    }
                }
            }
        }

        return g;
        //generateMetisInputFile(g, "metis", false, false, false, 0);
//    int numOfPartitions = 3;
        //getMetisPartitions(g, "metis", numOfPartitions);
    }

    private static Map<String, List<ExecutorDetails>> reverseExecstoCompMap(TopologyDetails td) {
        Map<String, List<ExecutorDetails>> compToExecsMap = new HashMap<>();
        for (Map.Entry<ExecutorDetails, String> exec :
                td.getExecutorToComponent().entrySet()) {
            if (compToExecsMap.containsKey(exec.getValue())) {
                compToExecsMap.get(exec.getValue().toString()).add(exec.getKey());
            } else {
                compToExecsMap.put(exec.getValue(), new ArrayList<>());
                compToExecsMap.get(exec.getValue()).add(exec.getKey());
            }
        }
        return compToExecsMap;
    }

    private static List<String> getExecsOfComponent(TopologyDetails td, String compName) {

        List<String> compExecs = new ArrayList<>();
        for (ExecutorDetails exec :
                td.getExecutorToComponent().keySet()) {
            if (td.getExecutorToComponent().get(exec).equalsIgnoreCase(compName)) {
                compExecs.add(exec.toString());
            }
        }
        return compExecs;
    }

    private static void calculateParallelismMap(TopologyDetails td) {
        for (Map.Entry<String, SpoutSpec> spout
                : td.getTopology().get_spouts().entrySet()) {
            parallelismMap.put(spout.getKey(), spout.getValue().get_common().get_parallelism_hint());
        }

        for (Map.Entry<String, Bolt> bolt
                : td.getTopology().get_bolts().entrySet()) {
            parallelismMap.put(bolt.getKey(), bolt.getValue().get_common().get_parallelism_hint());
        }
    }

    private static int getComponentParallelism(String componentName) {
        return parallelismMap.getOrDefault(componentName, 0);
    }

    /**
     * Writes the input data required for METIS and Returns the data as a String
     */

//  public static String generateMetisInputFile(Graph graph, String fileName, boolean fmtVertexSize,
//                                              boolean fmtVertexWeight, boolean fmtEdgeWeight, int ncon) {
//    String fmt = Integer.toString(fmtVertexSize ? 1 : 0)
//        + Integer.toString(fmtVertexWeight ? 1 : 0)
//        + Integer.toString(fmtEdgeWeight ? 1 : 0);
//
//    String stormHome = System.getProperty("user.home") + "/.stormdata";
//    String dir = stormHome + "/output/";
//    String file = dir + fileName;
//
//    File directory = new File(dir);
//    if (!directory.exists()) {
//      directory.mkdirs();
//    }
//
//    StringBuilder line = new StringBuilder();
//    try {
//
//      FileWriter out = new FileWriter(file);
//      Set<Map.Entry<Vertex, TreeSet<Vertex>>> adjListEntries = graph.getAdjList().entrySet();
//
//      //write header info (#v #e fmt ncon)
//      line.append("%------header------\n")
//          .append(graph.numVertices() + " ")
//          .append(graph.numEdges() + " ")
//          .append(fmt + " ")
//          .append(ncon)
//          .append("\n");
//      for (Map.Entry<Vertex, TreeSet<Vertex>> adjListRow
//          : adjListEntries) {
//        Vertex vertex = adjListRow.getKey();
//        TreeSet<Vertex> neighbours = adjListRow.getValue();
//        //write a comment to show the current(i'th) vertex name and it's weights
//        //%--------#VName(#weights)--------
//        line.append("%")
//            .append("--------")
//            .append(vertex.getName())
//            .append(vertex.getWeightsString(ncon))
//            .append("--------" + "\n");
//        //write each line: the weights of current vertex and neighbours + edge weights(if exist)
//        //#weights #neighbour1 #edgeWeight #neighbour2 #edgeWeight ...
//        line.append(graph.getNeighboursOf(vertex, neighbours, fmtEdgeWeight, ncon) + "\n")
//            .append("%" + graph.getNeighbourNamesOf(vertex, neighbours, fmtEdgeWeight) + "\n");
//      }
//
//      line.append("\n%------names------\n");
//      for (Map.Entry<Vertex, TreeSet<Vertex>> row
//          : adjListEntries) {
//        line.append("%")
//            .append(row.getKey().getId())
//            .append("->")
//            .append(row.getKey().getName())
//            .append("\n");
//      }
//
//      out.write(line.toString());
//      out.close();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//
//    return line.toString();
//  }
    public static String getMetisPartitions(Graph graph, String fileName, int numOfPartitions) {
        //ToDo: check the output and stops execution if errors occured
        String stormHome = System.getProperty("user.home") + "/.stormdata";
        String dir = stormHome + "/output/";
        String metisInputFile = dir + fileName;
        String metisOutputFile = dir + fileName + ".part." + numOfPartitions;

        List<Integer> metisOutput = new ArrayList<>();
        //String metisOutput = "";
        try {
            callMetis(numOfPartitions, metisInputFile);
            metisOutput = readFileList(metisOutputFile, Charset.defaultCharset());
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<Integer> items = new ArrayList<Integer>();
        HashMap<Integer, List<Vertex>> partitions = new HashMap<>();
        //need newPartitions because items in metisOutput list may be non contiguous
        int newPartitions = 0;

        for (int i = 0; i < metisOutput.size(); i++) {
            if (!items.contains(metisOutput.get(i))) {
                int index = ++newPartitions;
                items.add(metisOutput.get(i));
                List<Vertex> partitionVertices = new ArrayList<>();
                partitionVertices.add(graph.getVertex(i + 1));
                partitions.put(index, partitionVertices);
            } else {
                int index = items.indexOf(metisOutput.get(i)) + 1;
                partitions.get(index).add(graph.getVertex(i + 1));
            }
        }
        return metisOutputFile;
    }


    private static boolean callMetis(int numOfPartitions, String fileName) {
        //ToDo: read output and check for errors
        try {
            Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", "gpmetis " + fileName + " " + numOfPartitions});
            int retValue = p.waitFor();
            return (retValue == 0);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    private static String readFileString(String path, Charset encoding)
            throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    private static List<Integer> readFileList(String path, Charset encoding)
            throws IOException {
        List<Integer> numbers = new ArrayList<>();
        for (String line : Files.readAllLines(Paths.get(path))) {
            for (String part : line.split("\\s+")) {
                Integer i = Integer.valueOf(part);
                numbers.add(i);
            }
        }
        return numbers;
    }
}

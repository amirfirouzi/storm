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
import org.apache.storm.generated.StormTopology;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by amir on 11/21/16.
 */
public final class TopologyGraphBuilder {

    private static Map<String, Integer> parallelismMap = new HashMap<>();

    private TopologyGraphBuilder() {
        //not called
    }

    public static void buildGraph(StormTopology topology) {
        calculateParallelismMap(topology);
        Graph g = new Graph();
        //Add Vertices for Spouts
        int spoutParallelism;
        for (Map.Entry<String, SpoutSpec> spout
                : topology.get_spouts().entrySet()) {
            spoutParallelism = spout.getValue().get_common().get_parallelism_hint();
            for (int i = 1; i <= spoutParallelism; i++) {
                g.addVertex(spout.getKey() + "-" + Integer.toString(i));
            }
        }

        int boltParallelism;
        //Add Vertices for Bolts, then Add Edges Between Components(spouts->bolts and bolts->bolts)
        //and their sub-tasks(considering number of instances)
        for (Map.Entry<String, Bolt> bolt
                : topology.get_bolts().entrySet()) {
            boltParallelism = bolt.getValue().get_common().get_parallelism_hint();
            for (int i = 1; i <= boltParallelism; i++) {
                g.addVertex(bolt.getKey() + "-" + Integer.toString(i));
                for (GlobalStreamId input
                        : bolt.getValue().get_common().get_inputs().keySet()) {

                    int sourceParallelism = getComponentParallelism(input.get_componentId());
                    for (int j = 1; j <= sourceParallelism; j++) {
                        g.addEdge(input.get_componentId()
                                + "-" + Integer.toString(j), bolt.getKey()
                                + "-" + Integer.toString(i));
                    }
                }
            }
        }

        generateMetisInputFile(g, "metis", false, false, false, 0);
    }

    private static void calculateParallelismMap(StormTopology topology) {
        for (Map.Entry<String, SpoutSpec> spout
                : topology.get_spouts().entrySet()) {
            parallelismMap.put(spout.getKey(), spout.getValue().get_common().get_parallelism_hint());
        }

        for (Map.Entry<String, Bolt> bolt
                : topology.get_bolts().entrySet()) {
            parallelismMap.put(bolt.getKey(), bolt.getValue().get_common().get_parallelism_hint());
        }
    }

    private static int getComponentParallelism(String componentName) {
        return parallelismMap.getOrDefault(componentName, 0);
    }

    /**
     * Writes the input data required for METIS and Returns the data as a String
     */
    public static String generateMetisInputFile(Graph graph, String fileName, boolean fmtVertexSize,
                                                boolean fmtVertexWeight, boolean fmtEdgeWeight, int ncon) {
        String fmt = Integer.toString(fmtVertexSize ? 1 : 0)
                + Integer.toString(fmtVertexWeight ? 1 : 0)
                + Integer.toString(fmtEdgeWeight ? 1 : 0);

        String stormHome = System.getProperty("user.home") + "/.stormdata";
        String dir = stormHome + "/output/";
        String file = dir + fileName;

        File directory = new File(dir);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        StringBuilder line = new StringBuilder();
        try {

            FileWriter out = new FileWriter(file);
            Set<Map.Entry<Vertex, TreeSet<Vertex>>> adjListEntries = graph.getAdjList().entrySet();

            //write header info (#v #e fmt ncon)
            line.append("%------header------\n")
                    .append(graph.numVertices() + " ")
                    .append(graph.numEdges() + " ")
                    .append(fmt + " ")
                    .append(ncon)
                    .append("\n");
            for (Map.Entry<Vertex, TreeSet<Vertex>> adjListRow
                    : adjListEntries) {
                Vertex vertex = adjListRow.getKey();
                TreeSet<Vertex> neighbours = adjListRow.getValue();
                //write a comment to show the current(i'th) vertex name and it's weights
                //%--------#VName(#weights)--------
                line.append("%")
                        .append("--------")
                        .append(vertex.getName())
                        .append(vertex.getWeightsString(ncon))
                        .append("--------" + "\n");
                //write each line: the weights of current vertex and neighbours + edge weights(if exist)
                //#weights #neighbour1 #edgeWeight #neighbour2 #edgeWeight ...
                line.append(graph.getNeighboursOf(vertex, neighbours, fmtEdgeWeight, ncon) + "\n")
                        .append("%" + graph.getNeighbourNamesOf(vertex, neighbours, fmtEdgeWeight) + "\n");
            }

            line.append("\n%------names------\n");
            for (Map.Entry<Vertex, TreeSet<Vertex>> row
                    : adjListEntries) {
                line.append("%")
                        .append(row.getKey().getId())
                        .append("->")
                        .append(row.getKey().getName())
                        .append("\n");
            }

            out.write(line.toString());
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return line.toString();
    }
}

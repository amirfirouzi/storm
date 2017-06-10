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

import org.apache.storm.scheduler.ExecutorDetails;

import java.util.LinkedHashMap;
import java.util.TreeSet;

public class Graph {
    private static final TreeSet<Vertex> EMPTY_SET = new TreeSet<Vertex>();
    private LinkedHashMap<Vertex, TreeSet<Vertex>> adjList;
    private LinkedHashMap<String, Vertex> vertices;
    private LinkedHashMap<String, ExecutorDetails> executors;
    private LinkedHashMap<Integer, String> verticesIds;
    private LinkedHashMap<String, String> execsTovertices;


    private LinkedHashMap<String, Edge> edges;
    private int numOfVertices;
    private int numOfEdges;

    public Graph() {
        adjList = new LinkedHashMap<Vertex, TreeSet<Vertex>>();
        vertices = new LinkedHashMap<String, Vertex>();
        executors = new LinkedHashMap<>();
        verticesIds = new LinkedHashMap<Integer, String>();
        execsTovertices = new LinkedHashMap<String, String>();
        edges = new LinkedHashMap<String, Edge>();
        numOfVertices = numOfEdges = 0;

    }

    public LinkedHashMap<Vertex, TreeSet<Vertex>> getAdjList() {
        return adjList;
    }

    public Vertex addVertex(ExecutorEntity exec) {
        Vertex v;
        String name = exec.getComponentyName() + "-" + exec.getInstanceId();
        v = vertices.get(name);
        if (v == null) {
            v = new Vertex(name, exec.getExecutor());
            numOfVertices += 1;
            v.setId(numOfVertices);
            v.setExecutor(exec.getExecutor());
            vertices.put(name, v);
            executors.put(exec.getExecutor().toString(), exec.getExecutor());
            verticesIds.put(numOfVertices, name);
            execsTovertices.put(exec.getExecutorName(), name);
            adjList.put(v, new TreeSet<Vertex>());

        }
        return v;
    }

    public Vertex addVertex(ExecutorEntity exec, Resource weights) {
        Vertex v;
        String vertexName = exec.getComponentyName() + "-" + exec.getInstanceId();
        v = vertices.get(vertexName);
        if (v == null) {
            v = new Vertex(vertexName, exec.getExecutor(), weights);
            numOfVertices += 1;
            v.setId(numOfVertices);
            v.setExecutor(exec.getExecutor());
            vertices.put(vertexName, v);
            executors.put(exec.getExecutor().toString(), exec.getExecutor());
            verticesIds.put(numOfVertices, vertexName);
            String execName = exec.getExecutor().toString();
            execsTovertices.put(execName, vertexName);
            adjList.put(v, new TreeSet<Vertex>());

        }
        return v;
    }

    public Vertex getVertex(String name) {
        return vertices.get(name);
    }

    public Vertex getVertexFromExecutor(String execName) {
        return vertices.get(execsTovertices.get(execName));
    }

    public Vertex getVertex(Integer index) {
        return vertices.get(verticesIds.get(index));
    }

    public boolean hasVertex(String name) {
        return vertices.containsKey(name);
    }

    public boolean hasEdge(String from, String to) {

        if (!hasVertex(from) || !hasVertex(to)) {
            return false;
        }
        String edgeName = from + ">" + to;
        return adjList.get(vertices.get(from)).contains(vertices.get(to));
    }

    public Edge getEdge(String from, String to) {
        String edgeName = from + ">" + to;
        return edges.get(edgeName);
    }

    public Edge getEdgeFromExecutor(String from, String to) {
        Vertex vFrom = getVertexFromExecutor(from);
        Vertex vTo = getVertexFromExecutor(to);
        return getEdge(vFrom.getName(), vTo.getName());

    }

    public Edge addEdge(ExecutorEntity execFrom, ExecutorEntity execTo) {
        Vertex src;
        Vertex dest;
        String from = execFrom.getComponentyName() + "-" + execFrom.getInstanceId();
        String to = execTo.getComponentyName() + "-" + execTo.getInstanceId();
        Edge edge = getEdge(from, to);
        if (edge != null) {
            return edge;
        }
        edge = new Edge(from, to);
        edges.put(edge.getName(), edge);
        numOfEdges += 1;

        src = getVertex(from);
        if (src == null) {
            src = addVertex(execFrom);
        }
        dest = getVertex(to);
        if (dest == null) {
            dest = addVertex(execTo);
        }

        adjList.get(src).add(dest);
        adjList.get(dest).add(src);

        return edge;
    }

    public Iterable<Vertex> adjacentTo(String name) {
        if (!hasVertex(name)) {
            return EMPTY_SET;
        }
        return adjList.get(getVertex(name));
    }

    public Iterable<Vertex> getVertices() {
        return vertices.values();
    }

    public LinkedHashMap<Integer, String> getVerticesIds() {
        return verticesIds;
    }

    public int numVertices() {
        return numOfVertices;
    }

    public int numEdges() {
        return numOfEdges;
    }

    public String toString() {
        String str = "";
        for (Vertex v : this.getVertices()) {
            str += String.format("%s%s: ", v, v.getWeightsString());

            for (Vertex w : this.adjacentTo(v.getName())) {
                Edge e = this.getEdge(v.getName(), w.getName());
                String ew = "";
                if (e != null) {
                    ew = e.getWeightString();
                }
                str += String.format("%s->%s%s", ew, w, w.getWeightsString());
            }
            str += "\n";
        }
        return str;
    }

    public TreeSet<Vertex> getNeighbours(Vertex vertex) {
        return adjList.get(vertex);
    }

    public String getNeighboursOf(Vertex currentVertex, TreeSet<Vertex> neighbours,
                                  boolean fmtEdgeWeight, int ncon) {
        StringBuilder line = new StringBuilder();
        line.append(currentVertex.getWeightsString() + " ");
        Edge edge;
        for (Vertex vertex
                : neighbours) {
            edge = getEdge(currentVertex.getName(), vertex.getName());
            if (edge == null) {
                edge = getEdge(vertex.getName(), currentVertex.getName());
            }
            line.append(vertex.getId() + " ");
            if (fmtEdgeWeight) {
                line.append(edge.getWeightString(false) + " ");
            }
        }
        return line.toString().trim();
    }

    public String getNeighbourNamesOf(Vertex currentVertex, TreeSet<Vertex> neighbours,
                                      boolean fmtEdgeWeight) {
        StringBuilder line = new StringBuilder();
        Edge edge;
        for (Vertex vertex
                : neighbours) {
            edge = getEdge(currentVertex.getName(), vertex.getName());
            if (edge == null) {
                edge = getEdge(vertex.getName(), currentVertex.getName());
            }

            if (fmtEdgeWeight) {
                line.append(edge.getWeightString() + "->");
            }
            line.append(vertex.getName() + " ");

        }
        return line.toString().trim();
    }

    private String escapedVersion(String s) {
        return "\'" + s + "\'";

    }

    public LinkedHashMap<String, ExecutorDetails> getExecutors() {
        return executors;
    }

    public void setExecutors(LinkedHashMap<String, ExecutorDetails> executors) {
        this.executors = executors;
    }

    public void addExecutor(ExecutorDetails executor) {
        this.executors.put(executor.toString(), executor);
    }
}

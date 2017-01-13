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

import java.util.LinkedHashMap;
import java.util.TreeSet;

public class Graph {
  private static final TreeSet<Vertex> EMPTY_SET = new TreeSet<Vertex>();
  private LinkedHashMap<Vertex, TreeSet<Vertex>> adjList;
  private LinkedHashMap<String, Vertex> vertices;
  private LinkedHashMap<String, Edge> edges;
  private int numOfVertices;
  private int numOfEdges;

  public Graph() {
    adjList = new LinkedHashMap<Vertex, TreeSet<Vertex>>();
    vertices = new LinkedHashMap<String, Vertex>();
    edges = new LinkedHashMap<String, Edge>();
    numOfVertices = numOfEdges = 0;

  }

  public LinkedHashMap<Vertex, TreeSet<Vertex>> getAdjList() {
    return adjList;
  }

  /**
   * Add a new vertex name with no neighbors (if vertex does not yet exist)
   *
   * @param name vertex to be added
   */
  public Vertex addVertex(String name) {
    Vertex v;
    v = vertices.get(name);
    if (v == null) {
      v = new Vertex(name);
      numOfVertices += 1;
      v.setId(numOfVertices);
      vertices.put(name, v);
      adjList.put(v, new TreeSet<Vertex>());

    }
    return v;
  }

  /**
   * Returns the Vertex matching v
   *
   * @param name a String name of a Vertex that may be in
   *             this Graph
   * @return the Vertex with a name that matches v or null
   * if no such Vertex exists in this Graph
   */
  public Vertex getVertex(String name) {
    return vertices.get(name);
  }

  /**
   * Returns true iff v is in this Graph, false otherwise
   *
   * @param name a String name of a Vertex that may be in
   *             this Graph
   * @return true iff v is in this Graph
   */
  public boolean hasVertex(String name) {
    return vertices.containsKey(name);
  }

  /**
   * Is from-to, an edge in this Graph. The graph is
   * undirected so the order of from and to does not
   * matter.
   *
   * @param from the name of the first Vertex
   * @param to   the name of the second Vertex
   * @return true iff from-to exists in this Graph
   */
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

  /**
   * Add to to from's set of neighbors, and add from to to's
   * set of neighbors. Does not add an edge if another edge
   * already exists
   *
   * @param from the name of the first Vertex
   * @param to   the name of the second Vertex
   */
  public Edge addEdge(String from, String to) {
    Vertex src;
    Vertex dest;
    Edge edge = getEdge(from, to);
    if (edge != null) {
      return edge;
    }
    edge = new Edge(from, to);
    edges.put(edge.getName(), edge);
    numOfEdges += 1;

    src = getVertex(from);
    if (src == null) {
      src = addVertex(from);
    }
    dest = getVertex(to);
    if (dest == null) {
      dest = addVertex(to);
    }

    adjList.get(src).add(dest);
    adjList.get(dest).add(src);

    return edge;
  }

  /**
   * Return an iterator over the neighbors of Vertex named v
   *
   * @param name the String name of a Vertex
   * @return an Iterator over Vertices that are adjacent
   * to the Vertex named v, empty set if v is not in graph
   */
  public Iterable<Vertex> adjacentTo(String name) {
    if (!hasVertex(name)) {
      return EMPTY_SET;
    }
    return adjList.get(getVertex(name));
  }

  /**
   * Returns an Iterator over all Vertices in this Graph
   *
   * @return an Iterator over all Vertices in this Graph
   */
  public Iterable<Vertex> getVertices() {
    return vertices.values();
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

  public String getNeighboursOf(Vertex currentVertex, TreeSet<Vertex> neighbours,
                                boolean fmtEdgeWeight, int ncon) {
    StringBuilder line = new StringBuilder();
    line.append(currentVertex.getWeightsString(ncon, false) + " ");
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
}

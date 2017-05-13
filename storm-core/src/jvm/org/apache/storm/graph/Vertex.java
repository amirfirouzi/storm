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

import java.util.HashMap;
import java.util.Map;

public class Vertex implements Comparable<Vertex> {
  /**
   * label for Vertex
   */
  private String name;
  private String execName;
  private int id;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getExecName() {
    return execName;
  }

  public void setExecName(String execName) {
    this.execName = execName;
  }

  /**
   * length of shortest path from source
   */
  public int distance;

  /**
   * weights of vertex
   */
  private Map<String, Double> weights;

  public Vertex predecessor; // previous vertex

  /**
   * a measure of the structural importance of a vertex.
   * The value should initially be set to zero. A higher
   * centrality score should mean a Vertex is more central.
   */
  private double centrality;
  /**
   * Infinite distance indicates that there is no path
   * from the source to this vertex
   */
  public static final int INFINITY = Integer.MAX_VALUE;

  public Vertex(String v, String execName) {
    name = v;
    this.execName = execName;
    distance = INFINITY; // start as infinity away
    predecessor = null;
    centrality = 0.0;
    weights = new HashMap<String, Double>();
    ;
  }

  public Vertex(String v, String execName, Map<String, Double> weights) {
    name = v;
    this.execName = execName;
    distance = INFINITY; // start as infinity away
    predecessor = null;
    centrality = 0.0;
    this.weights = weights;
  }

  /**
   * The name of the Vertex is assumed to be unique, so it
   * is used as a HashCode
   *
   * @see Object#hashCode()
   */
  public int hashCode() {
    return name.hashCode();
  }

  public void addWeights(Map<String, Double> vertexWeights) {
    this.weights.putAll(vertexWeights);
  }

  public Map<String, Double> getWeights() {
    return this.weights;
  }

  public String getWeightsString() {
    String vw = "";
    if (!weights.isEmpty()) {
      vw = "(";
    }
    for (Map.Entry s : this.weights.entrySet()) {
      vw += s.getKey() + ": " + s.getValue() + "\n";
    }
//    if (!vw.isEmpty()) {
//      vw = vw.substring(0, vw.length() - 1) + ")";
//    }
    return vw;
  }

  public String toString() {
    return name;
  }

  /**
   * Compare on the basis of distance from source first and
   * then lexicographically
   */
  public int compareTo(Vertex other) {
    int diff = distance - other.distance;
    if (diff != 0) {
      return diff;
    } else {
      return name.compareTo(other.name);
    }
  }
}


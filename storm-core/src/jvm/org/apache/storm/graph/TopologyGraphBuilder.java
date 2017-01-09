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

import java.util.HashMap;
import java.util.Map;

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

          int sourceParallelism = getParallelism(input.get_componentId());
          for (int j = 1; j <= sourceParallelism; j++) {
            g.addEdge(input.get_componentId()
                + "-" + Integer.toString(j), bolt.getKey()
                + "-" + Integer.toString(i));
          }
        }
      }
    }

    g.writeMetis("metis", false, false, false, 0);
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

  private static int getParallelism(String componentName){
      return parallelismMap.getOrDefault(componentName,0);
  }

//  private static Object getComponent(String componentName, TopologyAPI.Topology.Builder builder) {
//    for (TopologyAPI.SpoutOrBuilder spout
//        : builder.getSpoutsList()) {
//      if (spout.getComp().getName().equals(componentName)) {
//        return spout;
//      }
//    }
//    for (TopologyAPI.BoltOrBuilder bolt
//        : builder.getBoltsList()) {
//      if (bolt.getComp().getName().equals(componentName)) {
//        return bolt;
//      }
//    }
//    return null;
//  }
}

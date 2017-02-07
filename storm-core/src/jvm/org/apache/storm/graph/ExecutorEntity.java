package org.apache.storm.graph;

/**
 * Created by amir on 2/7/17.
 */
public class ExecutorEntity {
  private String componentyName;
  private String instanceId;
  private String executorName;

  public ExecutorEntity(String componentyName, String instanceId, String executorName) {
    this.componentyName = componentyName;
    this.instanceId = instanceId;
    this.executorName = executorName;
  }

  public String getComponentyName() {
    return componentyName;
  }

  public void setComponentyName(String componentyName) {
    this.componentyName = componentyName;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getExecutorName() {
    return executorName;
  }

  public void setExecutorName(String executorName) {
    this.executorName = executorName;
  }
}

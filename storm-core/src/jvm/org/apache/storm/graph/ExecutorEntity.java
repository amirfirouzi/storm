package org.apache.storm.graph;

import org.apache.storm.scheduler.ExecutorDetails;

/**
 * Created by amir on 2/7/17.
 */
public class ExecutorEntity {
    private String componentName;
    private String instanceId;
    private ExecutorDetails executor;

    public ExecutorEntity(String componentName, String instanceId, ExecutorDetails executor) {
        this.componentName = componentName;
        this.instanceId = instanceId;
        this.executor = executor;
    }

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public ExecutorDetails getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorDetails executor) {
        this.executor = executor;
    }
}

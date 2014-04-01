package org.modeshape.jcr.brokenfolder;

/**
 * @author evgeniy.shevchenko
 * @version 1.0 3/26/14
 */

public class NodeTaskResult {
    private String nodeId;
    private String nodeContentId;
    private String nodeName;

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeContentId() {
        return nodeContentId;
    }

    public void setNodeContentId(String nodeContentId) {
        this.nodeContentId = nodeContentId;
    }
}

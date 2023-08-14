package com.overload;

public class Message {
    private final String type;
    private int destinationId;

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    private int nodeId;

    public Message(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public int getDestinationId() {
        return destinationId;
    }

    @Override
    public String toString() {
        return "Message{" +
                "type='" + type + '\'' +
                ", destinationId=" + destinationId +
                ", nodeId=" + nodeId +
                '}';
    }

    public void setDestinationId(int destinationId) {
        this.destinationId = destinationId;
    }
}
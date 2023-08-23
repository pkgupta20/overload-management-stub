package com.overload.threadpool;

import java.util.UUID;

public class Message {

    private UUID uuid;
    private  String type;
    private int destinationId;
    private int nodeId;

    private long initTime;

    private long timeSpentInDDRS;

    private long totalTimeInProcessing;
    public Message(){

    }

    public Message(UUID uuid, String type, long initTime) {
        this.uuid = uuid;
        this.type = type;
        this.initTime = initTime;
    }

    public String getType() {
        return type;
    }

    public void setType(String type){
        this.type = type;
    }

    public int getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(int destinationId) {
        this.destinationId = destinationId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }


    public long getInitTime() {
        return initTime;
    }

    public void setInitTime(long initTime) {
        this.initTime = initTime;
    }

    public long getTimeSpentInDDRS() {
        return timeSpentInDDRS;
    }

    public void setTimeSpentInDDRS(long timeSpentInDDRS) {
        this.timeSpentInDDRS = timeSpentInDDRS;
    }



    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    public long getTotalTimeInProcessing() {
        return totalTimeInProcessing;
    }

    public void setTotalTimeInProcessing(long totalTimeInProcessing) {
        this.totalTimeInProcessing = totalTimeInProcessing;
    }

    @Override
    public String toString() {
        return "Message{" +
                "uuid=" + uuid +
                ", type='" + type + '\'' +
                ", destinationId=" + destinationId +
                ", nodeId=" + nodeId +
                ", initTime=" + initTime +
                ", timeSpentInDDRS=" + timeSpentInDDRS +
                ", totalTimeInProcessing=" + totalTimeInProcessing +
                '}';
    }
}
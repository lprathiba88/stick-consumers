package io.logbase.stick.kinesis.consumer.locationsdistance.models;

public class DailyDistance {
  
  private double distance;
  private double prevLat;
  private double prevLong;
  private long prevTimestamp;
  private boolean running;
  private String currentTripID;
  
  public DailyDistance(double distance, double prevLat, double prevLong, long prevTimestamp, boolean running, String currentTripID) {
    this.distance = distance;
    this.prevLat = prevLat;
    this.prevLong = prevLong;
    this.prevTimestamp = prevTimestamp;
    this.running = running;
    this.currentTripID = currentTripID;
  }
  
  public double getDistance() {
    return distance;
  }
  public void setDistance(double distance) {
    this.distance = distance;
  }
  public double getPrevLat() {
    return prevLat;
  }
  public void setPrevLat(double prevLat) {
    this.prevLat = prevLat;
  }
  public double getPrevLong() {
    return prevLong;
  }
  public void setPrevLong(double prevLong) {
    this.prevLong = prevLong;
  }
  public long getPrevTimestamp() {
    return prevTimestamp;
  }
  public void setPrevTimestamp(long prevTimestamp) {
    this.prevTimestamp = prevTimestamp;
  }
  public boolean getRunning() {
    return running;
  }
  public void setRunning(boolean running) {
    this.running = running;
  }
  public String getCurrentTripID() {
    return currentTripID;
  }
  public void setCurrentTripID(String currentTripID) {
    this.currentTripID = currentTripID;
  }
  
}

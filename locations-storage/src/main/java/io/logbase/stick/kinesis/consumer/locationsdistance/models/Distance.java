package io.logbase.stick.kinesis.consumer.locationsdistance.models;

public class Distance {
  
  private double distance;
  private double prevLat;
  private double prevLong;
  private long prevTimestamp;
  
  public Distance(double distance, double prevLat, double prevLong, long prevTimestamp) {
    this.distance = distance;
    this.prevLat = prevLat;
    this.prevLong = prevLong;
    this.prevTimestamp = prevTimestamp;
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
  
}

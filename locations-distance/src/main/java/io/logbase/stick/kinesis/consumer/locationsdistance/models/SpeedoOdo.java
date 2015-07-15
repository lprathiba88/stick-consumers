package io.logbase.stick.kinesis.consumer.locationsdistance.models;

public class SpeedoOdo {

  private Double speed;
  private Double distance;
  private Double lat;
  private Double lon;
  
  public SpeedoOdo(Double speed, Double distance, Double lat, Double lon) {
    super();
    this.speed = speed;
    this.distance = distance;
    this.lat = lat;
    this.lon = lon;
  }
  
  public Double getSpeed() {
    return speed;
  }
  public void setSpeed(Double speed) {
    this.speed = speed;
  }
  public Double getDistance() {
    return distance;
  }
  public void setDistance(Double distance) {
    this.distance = distance;
  }
  public Double getLat() {
    return lat;
  }
  public void setLat(Double lat) {
    this.lat = lat;
  }
  public Double getLon() {
    return lon;
  }
  public void setLon(Double lon) {
    this.lon = lon;
  }
  
}

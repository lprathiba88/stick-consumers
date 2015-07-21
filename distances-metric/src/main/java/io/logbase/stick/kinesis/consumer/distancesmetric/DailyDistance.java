package io.logbase.stick.kinesis.consumer.distancesmetric;

public class DailyDistance {
  
  private String day;
  private Double distance;
  
  public DailyDistance(String day, Double distance) {
    super();
    this.day = day;
    this.distance = distance;
  }
  
  public String getDay() {
    return day;
  }
  public void setDay(String day) {
    this.day = day;
  }
  public Double getDistance() {
    return distance;
  }
  public void setDistance(Double distance) {
    this.distance = distance;
  }
  
}

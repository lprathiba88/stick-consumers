package io.logbase.geo.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class GeoCoderTest {
  
  @Test
  public void revGeoCode() {
    String apiKey = System.getenv("GMAP_API_KEY");
    String result = GeoCoder.revGeocode(apiKey, 11, 77, 1);
    assertEquals("Krishnaswamy Nagar", result);
  }

}

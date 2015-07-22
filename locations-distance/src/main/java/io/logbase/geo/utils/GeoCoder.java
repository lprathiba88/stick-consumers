package io.logbase.geo.utils;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.LatLng;

public class GeoCoder {
  
  public static String revGeocode(String apiKey, double lat, double lon, int addressLevel) {
    String address = null;
    GeoApiContext context = new GeoApiContext().setApiKey(apiKey);
    try {
      GeocodingResult[] results;
      results = GeocodingApi.reverseGeocode(context, new LatLng(lat, lon)).await();
      address = results[0].addressComponents[addressLevel].longName;
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return address;
  }

}

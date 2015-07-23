package io.logbase.geo.utils;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.LatLng;

/**
 * Created by prathikarthik on 7/22/15.
 */
public class GeoLocation {

    public static String getLocation(LatLng latLong, String apiKey, String addressType) {

        String location=null;
        String tempLocation = "";

        GeoApiContext context = new GeoApiContext().setApiKey(apiKey);
        GeocodingResult[] results = new GeocodingResult[0];
        try {
            results = GeocodingApi.reverseGeocode(context, latLong).await();
        } catch (Exception e) {
            e.printStackTrace();
            return location;
        }

        if(results.length>0) {
            int count = 0;

            search:
            for (int i = 0; i < results[0].addressComponents.length; i++) {
                for (int j = 0; j < results[0].addressComponents[i].types.length; j++) {
                    if (((results[0].addressComponents[i].types[j].toString()).equals(addressType))) {
                        count++;
                        if (count < 3)
                            tempLocation += String.valueOf(results[0].addressComponents[i].longName)+" ";
                        else
                            break search;
                    }
                }
            }
            if(tempLocation=="") {
                search1:
                for (int i = 0; i < results[0].addressComponents.length; i++) {
                    for (int j = 0; j < results[0].addressComponents[i].types.length; j++) {
                        if (((results[0].addressComponents[i].types[j].toString()).equals("ROUTE"))||((results[0].addressComponents[i].types[j].toString()).equals("ADMINISTRATIVE_AREA_LEVEL_2"))||((results[0].addressComponents[i].types[j].toString()).equals("ADMINISTRATIVE_AREA_LEVEL_1"))) {
                            count++;
                            if (count < 3)
                                tempLocation += String.valueOf(results[0].addressComponents[i].longName)+" ";
                            else
                                break search1;
                        }
                    }
                }
            }
        }
        if(tempLocation!="")
            location = tempLocation;
        return location;
    }
}

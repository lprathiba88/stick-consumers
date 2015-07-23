package io.logbase.geo.utils;
import static org.junit.Assert.*;
import com.google.maps.model.LatLng;

/**
 * Created by prathikarthik on 7/23/15.
 */
public class GeoLocationTest {

    public static void main(String args[]) throws Exception {

        LatLng latLng = new LatLng(10,30);
        String apiKey = System.getenv("GMAP_API_KEY");
        String addressType = "SUBLOCALITY";

        String result = GeoLocation.getLocation(latLng, apiKey, addressType);
        System.out.println("Location:"+result);
        //assertEquals(" Krishnaswamy Nagar Sowripalayam Pirivu", result);

    }
}

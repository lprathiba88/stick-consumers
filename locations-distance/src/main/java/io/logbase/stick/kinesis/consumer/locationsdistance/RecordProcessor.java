package io.logbase.stick.kinesis.consumer.locationsdistance;

import io.logbase.stick.kinesis.consumer.locationsdistance.models.Distance;
import io.logbase.stick.kinesis.consumer.locationsdistance.models.SpeedoOdo;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Precision;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.Record;
import com.firebase.client.AuthData;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;

/**
 * A class used to create Record Processors for every shard.
 *
 * Created by Abishek on 7/3/15.
 */

public class RecordProcessor implements IRecordProcessor {

  private String shardId;
  private static final Log LOG = LogFactory.getLog(RecordProcessor.class);
  private Map<String, Distance> distancesCache = new HashMap<String, Distance>();
  private Map<String, SortedMap<Long, SpeedoOdo>> speedsCache = new HashMap<String, SortedMap<Long, SpeedoOdo>>();
  private static final long TRIP_CHECK_WINDOW_MILLIS = 300000L;
  private static final float NOT_MOVING_AVG_SPEED = 1.6f;
  private static final float SPEED_NOISE_CUTOFF = 55.55f;
  
  // Backoff and retry settings
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 5;

  // Checkpoint about once a minute
  private static final long CHECKPOINT_INTERVAL_MILLIS = 60 * 000L;
  private long nextCheckpointTimeInMillis;
  private AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());

  private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

  private Firebase firebaseRef = new Firebase("https://logbasedev.firebaseio.com/");

  @Override
  public void initialize(String shardId) {
    this.shardId = shardId;
    LOG.info("Staring record processor for shard id: " + shardId);
    nextCheckpointTimeInMillis = System.currentTimeMillis()
        + CHECKPOINT_INTERVAL_MILLIS;

    // Create a handler to handle the result of firebase authentication
    Firebase.AuthResultHandler authResultHandler = new Firebase.AuthResultHandler() {
      @Override
      public void onAuthenticated(AuthData authData) {
        LOG.info("Authenticated with Firebase");
      }
      @Override
      public void onAuthenticationError(FirebaseError firebaseError) {
        LOG.error("Firebase authentication failed: " + firebaseError);
      }
    };
    // Authenticate users with a custom Firebase token
    firebaseRef.authWithCustomToken(System.getenv("FIREBASE_SECRET"), authResultHandler);
    
  }

  @Override
  public void processRecords(List<Record> records,
      IRecordProcessorCheckpointer checkpointer) {
    LOG.info("Processing " + records.size() + " records from " + shardId);
    try {
      processRecordsWithRetries(records);
    } catch (Exception e) {
      e.printStackTrace();
    }
    // Checkpoint every one minute
    if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
      checkpoint(checkpointer);
      nextCheckpointTimeInMillis = System.currentTimeMillis()
          + CHECKPOINT_INTERVAL_MILLIS;
    }
  }

  private void processRecordsWithRetries(List<Record> records) throws Exception {
    for (Record record : records) {
      // String eventData = decoder.decode(record.getData()).toString();
      boolean processedSuccessfully = false;
      for (int i = 0; i < NUM_RETRIES; i++) {
        if (businessLogic(record)) {
          processedSuccessfully = true;
          break;
        }
        // backoff if we encounter an exception and try again.
        try {
          Thread.sleep(BACKOFF_TIME_IN_MILLIS);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted sleep", e);
        }
      }
      if (!processedSuccessfully) {
        LOG.error("Couldn't process record " + record
            + ". Skipping the record.");
      }
    }
  }

  /*
   * MAIN BUSINESS LOGIC FOR THIS CONSUMER
   */
  private boolean businessLogic(Record record) throws Exception {

    String eventData = decoder.decode(record.getData()).toString();
    LOG.info("EVENT DATA RECEIVED AS: " + eventData);

    // 1. Deserialize the event data
    JSONArray locations = new JSONArray(eventData);

    for (int i = 0, size = locations.length(); i < size; i++) {

      JSONObject location = locations.getJSONObject(i);
      String locationSourceID = location.getString("source_id");
      String locationAccountID = location.getString("account_id");
      double locationLat = location.getDouble("lat");
      double locationLong = location.getDouble("long");
      long locationTimestamp = location.getLong("time");
      double locationSpeed = location.getDouble("speed");
      String day = getDay(locationTimestamp);

      Firebase distanceRef = firebaseRef.child("/accounts/" + locationAccountID
          + "/activity/devices/" + locationSourceID + "/daily/" + day);

      // 2. Check if prev distance available for that day, if not load from
      // firebase
      Distance distance = distancesCache.get(locationSourceID);
      if ( (distance == null) || (!getDay(distance.getPrevTimestamp()).equals(day)) ) {
        LOG.info("Distance not cached for: " + locationSourceID + "|" + day);
        distanceRef.addListenerForSingleValueEvent(new ValueEventListener() {
          @Override
          public void onDataChange(DataSnapshot snapshot) {
            Map<String, Object> dailyActivity = (Map<String, Object>) snapshot.getValue();
            if (dailyActivity == null) {
              LOG.info("Distance not updated in firebase: " + locationSourceID);
              fetchTripStatus(locationAccountID, locationSourceID, 0, locationLat, locationLong, locationTimestamp, false, locationLat, locationLong, locationTimestamp, distanceRef, locationSpeed);
              /*
              distancesCache.put(locationSourceID, new Distance(0, locationLat,
                  locationLong, locationTimestamp, false, null));
               */
            } else {
              double prevTravel = (Double)dailyActivity.get("distance");
              double prevLat = (Double)dailyActivity.get("latitude");
              double prevLong = (Double)dailyActivity.get("longitude");
              long prevTs = (Long)dailyActivity.get("timestamp");
              LOG.info("Distance present in firebase: " + locationSourceID
                  + "|" + prevTravel);
              fetchTripStatus(locationAccountID, locationSourceID, prevTravel, prevLat, prevLong, prevTs, true, locationLat, locationLong, locationTimestamp, distanceRef, locationSpeed);
              /*
              Distance d = new Distance(prevTravel, prevLat, prevLong, prevTs, false, null);
              //cache prev value
              distancesCache.put(locationSourceID, d);
              calculateNewDistance(d, locationAccountID, locationSourceID, locationLat, locationLong, locationTimestamp, distanceRef);
              */
            }
          }
          @Override
          public void onCancelled(FirebaseError firebaseError) {
            LOG.error("Unable to connect to firebase: " + firebaseError);
          }
        });
      } else {
        calculateNewDistance(distance, locationAccountID, locationSourceID, locationLat, locationLong, locationTimestamp, distanceRef);
        if(distance.getRunning())
          checkIfTripEnded(distance, locationAccountID, locationSourceID, locationSpeed, locationTimestamp);
        else
          checkIfTripStarted(distance, locationAccountID, locationSourceID, locationSpeed, locationTimestamp);
      }
    }//end of for loop
    return true;
  }
  
  private void fetchTripStatus(String accountID, String sourceID, double travel, double lat, double lon, long timestamp, boolean calcDistance, double newLat, double newLong, long newTs, Firebase distanceRef, double newSpeed) {
    Firebase statusRef = firebaseRef.child("/accounts/" + accountID
        + "/livecars/" + sourceID);
    statusRef.addListenerForSingleValueEvent(new ValueEventListener() {
      @Override
      public void onDataChange(DataSnapshot snapshot) {
        Map<String, Object> live = (Map<String, Object>) snapshot.getValue();
        Distance d = null;
        if ((live == null) || (live.get("running") == null) ) {
          LOG.info("Running status not updated in firebase: " + sourceID);
          d = new Distance(0, lat, lon, timestamp, false, null);
        } else {
          boolean running = (Boolean)live.get("running");
          String currentTripID = (String)live.get("currenttripid");
          LOG.info("Running status present in firebase: " + sourceID
              + "|" + running +  "|" + currentTripID);
          d = new Distance(travel, lat, lon, timestamp, running, currentTripID);
        }
        distancesCache.put(sourceID, d);  
        if(calcDistance) {
          calculateNewDistance(d, accountID, sourceID, newLat, newLong, newTs, distanceRef);
        }
        if(d.getRunning())
          checkIfTripEnded(d, accountID, sourceID, newSpeed, newTs);
        else
          checkIfTripStarted(d, accountID, sourceID, newSpeed, newTs);
      }
      @Override
      public void onCancelled(FirebaseError firebaseError) {
        LOG.error("Unable to connect to firebase: " + firebaseError);
      }
    });
  }
  
  private void checkIfTripStarted(Distance distance, String accountID, String sourceID, double speed, long timestamp) {
    //TODO
    //Insert new speed into the time sliced map
    //Check avg speed, if above critical speed then trip started
    //If trip started, update cache, firebase status and trip entry
    if(distance.getPrevTimestamp() <= timestamp) {    //To ensure event order
      if(speed < SPEED_NOISE_CUTOFF) {                //To ensure quality
        
        SortedMap<Long, SpeedoOdo> speeds = speedsCache.get(sourceID);
        
        if(speeds == null){
          speeds = new TreeMap<Long, SpeedoOdo>();
          speeds.put(timestamp, new SpeedoOdo(speed, distance.getDistance(), distance.getPrevLat(), distance.getPrevLong()));
          speedsCache.put(sourceID, speeds);
        } else {
          //Check if speeds needs trimming
          long currentWindowStart = timestamp - TRIP_CHECK_WINDOW_MILLIS;
          if (speeds.size() > 0) {
            long firstKey = speeds.firstKey();
            if(firstKey < currentWindowStart){
                //Get a tail map
                SortedMap newSpeeds = speeds.tailMap(currentWindowStart);
                speeds = newSpeeds;
                LOG.info("Trimmed speeds");;
            }
          }
          //Insert into speeds Map
          speeds.put(timestamp, new SpeedoOdo(speed, distance.getDistance(), distance.getPrevLat(), distance.getPrevLong()));
        }
        
        //Now check avg speed
        //If we have sufficient data
        LOG.info("SPEED WINDOW: " + speeds.firstKey() + "|" + speeds.lastKey() + " Diff: " + (speeds.lastKey() - speeds.firstKey()));
        if((speeds.lastKey() - speeds.firstKey()) > (TRIP_CHECK_WINDOW_MILLIS/2) ) {
          double sumSpeed = 0;
          double avgSpeed = 0;
          for (long time : speeds.keySet())
            sumSpeed = sumSpeed + speeds.get(time).getSpeed();
          avgSpeed = sumSpeed / speeds.size();
          LOG.info("AVG SPEED: " + avgSpeed);
          if (avgSpeed > NOT_MOVING_AVG_SPEED) {
            //Started!
            
            //update cache
            String tripName = sourceID + "-" + getDateTime(speeds.firstKey());
            distance.setRunning(true);
            distance.setCurrentTripID(tripName);

            //Status update on firebase
            Firebase statusRef = firebaseRef.child("/accounts/" + accountID
                + "/livecars/" + sourceID);
            Map<String, Object> firebaseStatusUpdate = new HashMap<String, Object>();
            firebaseStatusUpdate.put("running", true);
            firebaseStatusUpdate.put("currenttripid", tripName);
            statusRef.updateChildren(firebaseStatusUpdate);
            //statusRef.setValue(firebaseStatusUpdate);
            LOG.info("Updated live car status as running: " + tripName);
            
            //Trip update on firebase
            long tripStartTime = speeds.firstKey();
            SpeedoOdo tripStartData = speeds.get(tripStartTime);
            Firebase tripRef = firebaseRef.child("/accounts/" + accountID
                + "/trips/devices/" + sourceID + "/daily/" + getDay(tripStartTime) + "/" + tripName);
            Map<String, Object> firebaseTripUpdate = new HashMap<String, Object>();
            firebaseTripUpdate.put("starttime", tripStartTime);
            firebaseTripUpdate.put("startlatitude", tripStartData.getLat());
            firebaseTripUpdate.put("startlongitude", tripStartData.getLon());
            firebaseTripUpdate.put("startodo", tripStartData.getDistance());
            tripRef.setValue(firebaseTripUpdate);
            LOG.info("Updated trip started data on firebase: " + tripName);            
            
          } else
            LOG.info("MOVEMENT NOT DETECTED: " + sourceID);
        } else
          LOG.info("NOT SUFFICIENT DATA TO CHECK TRIP START: " + sourceID);  
        
      }
    }
    
  }
  
  private void checkIfTripEnded(Distance distance, String accountID, String sourceID, double speed, long timestamp) {

    //TODO
    //Insert new speed into the time sliced map
    //Check avg speed, if below critical speed then trip ended
    //If trip ended, update cache, firebase status and trip entry
    if(distance.getPrevTimestamp() <= timestamp) {    //To ensure event order
      if(speed < SPEED_NOISE_CUTOFF) {                //To ensure quality
        
        SortedMap<Long, SpeedoOdo> speeds = speedsCache.get(sourceID);
        
        if(speeds == null){
          speeds = new TreeMap<Long, SpeedoOdo>();
          speeds.put(timestamp, new SpeedoOdo(speed, distance.getDistance(), distance.getPrevLat(), distance.getPrevLong()));
          speedsCache.put(sourceID, speeds);
        } else {
          //Check if speeds needs trimming
          long currentWindowStart = timestamp - TRIP_CHECK_WINDOW_MILLIS;
          if (speeds.size() > 0) {
            long firstKey = speeds.firstKey();
            if(firstKey < currentWindowStart){
                //Get a tail map
                SortedMap newSpeeds = speeds.tailMap(currentWindowStart);
                speeds = newSpeeds;
                LOG.info("Trimmed speeds");;
            }
          }
          //Insert into speeds Map
          speeds.put(timestamp, new SpeedoOdo(speed, distance.getDistance(), distance.getPrevLat(), distance.getPrevLong()));
        }
        
        //Now check avg speed
        //If we have sufficient data
        LOG.info("SPEED WINDOW: " + speeds.firstKey() + "|" + speeds.lastKey() + " Diff: " + (speeds.lastKey() - speeds.firstKey()));
        if((speeds.lastKey() - speeds.firstKey()) > (TRIP_CHECK_WINDOW_MILLIS/2) ) {
          double sumSpeed = 0;
          double avgSpeed = 0;
          for (long time : speeds.keySet())
            sumSpeed = sumSpeed + speeds.get(time).getSpeed();
          avgSpeed = sumSpeed / speeds.size();
          LOG.info("AVG SPEED: " + avgSpeed);
          if (avgSpeed <= NOT_MOVING_AVG_SPEED) {
            //Ended!
            String tripName = distance.getCurrentTripID();
            LOG.info("TRIP ENDED: " + tripName);
            
            //update cache
            distance.setRunning(false);
            distance.setCurrentTripID(null);

            //Status update on firebase
            Firebase statusRef = firebaseRef.child("/accounts/" + accountID
                + "/livecars/" + sourceID);
            Map<String, Object> firebaseStatusUpdate = new HashMap<String, Object>();
            firebaseStatusUpdate.put("running", false);
            firebaseStatusUpdate.put("currenttripid", null);
            statusRef.updateChildren(firebaseStatusUpdate);
            //statusRef.setValue(firebaseStatusUpdate);
            LOG.info("Updated live car status as stopped: " + tripName);
            
            //Trip update on firebase
            String tripStartDay = tripName.substring(tripName.length() - 19, tripName.length() -11);
            LOG.info("Trip start day extracted as: " + tripStartDay);
            long tripEndTime = speeds.firstKey();
            SpeedoOdo tripEndData = speeds.get(tripEndTime);
            Firebase tripRef = firebaseRef.child("/accounts/" + accountID
                + "/trips/devices/" + sourceID + "/daily/" + tripStartDay + "/" + tripName);
            Map<String, Object> firebaseTripUpdate = new HashMap<String, Object>();
            firebaseTripUpdate.put("endtime", tripEndTime);
            firebaseTripUpdate.put("endlatitude", tripEndData.getLat());
            firebaseTripUpdate.put("endlongitude", tripEndData.getLon());
            firebaseTripUpdate.put("endodo", tripEndData.getDistance());
            tripRef.updateChildren(firebaseTripUpdate);
            LOG.info("Updated trip ended data on firebase: " + tripName);            
            
          } else
            LOG.info("STILL MOVING...: " + sourceID);
        } else
          LOG.info("NOT SUFFICIENT DATA TO CHECK TRIP END: " + sourceID);  
        
      }
    }
  }
  
  private void calculateNewDistance(Distance distance, String locationAccountID, String locationSourceID, double locationLat, double locationLong, long locationTimestamp, Firebase distanceRef){
    if(distance.getPrevTimestamp() < locationTimestamp) {
      // 3. With new location, calculate new distance
      double travel = calcDistance(distance.getPrevLat(),
          distance.getPrevLong(), locationLat, locationLong, 'K');
      LOG.info("Tavel: " + locationSourceID + "|" + travel);
      // Ignore if this travel is abnormal (very less or very high)
      if ((travel < 0.004) || (travel > 0.5))
        travel = 0;
      double newTravel = travel + distance.getDistance();
      LOG.info("New Distance: " + locationSourceID + "|" + newTravel);
      distance.setDistance(newTravel);
      distance.setPrevLat(locationLat);
      distance.setPrevLong(locationLong);
      distance.setPrevTimestamp(locationTimestamp);
      // 4. Update the new distance to firebase
      Map<String, Object> firebaseUpdate = new HashMap<String, Object>();
      firebaseUpdate.put("distance", newTravel);
      firebaseUpdate.put("latitude", locationLat);
      firebaseUpdate.put("longitude", locationLong);
      firebaseUpdate.put("timestamp", locationTimestamp);
      distanceRef.setValue(firebaseUpdate);
      // 5. Send back to kinesis for aggregation
      //Send just the incremental travel
      JSONObject distanceJson = new JSONObject();
      distanceJson.put("account_id", locationAccountID);
      distanceJson.put("source_id", locationSourceID);
      distanceJson.put("distance", travel);
      distanceJson.put("timestamp", locationTimestamp);
      String distanceEvent = distanceJson.toString();
      LOG.info("DISTANCE EVENT TO KINESIS: " + distanceEvent);
      PutRecordRequest putRecordRequest = new PutRecordRequest();
      putRecordRequest.setStreamName("stick-distances");
      putRecordRequest.setData(ByteBuffer.wrap( distanceEvent.getBytes() ));
      putRecordRequest.setPartitionKey(locationAccountID);  
      amazonKinesisClient.putRecord( putRecordRequest );
    } else 
      LOG.info("Not calculating new distance as timestamps are not in order");
  }

  @Override
  public void shutdown(IRecordProcessorCheckpointer checkpointer,
      ShutdownReason reason) {
    if (reason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer);
    }
    LOG.info("Shutting down record processor for shard id: " + shardId);
  }

  /**
   * Checkpoint with retries. This will update the checkpoint in DynamoDB for
   * every record processed. This will keep the lease alive for the shard.
   *
   * @param checkpointer
   */
  private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
    LOG.info("Checkpointing shard " + shardId);
    for (int i = 0; i < NUM_RETRIES; i++) {
      try {
        checkpointer.checkpoint();
        break;
      } catch (ShutdownException se) {
        // Ignore checkpoint if the processor instance has been shutdown (fail
        // over).
        LOG.info("Caught shutdown exception, skipping checkpoint.", se);
        break;
      } catch (ThrottlingException e) {
        // Backoff and re-attempt checkpoint upon transient failures
        if (i >= (NUM_RETRIES - 1)) {
          LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
          break;
        } else {
          LOG.info("Transient issue when checkpointing - attempt " + (i + 1)
              + " of " + NUM_RETRIES, e);
        }
      } catch (InvalidStateException e) {
        LOG.error(
            "Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
        break;
      }
      try {
        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted sleep", e);
      }
    }
  }

  // Utilities
  private double calcDistance(double lat1, double lon1, double lat2,
      double lon2, char unit) {
    double theta = lon1 - lon2;
    double dist = Math.sin(deg2rad(lat1)) * Math.sin(deg2rad(lat2))
        + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2))
        * Math.cos(deg2rad(theta));
    dist = Math.acos(dist);
    dist = rad2deg(dist);
    dist = dist * 60 * 1.1515;
    if (unit == 'K') {
      dist = dist * 1.609344;
    } else if (unit == 'N') {
      dist = dist * 0.8684;
    }
    double roundedDist = Precision.round(dist, 3);
    return (roundedDist);
  }

  private double deg2rad(double deg) {
    return (deg * Math.PI / 180.0);
  }

  private double rad2deg(double rad) {
    return (rad * 180.0 / Math.PI);
  }

  private String getDay(long timestamp) {
    Format format = new SimpleDateFormat("yyyyMMdd");
    String day = format.format(new Date(timestamp));
    return day;
  }
  
  private String getDateTime(long timestamp) {
    Format format = new SimpleDateFormat("yyyyMMdd-HHmmss-SSS");
    String datetime = format.format(new Date(timestamp));
    return datetime;
  }

}
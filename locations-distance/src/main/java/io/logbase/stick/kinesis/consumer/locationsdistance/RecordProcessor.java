package io.logbase.stick.kinesis.consumer.locationsdistance;

import io.logbase.geo.utils.GeoCoder;
import io.logbase.stick.kinesis.consumer.locationsdistance.models.DailyDistance;
import io.logbase.stick.kinesis.consumer.locationsdistance.models.SpeedoOdo;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TimeZone;
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
  private Map<String, DailyDistance> distancesCache = new HashMap<String, DailyDistance>();
  private Map<String, SortedMap<Long, SpeedoOdo>> speedsCache = new HashMap<String, SortedMap<Long, SpeedoOdo>>();
  /*General logic for trip detection:
   * Movement start is when average speed is above NOT_MOVING_AVG_SPEED for more than START_TRIP_CHECK_WINDOW_MILLIS.
   * Movement stop is when average speed is below NOT_MOVING_AVG_SPEED for more than END_TRIP_CHECK_WINDOW_MILLIS.
   * A trip is recorded when there is movement recorded for MIN_TRIP_INTERVAL
   */
  private static final long START_TRIP_CHECK_WINDOW_MILLIS = 10000L;
  private static final long END_TRIP_CHECK_WINDOW_MILLIS = 300000L;
  private static final float NOT_MOVING_AVG_SPEED = 1.6f;
  private static final long MIN_TRIP_INTERVAL = 60000L;
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
  private static final String GMAP_API_KEY = System.getenv("GMAP_API_KEY");

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
    if(GMAP_API_KEY == null)
      LOG.error("GMAP API KEY Env variable not set");
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
      DailyDistance dailyDistance = distancesCache.get(locationSourceID);
      if ( (dailyDistance == null) || (!getDay(dailyDistance.getPrevTimestamp()).equals(day)) ) {
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
        calculateNewDistance(dailyDistance, locationAccountID, locationSourceID, locationLat, locationLong, locationTimestamp, distanceRef);
        if(dailyDistance.getRunning())
          checkIfTripEnded(dailyDistance, locationAccountID, locationSourceID, locationSpeed, locationTimestamp);
        else
          checkIfTripStarted(dailyDistance, locationAccountID, locationSourceID, locationSpeed, locationTimestamp);
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
        DailyDistance d = null;
        if ((live == null) || (live.get("running") == null) ) {
          LOG.info("Running status not updated in firebase: " + sourceID);
          d = new DailyDistance(0, lat, lon, timestamp, false, null);
        } else {
          boolean running = (Boolean)live.get("running");
          String currentTripID = (String)live.get("currenttripid");
          LOG.info("Running status present in firebase: " + sourceID
              + "|" + running +  "|" + currentTripID);
          d = new DailyDistance(travel, lat, lon, timestamp, running, currentTripID);
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
  
  private void checkIfTripStarted(DailyDistance distance, String accountID, String sourceID, double speed, long timestamp) {
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
          long currentWindowStart = timestamp - START_TRIP_CHECK_WINDOW_MILLIS;
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
        //If critical number of samples
        if((speeds.lastKey() - speeds.firstKey()) > (START_TRIP_CHECK_WINDOW_MILLIS/2) ) {
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
            //Rev GeoCode
            String address = GeoCoder.revGeocode(GMAP_API_KEY, tripStartData.getLat(), tripStartData.getLon(), 1);
            Map<String, Object> firebaseTripUpdate = new HashMap<String, Object>();
            firebaseTripUpdate.put("starttime", tripStartTime);
            firebaseTripUpdate.put("startlatitude", tripStartData.getLat());
            firebaseTripUpdate.put("startlongitude", tripStartData.getLon());
            firebaseTripUpdate.put("startodo", tripStartData.getDistance());
            firebaseTripUpdate.put("startaddress", address);
            tripRef.setValue(firebaseTripUpdate);
            LOG.info("Updated trip started data on firebase: " + tripName);            
            
          } else
            LOG.info("MOVEMENT NOT DETECTED: " + sourceID);
        } else
          LOG.info("NOT SUFFICIENT DATA TO CHECK TRIP START: " + sourceID);  
        
      }
    }
    
  }
  
  private void checkIfTripEnded(DailyDistance distance, String accountID, String sourceID, double speed, long timestamp) {

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
          long currentWindowStart = timestamp - END_TRIP_CHECK_WINDOW_MILLIS;
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
        if((speeds.lastKey() - speeds.firstKey()) > (END_TRIP_CHECK_WINDOW_MILLIS/2) ) {
          double sumSpeed = 0;
          double avgSpeed = 0;
          for (long time : speeds.keySet())
            sumSpeed = sumSpeed + speeds.get(time).getSpeed();
          avgSpeed = sumSpeed / speeds.size();
          LOG.info("AVG SPEED: " + avgSpeed);
          if (avgSpeed <= NOT_MOVING_AVG_SPEED) {
            //Ended!
            String tripName = distance.getCurrentTripID();
            LOG.info("MOVEMENT STOPPED: " + tripName);
            
            //Further checks on distance, time lapsed
            String tripStartDay = tripName.substring(tripName.length() - 19, tripName.length() -11);
            long tripStartTime = getEpoch(tripName.substring(tripName.length() - 19, tripName.length()));
            LOG.info("Trip start day extracted as: " + tripStartDay);
            long tripEndTime = speeds.firstKey();
            String tripEndDay = getDay(tripEndTime);
            
            SpeedoOdo tripEndData = speeds.get(tripEndTime);
            
            // 1. Check time lapsed
            if (tripEndTime - tripStartTime >= MIN_TRIP_INTERVAL ) {
              //Successful trip.
              //calculate distance
              if(tripStartDay.equals(tripEndDay)) {
                //Use daily Odo from trip data stored.
                Firebase tripRef = firebaseRef.child("/accounts/" + accountID
                    + "/trips/devices/" + sourceID + "/daily/" + tripStartDay + "/" + tripName);
                tripRef.addListenerForSingleValueEvent(new ValueEventListener() {
                  @Override
                  public void onDataChange(DataSnapshot snapshot) {
                    Map<String, Object> trip = (Map<String, Object>) snapshot.getValue();
                    if(trip != null) {
                      long startOdo = (Long) trip.get("startodo");
                      double tripDistance = tripEndData.getDistance() - startOdo;
                      updateTripEnded(tripName, tripDistance, tripStartDay, tripEndTime, tripEndData, distance, accountID, sourceID);
                    }
                  }
                  @Override
                  public void onCancelled(FirebaseError firebaseError) {
                    LOG.error("Unable to connect to firebase: " + firebaseError);
                  }
                });
              } else {
                //Need to look up end of day odo as a day crossed.
                Firebase tripRef = firebaseRef.child("/accounts/" + accountID
                    + "/trips/devices/" + sourceID + "/daily/" + tripStartDay + "/" + tripName);
                tripRef.addListenerForSingleValueEvent(new ValueEventListener() {
                  @Override
                  public void onDataChange(DataSnapshot snapshot) {
                    Map<String, Object> trip = (Map<String, Object>) snapshot.getValue();
                    if(trip != null) {
                      long startOdo = (Long) trip.get("startodo");
                      //Cant use this odo as it is as the day has crossed
                      //Get end of day data for this device from daily activity
                      //tripDistance = (endOfDay Odo - startOdo) + tripEndData Odo
                      Firebase dailyActivityRef = firebaseRef.child("/accounts/" + accountID
                          + "/activity/devices/" + sourceID + "/daily/" + tripStartDay);
                      dailyActivityRef.addListenerForSingleValueEvent(new ValueEventListener() {
                        @Override
                        public void onDataChange(DataSnapshot snapshot) {
                          Map<String, Object> activity = (Map<String, Object>) snapshot.getValue();
                          if( (activity != null) && (activity.get("distance") != null) ) {
                            double dailyDistance = (Double) activity.get("distance");
                            double tripDistance = (dailyDistance - startOdo) + tripEndData.getDistance();
                            updateTripEnded(tripName, tripDistance, tripStartDay, tripEndTime, tripEndData, distance, accountID, sourceID);
                          } else {
                            // something fishy, reset trip
                            resetTrip(tripName, tripStartDay, distance, accountID, sourceID);
                          }
                        }
                        @Override
                        public void onCancelled(FirebaseError firebaseError) {
                          LOG.error("Unable to connect to firebase: " + firebaseError);
                        }
                      });
                    }
                  }
                  @Override
                  public void onCancelled(FirebaseError firebaseError) {
                    LOG.error("Unable to connect to firebase: " + firebaseError);
                  }
                });
                
              }
            } else {
              // Trip interval insignificant, reset the trip start.
              resetTrip(tripName, tripStartDay, distance, accountID, sourceID);
            }

          } else
            LOG.info("STILL MOVING...: " + sourceID);
        } else
          LOG.info("NOT SUFFICIENT DATA TO CHECK TRIP END: " + sourceID);  
        
      }
    }
  }
  
  private void updateTripEnded(String tripName, double tripDistance, String tripStartDay, long tripEndTime, SpeedoOdo tripEndData, DailyDistance ddCached, String accountID, String sourceID) {
    //update cache
    ddCached.setRunning(false);
    ddCached.setCurrentTripID(null);
    
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
    Firebase tripRef = firebaseRef.child("/accounts/" + accountID
        + "/trips/devices/" + sourceID + "/daily/" + tripStartDay + "/" + tripName);
    //Rev GeoCode
    String address = GeoCoder.revGeocode(GMAP_API_KEY, tripEndData.getLat(), tripEndData.getLon(), 1);
    Map<String, Object> firebaseTripUpdate = new HashMap<String, Object>();
    firebaseTripUpdate.put("endtime", tripEndTime);
    firebaseTripUpdate.put("endlatitude", tripEndData.getLat());
    firebaseTripUpdate.put("endlongitude", tripEndData.getLon());
    firebaseTripUpdate.put("endodo", tripEndData.getDistance());
    firebaseTripUpdate.put("tripdistance", tripDistance);
    firebaseTripUpdate.put("endaddress", address);
    tripRef.updateChildren(firebaseTripUpdate);
    LOG.info("Updated trip ended data on firebase: " + tripName);            
  }
  
  private void resetTrip(String tripName, String tripStartDay, DailyDistance ddCached, String accountID, String sourceID) {
    //update cache
    ddCached.setRunning(false);
    ddCached.setCurrentTripID(null);
    
    //remove status on firebase
    Firebase statusRef = firebaseRef.child("/accounts/" + accountID
        + "/livecars/" + sourceID);
    Map<String, Object> firebaseStatusUpdate = new HashMap<String, Object>();
    firebaseStatusUpdate.put("running", false);
    firebaseStatusUpdate.put("currenttripid", null);
    statusRef.updateChildren(firebaseStatusUpdate);
    //statusRef.setValue(firebaseStatusUpdate);
    LOG.info("Updated live car status as stopped: " + tripName);
    
    //Trip update on firebase
    Firebase tripRef = firebaseRef.child("/accounts/" + accountID
        + "/trips/devices/" + sourceID + "/daily/" + tripStartDay + "/" + tripName);
    tripRef.removeValue();
    LOG.info("Removed trip ended data on firebase: " + tripName);            
  }
  
  private void calculateNewDistance(DailyDistance distanceCached, String locationAccountID, String locationSourceID, double locationLat, double locationLong, long locationTimestamp, Firebase distanceRef){
    if(distanceCached.getPrevTimestamp() < locationTimestamp) {
      // 3. With new location, calculate new distance
      double travel = 0;
      travel = calcDistance(distanceCached.getPrevLat(),
          distanceCached.getPrevLong(), locationLat, locationLong, 'K');
      LOG.info("Tavel: " + locationSourceID + "|" + travel);
      // Ignore if this travel is abnormal (very less or very high)
      if ((travel < 0.004) || (travel > 0.5))
        travel = 0;
      double newTravel = travel + distanceCached.getDistance();
      LOG.info("New Distance: " + locationSourceID + "|" + newTravel);
      distanceCached.setDistance(newTravel);
      distanceCached.setPrevLat(locationLat);
      distanceCached.setPrevLong(locationLong);
      distanceCached.setPrevTimestamp(locationTimestamp);
      // 4. Update the new distance to firebase
      Map<String, Object> firebaseUpdate = new HashMap<String, Object>();
      firebaseUpdate.put("distance", newTravel);
      firebaseUpdate.put("latitude", locationLat);
      firebaseUpdate.put("longitude", locationLong);
      firebaseUpdate.put("timestamp", locationTimestamp);
      distanceRef.setValue(firebaseUpdate);
      // 5. Send back to kinesis for aggregation
      //Send just the incremental travel
      //Send only if distance > 0
      if (travel > 0) {
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
      }
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
    double roundedDist = 0;
    roundedDist = Precision.round(dist, 3);
    return (roundedDist);
  }

  private double deg2rad(double deg) {
    return (deg * Math.PI / 180.0);
  }

  private double rad2deg(double rad) {
    return (rad * 180.0 / Math.PI);
  }

  private String getDay(long timestamp) {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    String day = format.format(new Date(timestamp));
    return day;
  }
  
  private String getDateTime(long timestamp) {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HHmmss-SSS");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    String datetime = format.format(new Date(timestamp));
    return datetime;
  }
  
  private long getEpoch(String datetime) {
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-HHmmss-SSS");
    format.setTimeZone(TimeZone.getTimeZone("UTC"));
    long epoch = 0;
    try {
      epoch  = format.parse(datetime).getTime();
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return epoch;
  }

}
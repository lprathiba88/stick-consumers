package io.logbase.stick.kinesis.consumer.locationsdistance;

import io.logbase.stick.kinesis.consumer.locationsdistance.models.Distance;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
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
  private Map<String, Distance> distances = new HashMap<String, Distance>();

  // Backoff and retry settings
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 5;

  // Checkpoint about once a minute
  private static final long CHECKPOINT_INTERVAL_MILLIS = 60 * 000L;
  private long nextCheckpointTimeInMillis;

  private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

  private Firebase firebaseRef = new Firebase(
      "https://logbasedev.firebaseio.com/");

  @Override
  public void initialize(String shardId) {
    this.shardId = shardId;
    LOG.info("Staring record processor for shard id: " + shardId);
    nextCheckpointTimeInMillis = System.currentTimeMillis()
        + CHECKPOINT_INTERVAL_MILLIS;
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
      Double locationLat = location.getDouble("lat");
      Double locationLong = location.getDouble("long");
      Long locationTimestamp = location.getLong("time");
      Double locationSpeed = location.getDouble("speed");

      Date date = new Date(locationTimestamp);
      Format format = new SimpleDateFormat("yyyyMMdd");
      String day = format.format(date);
      
      Firebase distanceRef = firebaseRef.child("/accounts/"
          + locationAccountID + "/activity/devices/" + locationSourceID
          + "/daily/" + day + "/distance/");

      // 2. Check if prev distance available for that day, if not load from firebase
      Distance distance = distances.get(locationSourceID);
      if (distance == null) {
        distanceRef.addListenerForSingleValueEvent(new ValueEventListener() {
          @Override
          public void onDataChange(DataSnapshot snapshot) {
            Double d = (Double) snapshot.getValue();
            if (d == null)
              distances.put(locationSourceID, new Distance(0, locationLat,
                  locationLong, locationTimestamp));
            else
              distances.put(locationSourceID, new Distance(d, locationLat,
                  locationLong, locationTimestamp));
          }

          @Override
          public void onCancelled(FirebaseError firebaseError) {
            LOG.error("Unable to connect to firebase: " + firebaseError);
          }
        });
      } else {
        // Distance is in cache, check if date matches, if not roll over.
        if ( (getDay(distance.getPrevTimestamp())).equals(day) ) {
          // 3. With new location, calculate new distance
          double travel = calcDistance(distance.getPrevLat(), distance.getPrevLong(), locationLat, locationLong, 'K');
          //Ignore if this travel is abnormal (very less or very high)
          if ((travel < 0.001) || (travel > 1))
            travel = 0;
          double newDistance = travel + distance.getDistance();
          distance.setDistance(newDistance);
          distance.setPrevLat(locationLat);
          distance.setPrevLong(locationLong);
          // 4. Update the new distance to firebase
          distanceRef.setValue(newDistance);
          // 5. TODO: Send back to kinesis?
        } else
          distances.put(locationSourceID, new Distance(0, locationLat, locationLong, locationTimestamp));
      }
    }
    return true;
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
            "Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.",
            e);
        break;
      }
      try {
        Thread.sleep(BACKOFF_TIME_IN_MILLIS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted sleep", e);
      }
    }
  }

  private double calcDistance(double lat1, double lon1, double lat2, double lon2, char unit) {
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
    return (dist);
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

}
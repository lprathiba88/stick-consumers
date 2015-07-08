package io.logbase.stick.kinesis.consumer.distancesmetric;

import java.nio.ByteBuffer;
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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
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
  private Map<String, Map<String, Double>> distancesCache = new HashMap<String, Map<String, Double>>();

  // Backoff and retry settings
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 5;

  // Checkpoint about once a minute
  private static final long CHECKPOINT_INTERVAL_MILLIS = 60 * 000L;
  private long nextCheckpointTimeInMillis;
  private AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());

  private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

  private Firebase firebaseRef = new Firebase(
      "https://logbasedev.firebaseio.com/");

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
    /*
    JSONArray locations = new JSONArray(eventData);

    for (int i = 0, size = locations.length(); i < size; i++) {
      JSONObject location = locations.getJSONObject(i);
      String locationSourceID = location.getString("source_id");
      String locationAccountID = location.getString("account_id");
      Double locationLat = location.getDouble("lat");
      Double locationLong = location.getDouble("long");
      Long locationTimestamp = location.getLong("time");
      Double locationSpeed = location.getDouble("speed");
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
              distancesCache.put(locationSourceID, new Distance(0, locationLat,
                  locationLong, locationTimestamp));
            } else {
              Double prevTravel = (Double)dailyActivity.get("distance");
              Double prevLat = (Double)dailyActivity.get("latitude");
              Double prevLong = (Double)dailyActivity.get("longitude");
              Long prevTs = (Long)dailyActivity.get("timestamp");
              LOG.info("Distance present in firebase: " + locationSourceID
                  + "|" + prevTravel);
              Distance d = new Distance(prevTravel, prevLat, prevLong, prevTs);
              //cache prev value
              distancesCache.put(locationSourceID, d);
              calculateNewDistance(d, locationAccountID, locationSourceID, locationLat, locationLong, locationTimestamp, distanceRef);
            }
          }
          @Override
          public void onCancelled(FirebaseError firebaseError) {
            LOG.error("Unable to connect to firebase: " + firebaseError);
          }
        });
      } else
        calculateNewDistance(distance, locationAccountID, locationSourceID, locationLat, locationLong, locationTimestamp, distanceRef);
    
    }//end of for loop
    */
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

  private String getDay(long timestamp) {
    Format format = new SimpleDateFormat("yyyyMMdd");
    String day = format.format(new Date(timestamp));
    return day;
  }

}
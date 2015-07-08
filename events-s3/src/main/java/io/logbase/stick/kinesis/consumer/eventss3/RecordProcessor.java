package io.logbase.stick.kinesis.consumer.eventss3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

/**
 * A class used to create Record Processors for every shard.
 *
 * Created by Abishek on 7/3/15.
 */

public class RecordProcessor implements IRecordProcessor {

  private String shardId;
  private static final Log LOG = LogFactory.getLog(RecordProcessor.class);

  // Backoff and retry settings
  private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
  private static final int NUM_RETRIES = 5;

  // Checkpoint about once a minute
  private static final long CHECKPOINT_INTERVAL_MILLIS = 60 * 000L;
  private long nextCheckpointTimeInMillis;

  // Upload to S3 once every hour;
  private static final long S3_UPLOAD_TIME_INTERVAL_MILLIS = 3600 * 1000L;
  private long s3UploadTimeIntervalMillis = S3_UPLOAD_TIME_INTERVAL_MILLIS;
  private long nextS3UploadTimeInMillis;

  private String path;
  private String s3BucketName = "io.logbase.stick.events";
  private String eventName = "events";
  private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

  @Override
  public void initialize(String shardId) {
    this.shardId = shardId;
    LOG.info("Staring record processor for shard id: " + shardId);
    path = System.getenv("LOCAL_PATH");
    if (path == null) {
      path = "./events/";
      LOG.info("Set default file path as env variable was null");
      File f = new File(path);
      if (!f.exists()) {
        f.mkdir();
        LOG.info("Created a default directory");
      }
    } else {
      path = path + "/";
    }
    LOG.info("File path: " + path);
    String s3UploadTimeLimit = System.getenv("S3_UPLOAD_TIME_INTERVAL");
    try {
      s3UploadTimeIntervalMillis = Long.parseLong(s3UploadTimeLimit) * 1000;
    } catch (Exception e) {
      LOG.error("Invalid S3_UPLOAD_TIME_INTERVAL");
    }
    LOG.info("Set s3 upload time interval as - " + s3UploadTimeIntervalMillis);
    nextS3UploadTimeInMillis = System.currentTimeMillis()
        + s3UploadTimeIntervalMillis;
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
    // Upload to S3 every one hour
    if (System.currentTimeMillis() > nextS3UploadTimeInMillis) {
      persistToS3();
      nextS3UploadTimeInMillis = System.currentTimeMillis()
          + S3_UPLOAD_TIME_INTERVAL_MILLIS;
    }
  }

  private void processRecordsWithRetries(List<Record> records) throws Exception {
    for (Record record : records) {
      String eventData = decoder.decode(record.getData()).toString();
      boolean processedSuccessfully = false;
      for (int i = 0; i < NUM_RETRIES; i++) {
        if (persistToLocal(record, eventData, eventName)) {
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

  boolean persistToLocal(Record record, String eventData,
      String eventName) {
    String fileName = path + getLocalFileName("stick_events");
    File file = new File(fileName);
    try {
      FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
      BufferedWriter bw = new BufferedWriter(fw);
      if (!file.exists())
        file.createNewFile();
      bw.write(eventData);
      bw.newLine();
      bw.close();
      return true;
    } catch(IOException e){
      LOG.error("Unable to write to file: " + fileName);
      return false;
    }
  }

  boolean persistToS3() {
    LOG.info("Persisting to S3");
    AmazonS3 s3client = new AmazonS3Client(new DefaultAWSCredentialsProviderChain());
    File[] files = new File(path).listFiles();
    for (File file : files) {
      if (file.isFile()) {
        s3client.putObject(new PutObjectRequest(s3BucketName, getS3FileName(file.getName()), file));
        LOG.info("S3 file upload completed - " + getS3FileName(file.getName()));
        file.delete();
        LOG.info("Local file deleted - " + file.getName());
      }
    }
    return true;
  }
  
  private String getS3FileName(String localFileName) {
    String s3FileName;
    s3FileName = localFileName.replace("@", new SimpleDateFormat("yyyy/MM/dd/HH").format(new Date()));
    s3FileName = s3FileName.replace("-", "/");
    LOG.info("S3 file name - " + s3FileName);
    return s3FileName;
  }

  private String getLocalFileName(String fileName) {
    String localFileName;
    localFileName = "events" + "-@-" + fileName;
    return localFileName;
  }
  
}
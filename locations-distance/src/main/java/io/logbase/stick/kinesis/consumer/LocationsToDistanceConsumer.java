package io.logbase.stick.kinesis.consumer;

import java.net.InetAddress;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class LocationsToDistanceConsumer {

  private static final Log LOG = LogFactory.getLog(LocationsToDistanceConsumer.class);
  public static final String STREAM_NAME = "stick-locations";
  private static final String CONSUMER_NAME = "LocationsToFirebaseConsumer";
  // Initial position in the stream when the application starts up for the first
  // time.
  // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest
  // available data)
  private static final InitialPositionInStream INITIAL_POSITION_IN_STREAM = InitialPositionInStream.TRIM_HORIZON;
  private static AWSCredentialsProvider credentialsProvider;

  private static void init() {
    // Ensure the JVM will refresh the cached IP values of AWS resources (e.g.
    // service endpoints).
    java.security.Security.setProperty("networkaddress.cache.ttl", "60");
    credentialsProvider = new DefaultAWSCredentialsProviderChain();
    try {
      credentialsProvider.getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials.", e);
    }
  }

  public static void main(String[] args) throws Exception {
    init();
    String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":"
        + UUID.randomUUID();
    KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
        CONSUMER_NAME, STREAM_NAME, credentialsProvider, workerId);
    kinesisClientLibConfiguration
        .withInitialPositionInStream(INITIAL_POSITION_IN_STREAM);
    IRecordProcessorFactory recordProcessorFactory = new RecordProcessorFactory();
    Worker worker = new Worker(recordProcessorFactory,
        kinesisClientLibConfiguration);

    LOG.info("Running consumer: " + CONSUMER_NAME + "|" + STREAM_NAME + "|"
        + workerId);
    int exitCode = 0;
    try {
      worker.run();
    } catch (Throwable t) {
      LOG.info("Caught error while running consumer: " + CONSUMER_NAME + "|"
          + STREAM_NAME + "|" + workerId);
      t.printStackTrace();
      exitCode = 1;
    }
    System.exit(exitCode);
  }

}

package org.apache.spark.shuffle.sort.io;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.S3IndexShuffleBlockResolver;
import org.apache.spark.shuffle.io.S3ShuffleReadSupport;
import org.apache.spark.shuffle.api.*;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.MapOutputTracker;
import org.apache.spark.serializer.SerializerManager;

import java.util.Map;
import java.util.Optional;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3ShuffleExecutorComponents implements ShuffleExecutorComponents {

    private final SparkConf sparkConf;
    private S3ShuffleReadSupport shuffleReadSupport; // From local disk read patch
    private BlockManager blockManager;
    private S3IndexShuffleBlockResolver blockResolver;

    private AmazonS3 s3Client;
    private String shuffleUUID;

    // Receive UUID from driver in sparkConf
    public S3ShuffleExecutorComponents(SparkConf sparkConf) {
        this.sparkConf = sparkConf;

        // Initialize S3 instance
        Regions region = Regions.fromName("us-east-1");
        s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    }

    @VisibleForTesting
    public S3ShuffleExecutorComponents(
            SparkConf sparkConf,
            BlockManager blockManager,
            S3IndexShuffleBlockResolver blockResolver,
            S3ShuffleReadSupport shuffleReadSupport) {
        this.sparkConf = sparkConf;
        this.blockManager = blockManager;
        this.blockResolver = blockResolver;
        this.shuffleReadSupport = shuffleReadSupport;

        // Initialize S3 instance
        Regions region = Regions.fromName("us-east-1");
        s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    }

    @Override
    public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {

        // Get value of shuffle UUID from extraConfigs here
        this.shuffleUUID = extraConfigs.get("shuffleUUID");

        blockManager = SparkEnv.get().blockManager();
        if (blockManager == null) {
            throw new IllegalStateException("No blockManager available from the SparkEnv.");
        }
        blockResolver = new S3IndexShuffleBlockResolver(sparkConf, blockManager);

        MapOutputTracker mapOutputTracker = SparkEnv.get().mapOutputTracker();
        SerializerManager serializerManager = SparkEnv.get().serializerManager();
        shuffleReadSupport = new S3ShuffleReadSupport(
                blockManager, mapOutputTracker, serializerManager, sparkConf);
    }

    @Override
    public ShuffleMapOutputWriter createMapOutputWriter(
            int shuffleId,
            long mapTaskId,
            int numPartitions) {

        return createMapOutputWriter(s3Client, shuffleId, mapTaskId, numPartitions);
    }

    public ShuffleMapOutputWriter createMapOutputWriter(AmazonS3 s3Client, int shuffleId,
                                                        long mapTaskId, int numPartitions) {
        if (blockResolver == null) {
            throw new IllegalStateException(
                    "Executor components must be initialized before getting writers.");
        }
        return new S3ShuffleMapOutputWriter(
                s3Client, sparkConf, shuffleId, mapTaskId, numPartitions, blockResolver, shuffleUUID);
    }

    @Override
    public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
            int shuffleId,
            long mapId) {
        if (blockResolver == null) {
            throw new IllegalStateException(
                    "Executor components must be initialized before getting writers.");
        }
        return Optional.of(new S3SingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
    }
}

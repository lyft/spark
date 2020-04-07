/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort.io;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.SingleSpillShuffleMapOutputWriter;
import org.apache.spark.storage.BlockManager;

import java.util.Map;
import java.util.Optional;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3ShuffleExecutorComponents implements ShuffleExecutorComponents {

  private final SparkConf sparkConf;
  private BlockManager blockManager;
  private IndexShuffleBlockResolver blockResolver;

  private AmazonS3 s3Client;

  public S3ShuffleExecutorComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;

    // Initialize S3 instance
    s3Client = new AmazonS3Client();
    Region region = Region.getRegion(Regions.US_EAST_1);
    s3Client.setRegion(region);
  }

  @VisibleForTesting
  public S3ShuffleExecutorComponents(
      SparkConf sparkConf,
      BlockManager blockManager,
      IndexShuffleBlockResolver blockResolver) {
    this.sparkConf = sparkConf;
    this.blockManager = blockManager;
    this.blockResolver = blockResolver;

    // Initialize S3 instance
    Region region = Region.getRegion(Regions.US_EAST_1);
    s3Client.setRegion(region);
  }

  @Override
  public void initializeExecutor(String appId, String execId, Map<String, String> extraConfigs) {
    blockManager = SparkEnv.get().blockManager();
    if (blockManager == null) {
      throw new IllegalStateException("No blockManager available from the SparkEnv.");
    }
    blockResolver = new IndexShuffleBlockResolver(sparkConf, blockManager);
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
            s3Client, shuffleId, mapTaskId, numPartitions, blockResolver, sparkConf);
  }

  @Override
  public Optional<SingleSpillShuffleMapOutputWriter> createSingleFileMapOutputWriter(
      int shuffleId,
      long mapId) {
    if (blockResolver == null) {
      throw new IllegalStateException(
          "Executor components must be initialized before getting writers.");
    }
    return Optional.of(new LocalDiskSingleSpillMapOutputWriter(shuffleId, mapId, blockResolver));
  }
}

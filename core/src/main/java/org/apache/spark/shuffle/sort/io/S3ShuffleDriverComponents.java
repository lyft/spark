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

import com.amazonaws.services.s3.model.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.storage.BlockManagerMaster;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import java.util.UUID;

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


import org.apache.spark.internal.config.package$;

public class S3ShuffleDriverComponents implements ShuffleDriverComponents {

  private final SparkConf sparkConf;
  private BlockManagerMaster blockManagerMaster;
  private AmazonS3 s3Client;
  private String s3Bucket;
  private String s3KeyPrefix;
  private String s3RegionCode;
  private final String shuffleUUID ; // Subdirectory in S3 for this job's shuffle files

  public S3ShuffleDriverComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
    shuffleUUID = "shuffle-" + UUID.randomUUID().toString();
    s3Bucket = sparkConf.get(package$.MODULE$.SHUFFLE_S3_BUCKET_NAME());
    s3KeyPrefix = sparkConf.get(package$.MODULE$.SHUFFLE_S3_PREFIX()) + shuffleUUID;
    s3RegionCode = sparkConf.get(package$.MODULE$.SHUFFLE_S3_REGION_CODE());
  }

  @Override
  public Map<String, String> initializeApplication() {
    blockManagerMaster = SparkEnv.get().blockManager().master();

    // Initialize S3 instance
    Regions region = Regions.fromName(s3RegionCode);

    s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    // Create temp file to set up in path
    File file = null;

    try {
      file = File.createTempFile("temp", null);
    } catch (IOException e) {
      e.printStackTrace();
    }
    String s3FileName = s3KeyPrefix + "/temp.txt";

    // Set up file path for subdirectors in S3
    try {
      s3Client.putObject(s3Bucket, s3FileName, file);
    } catch (AmazonS3Exception amazonS3Exception) {
      System.err.println(amazonS3Exception.getMessage());
      System.err.println("Run with \'aws-okta exec zimride-sudo-developer\'");
      System.exit(1); // Immediately exit if permissions are not granted
    } catch (AmazonServiceException e) {
      System.err.println(e.getErrorMessage());
    }

    file.deleteOnExit();

    System.out.println("Driver wrote to S3 at folder " + s3KeyPrefix);

    // Create Map to populate SparkConf with shuffle UUID for executors
    Map<String, String> uuidConfig = new HashMap<>();
    uuidConfig.put("shuffleUUID", shuffleUUID);

    return uuidConfig;
  }

  @Override
  public void cleanupApplication() {
    // S3 prefix spark-shuffle-poc/ set to 2-day lifecycle expiration policy
  }

  @Override
  public void removeShuffle(int shuffleId, boolean blocking) {
    if (blockManagerMaster == null) {
      throw new IllegalStateException("Driver components must be initialized before using");
    }
    blockManagerMaster.removeShuffle(shuffleId, blocking);
  }
}
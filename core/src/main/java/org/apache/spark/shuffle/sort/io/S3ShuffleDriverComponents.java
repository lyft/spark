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
import org.apache.spark.SparkContext;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.storage.BlockManagerMaster;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;


public class S3ShuffleDriverComponents implements ShuffleDriverComponents {

  private final SparkConf sparkConf;
  private BlockManagerMaster blockManagerMaster;
  private AmazonS3 s3Client;
  String s3Bucket;
  String s3Key;

  public S3ShuffleDriverComponents(SparkConf sparkConf) {
    this.sparkConf = sparkConf;
  }

  @Override
  public Map<String, String> initializeApplication() {
    blockManagerMaster = SparkEnv.get().blockManager().master();

    // Initialize S3 instance
    s3Client = new AmazonS3Client();
    s3Bucket = "data-team";
    Region region = Region.getRegion(Regions.US_EAST_1);
    s3Client.setRegion(region);
    s3Key = "awen/" + sparkConf.getAppId();

   //  Set up file path for subdirectors in S3
    try {
      s3Client.putObject(new PutObjectRequest(s3Bucket, s3Key, File.createTempFile("temp", null)));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return Collections.emptyMap();
  }

  @Override
  public void cleanupApplication() {
    // Clean up S3 subdirectory
    // s3Key = "awen/" + sparkConf.getAppId(); // Can't access app ID in initialize
    s3Client.deleteObject(new DeleteObjectRequest(s3Bucket, s3Key));
  }

  @Override
  public void removeShuffle(int shuffleId, boolean blocking) {
    if (blockManagerMaster == null) {
      throw new IllegalStateException("Driver components must be initialized before using");
    }
    blockManagerMaster.removeShuffle(shuffleId, blocking);
  }
}

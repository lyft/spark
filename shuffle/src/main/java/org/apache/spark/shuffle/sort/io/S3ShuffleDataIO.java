package org.apache.spark.shuffle.sort.io;

import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleDriverComponents;
import org.apache.spark.shuffle.api.ShuffleExecutorComponents;

/**
 * Implementation of the {@link ShuffleDataIO} plugin system that moves the local shuffle
 * storage and index file functionality from local disk to S3 cloud storage.
 */
public class S3ShuffleDataIO implements ShuffleDataIO {

    private final SparkConf sparkConf;

    public S3ShuffleDataIO(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
    }

    @Override
    public ShuffleExecutorComponents executor() {
        return new S3ShuffleExecutorComponents(sparkConf);
    }

    @Override
    public ShuffleDriverComponents driver() {
        // return new S3ShuffleDriverComponents(sparkConf, uuid);
        return new S3ShuffleDriverComponents(sparkConf);
    }
}

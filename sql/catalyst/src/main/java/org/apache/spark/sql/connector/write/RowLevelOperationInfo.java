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

<<<<<<<< HEAD:sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/colstats/HistogramBin.java
package org.apache.spark.sql.connector.read.colstats;

import org.apache.spark.annotation.Evolving;

/**
 * An interface to represent a bin in an equi-height histogram.
========
package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.write.RowLevelOperation.Command;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * An interface with logical information for a row-level operation such as DELETE, UPDATE, MERGE.
>>>>>>>> c93bba8b9d4823c0d891561e041eaec91be0c11b:sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/RowLevelOperationInfo.java
 *
 * @since 3.4.0
 */
<<<<<<<< HEAD:sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/colstats/HistogramBin.java
@Evolving
public interface HistogramBin {
  /**
   * @return lower bound of the value range in this bin
   */
  double lo();

  /**
   * @return higher bound of the value range in this bin
   */
  double hi();

  /**
   * @return approximate number of distinct values in this bin
   */
  long ndv();
========
@Experimental
public interface RowLevelOperationInfo {
  /**
   * Returns options that the user specified when performing the row-level operation.
   */
  CaseInsensitiveStringMap options();

  /**
   * Returns the row-level SQL command (e.g. DELETE, UPDATE, MERGE).
   */
  Command command();
>>>>>>>> c93bba8b9d4823c0d891561e041eaec91be0c11b:sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/RowLevelOperationInfo.java
}

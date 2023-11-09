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

<<<<<<<< HEAD:sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsDelta.java
package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A mix-in interface for {@link RowLevelOperation}. Data sources can implement this interface
 * to indicate they support handling deltas of rows.
========
package org.apache.spark.sql.connector.expressions;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.DataType;

/**
 * Represents a cast expression in the public logical expression API.
>>>>>>>> c93bba8b9d4823c0d891561e041eaec91be0c11b:sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/Cast.java
 *
 * @since 3.4.0
 */
<<<<<<<< HEAD:sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/SupportsDelta.java
@Experimental
public interface SupportsDelta extends RowLevelOperation {
  @Override
  DeltaWriteBuilder newWriteBuilder(LogicalWriteInfo info);

  /**
   * Returns the row ID column references that should be used for row equality.
   */
  NamedReference[] rowId();
========
@Evolving
public class Cast implements Expression, Serializable {
  private Expression expression;
  private DataType dataType;

  public Cast(Expression expression, DataType dataType) {
    this.expression = expression;
    this.dataType = dataType;
  }

  public Expression expression() { return expression; }
  public DataType dataType() { return dataType; }

  @Override
  public Expression[] children() { return new Expression[]{ expression() }; }
>>>>>>>> c93bba8b9d4823c0d891561e041eaec91be0c11b:sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/Cast.java
}

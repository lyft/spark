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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericRowWithSchema}
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Table}

/**
 * Physical plan node for showing table properties.
 */
case class ShowTablePropertiesExec(
    output: Seq[Attribute],
    catalogTable: Table,
    propertyKey: Option[String]) extends V2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    import scala.collection.JavaConverters._
    val toRow = RowEncoder(schema).resolveAndBind().createSerializer()

    // The reserved properties are accessible through DESCRIBE
    val properties = catalogTable.properties.asScala
      .filter { case (k, v) => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(k) }
    propertyKey match {
      case Some(p) =>
        val propValue = properties
          .getOrElse(p, s"Table ${catalogTable.name} does not have property: $p")
        Seq(toRow(new GenericRowWithSchema(Array(p, propValue), schema)).copy())
      case None =>
        properties.keys.map(k =>
          toRow(new GenericRowWithSchema(Array(k, properties(k)), schema)).copy()).toSeq
    }
  }
}

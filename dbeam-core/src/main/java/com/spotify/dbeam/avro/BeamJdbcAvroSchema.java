/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam.avro;

import com.spotify.dbeam.DBeamException;
import com.spotify.dbeam.args.JdbcExportArgs;
import com.spotify.dbeam.options.JobNameConfiguration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeamJdbcAvroSchema {

  private static Logger LOGGER = LoggerFactory.getLogger(BeamJdbcAvroSchema.class);

  /**
   * Create AVRO schema of the table currently being exported in this job by reading one record.
   * Also Exposes the time taken to create the schema as a counter.
   *
   * <p>The job name is set from here since it depends on the dbName
   */
  public static Schema createSchema(Pipeline pipeline, JdbcExportArgs args) throws DBeamException {
    Schema generatedSchema;
    String dbName;
    final long startTimeMillis = System.currentTimeMillis();
    try (Connection connection = args.createConnection()) {
      final String dbUrl = connection.getMetaData().getURL();
      final String avroDoc = args.avroDoc().orElseGet(() ->
          String.format("Generate schema from JDBC ResultSet from %s %s",
              args.queryBuilderArgs().tableName(),
              dbUrl));
      dbName = connection.getCatalog();
      generatedSchema = JdbcAvroSchema.createSchemaByReadingOneRow(
          connection, args.queryBuilderArgs().tableName(),
          args.avroSchemaNamespace(), avroDoc, args.useAvroLogicalTypes());
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());
      throw new DBeamException("Failed to create AVRO schema for given table", e);
    }
    final long elapsedTimeSchema = System.currentTimeMillis() - startTimeMillis;
    LOGGER.info("Elapsed time to schema {} seconds", elapsedTimeSchema / 1000.0);

    JobNameConfiguration.configureJobName(
        pipeline.getOptions(), dbName, args.queryBuilderArgs().tableName());
    final Counter cnt =
        Metrics.counter(BeamJdbcAvroSchema.class.getCanonicalName(),
            "schemaElapsedTimeMs");
    pipeline
        .apply("ExposeSchemaCountersSeed",
            Create.of(Collections.singletonList(0))
                .withType(TypeDescriptors.integers()))
        .apply("ExposeSchemaCounters",
            MapElements.into(TypeDescriptors.integers()).via(v -> {
              cnt.inc(elapsedTimeSchema);
              return v;
            }));
    return generatedSchema;
  }

}

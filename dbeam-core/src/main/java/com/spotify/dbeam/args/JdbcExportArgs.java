/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.dbeam.args;

import com.google.auto.value.AutoValue;
import com.spotify.dbeam.DBeamException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

@AutoValue
public abstract class JdbcExportArgs implements Serializable {

  public abstract JdbcAvroArgs jdbcAvroOptions();

  public abstract QueryBuilderArgs queryBuilderArgs();

  public abstract String avroSchemaNamespace();

  public abstract Optional<String> avroDoc();

  public abstract Boolean useAvroLogicalTypes();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setJdbcAvroOptions(JdbcAvroArgs jdbcAvroArgs);

    abstract Builder setQueryBuilderArgs(QueryBuilderArgs queryBuilderArgs);

    abstract Builder setAvroSchemaNamespace(String avroSchemaNamespace);

    abstract Builder setAvroDoc(Optional<String> avroDoc);

    abstract Builder setUseAvroLogicalTypes(Boolean useAvroLogicalTypes);

    abstract JdbcExportArgs build();
  }

  public static JdbcExportArgs create(JdbcAvroArgs jdbcAvroArgs,
                                      QueryBuilderArgs queryBuilderArgs) {
    return create(jdbcAvroArgs, queryBuilderArgs,
                  "dbeam_generated", Optional.empty(), false);
  }

  public static JdbcExportArgs create(JdbcAvroArgs jdbcAvroArgs,
                                      QueryBuilderArgs queryBuilderArgs,
                                      String avroSchemaNamespace,
                                      Optional<String> avroDoc,
                                      Boolean useAvroLogicalTypes) {
    return new AutoValue_JdbcExportArgs.Builder()
        .setJdbcAvroOptions(jdbcAvroArgs)
        .setQueryBuilderArgs(queryBuilderArgs)
        .setAvroSchemaNamespace(avroSchemaNamespace)
        .setAvroDoc(avroDoc)
        .setUseAvroLogicalTypes(useAvroLogicalTypes)
        .build();
  }

  public Connection createConnection() throws DBeamException {
    try {
      return this.jdbcAvroOptions().jdbcConnectionConfiguration().createConnection();
    } catch (SQLException | ClassNotFoundException e) {
      throw new DBeamException("Failed to create connection to Database:", e);
    }
  }

}

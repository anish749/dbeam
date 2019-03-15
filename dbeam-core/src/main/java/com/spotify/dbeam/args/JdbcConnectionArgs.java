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
import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.SQLException;
import javax.annotation.Nullable;

/**
 * A POJO describing a how to create a JDBC {@link Connection}.
 */
@AutoValue
public abstract class JdbcConnectionArgs implements Serializable {

  public abstract String driverClassName();

  public abstract String url();

  @Nullable abstract String username();

  @Nullable abstract String password();

  abstract Builder builder();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setDriverClassName(String driverClassName);

    abstract Builder setUrl(String url);

    abstract Builder setUsername(String username);

    abstract Builder setPassword(String password);

    abstract JdbcConnectionArgs build();
  }

  public static JdbcConnectionArgs create(String url)
      throws ClassNotFoundException {
    Preconditions.checkArgument(url != null,
                  "DataSourceConfiguration.create(driverClassName, url) called "
                  + "with null url");
    final String driverClassName = JdbcConnectionUtil.getDriverClass(url);
    return new AutoValue_JdbcConnectionArgs.Builder()
        .setDriverClassName(driverClassName)
        .setUrl(url)
        .build();
  }

  public JdbcConnectionArgs withUsername(String username) {
    return builder().setUsername(username).build();
  }

  public JdbcConnectionArgs withPassword(String password) {
    return builder().setPassword(password).build();
  }

  public Connection createConnection() throws SQLException, ClassNotFoundException {
    Class.forName(driverClassName());
    Connection connection = DriverManager.getConnection(url(), username(), password());
    connection.setAutoCommit(false);
    return connection;
  }
}

/**
 * Copyright (C) 2018-2020 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.hcommon.hive.metastore.paths;

import static com.hotels.hcommon.hive.metastore.util.LocationUtils.locationAsPath;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;

class UnpartitionedTablePathResolver implements TablePathResolver {

  private final Path tableBaseLocation;
  private final Path globPath;
  private final Table table;

  UnpartitionedTablePathResolver(Table table) {
    this.table = table;
    Path localTableBaseLocation = locationAsPath(table);
    tableBaseLocation = localTableBaseLocation.getParent();
    globPath = new Path(tableBaseLocation, "*");
  }

  @Override
  public Path getGlobPath() {
    return globPath;
  }

  @Override
  public Path getTableBaseLocation() {
    return tableBaseLocation;
  }

  @Override
  public Set<Path> getMetastorePaths(short batchSize) throws URISyntaxException {
    return Collections.singleton(PathUtils.normalise(new Path(new URI(table.getSd().getLocation()))));
  }
}

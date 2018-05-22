/**
 * Copyright (C) 2017-2018 Expedia Inc.
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
package com.hotels.hcommon.hive.metastore.core.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Test;

import com.hotels.hcommon.hive.metastore.core.util.FieldSchemaUtils;

public class FieldSchemaUtilsTest {

  @Test
  public void getFieldNames() {
    List<String> actual = FieldSchemaUtils
        .getFieldNames(Arrays.asList(new FieldSchema("a", "string", null), new FieldSchema("b", "string", null)));
    assertThat(actual, is(Arrays.asList("a", "b")));
  }
}

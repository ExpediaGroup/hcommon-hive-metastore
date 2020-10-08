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
package com.hotels.hcommon.hive.metastore.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HiveConfFactoryTest {

  private static final File CLASSPATH_DIR = new File("target/test-classes");
  private static final File CLASSPATH_CUSTOM_SITE_XML = new File(CLASSPATH_DIR, "custom-site.xml");

  private static final String CUSTOM_TEST_KEY = "custom.site.test.key";
  private static final String CUSTOM_TEST_VALUE = "custom.site.test.value";

  @Before
  public void before() throws IOException {
    if (CLASSPATH_CUSTOM_SITE_XML.exists()) {
      CLASSPATH_CUSTOM_SITE_XML.delete();
    }
    writeConf(CLASSPATH_CUSTOM_SITE_XML, CUSTOM_TEST_KEY, CUSTOM_TEST_VALUE);
  }

  @After
  public void after() {
    CLASSPATH_CUSTOM_SITE_XML.delete();
  }

  @Test
  public void nullResources() throws Exception {
    HiveConf hiveConf = new HiveConfFactory(null, null).newInstance();

    assertThat(hiveConf.get(CUSTOM_TEST_KEY), is(nullValue()));
  }

  @Test
  public void emptyResources() throws Exception {
    HiveConf hiveConf = new HiveConfFactory(Collections.<String> emptyList(), null).newInstance();

    assertThat(hiveConf.get(CUSTOM_TEST_KEY), is(nullValue()));
  }

  @Test
  public void customResources() throws Exception {
    HiveConf hiveConf = new HiveConfFactory(Arrays.asList(CLASSPATH_CUSTOM_SITE_XML.getName()), null).newInstance();

    assertThat(hiveConf.get(CUSTOM_TEST_KEY), is(CUSTOM_TEST_VALUE));
  }

  @Test
  public void additionalProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("a", "b");
    HiveConf hiveConf = new HiveConfFactory(null, properties).newInstance();

    assertThat(hiveConf.get("a"), is("b"));
  }

  private static void writeConf(File file, String key, String value) throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(key, value);
    try (FileWriter out = new FileWriter(file)) {
      conf.writeXml(out);
    }
  }
}

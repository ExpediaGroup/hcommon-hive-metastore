/**
 * Copyright (C) 2018 Expedia Inc.
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
package com.hotels.hcommon.hive.metastore.client.tunnelling;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.junit.Before;
import org.junit.Test;

public class MetastoreTunnelTest {

  private final MetastoreTunnel tunnel = new MetastoreTunnel();
  private final String knownHosts = "knownHosts";
  private final String privateKey = "privateKey";
  private final String route = "hostA -> hostB";
  private final int timeout = 123;

  private static Validator validator;

  @Before
  public void before() {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    validator = factory.getValidator();

    tunnel.setKnownHosts(knownHosts);
    tunnel.setPrivateKeys(privateKey);
    tunnel.setRoute(route);
    tunnel.setTimeout(timeout);
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void parametersAreAssignedCorrectly() {
    assertThat(tunnel.getKnownHosts(), is(knownHosts));
    assertThat(tunnel.getPrivateKeys(), is(privateKey));
    assertThat(tunnel.getRoute(), is(route));
    assertThat(tunnel.getTimeout(), is(timeout));
  }

  @Test
  public void infiniteTimeout() {
    tunnel.setTimeout(0);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void portTooHigh() {
    tunnel.setPort(65536);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void portTooLow() {
    tunnel.setPort(0);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullRoute() {
    tunnel.setRoute(null);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyRoute() {
    tunnel.setRoute("");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankRoute() {
    tunnel.setRoute(" ");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullKnownHosts() {
    tunnel.setKnownHosts(null);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyKnownHosts() {
    tunnel.setKnownHosts("");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankKnownHosts() {
    tunnel.setKnownHosts(" ");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullPrivateKey() {
    tunnel.setPrivateKeys(null);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyPrivateKey() {
    tunnel.setPrivateKeys("");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankPrivateKey() {
    tunnel.setPrivateKeys(" ");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativeTimeout() {
    tunnel.setTimeout(-1);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void timeoutTooHigh() {
    tunnel.setTimeout(Integer.MAX_VALUE + 1);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void strictHostKeyCheckingSetToYes() {
    tunnel.setStrictHostKeyChecking("yes");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void strictHostKeyCheckingSetToNo() {
    tunnel.setStrictHostKeyChecking("no");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void strictHostKeyCheckingSetToIncorrectValue() {
    tunnel.setStrictHostKeyChecking("foo");
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void strictHostKeyCheckingDefaultsToYes() {
    assertThat(tunnel.getStrictHostKeyChecking(), is("yes"));
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void getLocalHost() {
    String localHost = "localHost";
    tunnel.setLocalhost(localHost);
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
    assertThat(tunnel.getLocalhost(), is(localHost));
  }

  @Test
  public void isStrictHostKeyCheckingForYes() {
    tunnel.setStrictHostKeyChecking("yes");
    assertThat(tunnel.getIsStrictHostKeyChecking(), is(true));
  }

  @Test
  public void isStrictHostKeyCheckingForNo() {
    tunnel.setStrictHostKeyChecking("no");
    assertThat(tunnel.getIsStrictHostKeyChecking(), is(false));
  }
}

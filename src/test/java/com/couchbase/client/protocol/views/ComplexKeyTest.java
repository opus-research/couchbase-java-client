/**
 * Copyright (C) 2012-2012 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.client.protocol.views;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the creation of complex keys for views.
 */
public class ComplexKeyTest {

  public ComplexKeyTest() {
  }

  @BeforeClass
  public static void setUpClass() {
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  /**
   * Test of of method, of class ComplexKey.
   */
  @Test
  public void testOf() {
    System.out.println("of");
    String expResult = "[2012,9,7]";
    ComplexKey result = ComplexKey.of(2012, 9, 7);
    assertEquals(expResult, result.toJson());
  }

  /**
   * Test of of method, of class ComplexKey.
   */
  @Test
  public void testOfEmptyArray() {
    System.out.println("ofEmptyArray");
    String expResult = "[]";
    ComplexKey result = ComplexKey.of(ComplexKey.emptyArray());
    assertEquals(expResult, result.toJson());
  }

  /**
   * Test of of method, of class ComplexKey.
   */
  @Test
  public void testOfEmptyObject() {
    System.out.println("ofEmptyArray");
    String expResult = "{}";
    ComplexKey result = ComplexKey.of(ComplexKey.emptyObject());
    assertEquals(expResult, result.toJson());
  }

  /**
   * Test of emptyArray method, of class ComplexKey.
   */
  @Test
  public void testEmptyArray() {
    System.out.println("emptyArray");
    Object[] expResult = new Object[] {};
    Object[] result = ComplexKey.emptyArray();
    assertArrayEquals(expResult, result);
  }

}

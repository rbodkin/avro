/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.AvroTestUtil;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDataFileTools {
  static final int COUNT = 10;
  static File sampleFile;
  static String jsonData;
  static Schema schema;
  static File schemaFile;
  
  @BeforeClass
  public static void writeSampleFile() throws IOException {
    sampleFile = AvroTestUtil.tempFile(
      TestDataFileTools.class.getName() + ".avro");
    schema = Schema.create(Type.INT);
    schemaFile = File.createTempFile("schema-temp", "schema");
    FileWriter fw = new FileWriter(schemaFile);
    fw.append(schema.toString());
    fw.close();
    
    DataFileWriter<Object> writer
      = new DataFileWriter<Object>(new GenericDatumWriter<Object>(schema))
      .create(schema, sampleFile);
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < COUNT; ++i) {
      builder.append(Integer.toString(i));
      builder.append("\n");
      writer.append(i);
    }

    writer.flush();
    writer.close();
    
    jsonData = builder.toString();
  }
  
  @Test
  public void testRead() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(baos);
    new DataFileReadTool().run(
        null, // stdin
        p, // stdout
        null, // stderr
        Arrays.asList(sampleFile.getPath()));
    assertEquals(jsonData.toString(), baos.toString("UTF-8").
        replace("\r", ""));        
  }
  
  @Test
  public void testGetSchema() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(baos);
    new DataFileGetSchemaTool().run(
        null, // stdin
        p, // stdout
        null, // stderr
        Arrays.asList(sampleFile.getPath()));
    assertEquals(schema.toString() + "\n",
        baos.toString("UTF-8").replace("\r", ""));
  }
  
  @Test
  public void testWriteWithDeflate() throws Exception {
    testWrite("deflate", Arrays.asList("--codec", "deflate"), "deflate");
  }
  
  @Test
  public void testWrite() throws Exception {
    testWrite("plain", Collections.<String>emptyList(), "null");
  }
  
  public void testWrite(String name, List<String> extra, String expectedCodec) 
      throws Exception {
      testWrite(name, extra, expectedCodec, "-schema", schema.toString());
      testWrite(name, extra, expectedCodec, "-schema-file", schemaFile.toString());
  }
  
  public void testWrite(String name, List<String> extra, String expectedCodec, String... extraArgs) 
  throws Exception {
    File outFile = AvroTestUtil.tempFile(
        TestDataFileTools.class + ".testWrite." + name + ".avro");
    FileOutputStream fout = new FileOutputStream(outFile);
    PrintStream out = new PrintStream(fout);
    List<String> args = new ArrayList<String>();
    args.add("-");
    for (String arg : extraArgs) {
        args.add(arg);
    }    
    args.addAll(extra);
    new DataFileWriteTool().run(
        new StringInputStream(jsonData),
        new PrintStream(out), // stdout
        null, // stderr
        args);
    out.close();
    fout.close();
    
    // Read it back, and make sure it's valid.
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    DataFileReader<Object> fileReader = new DataFileReader<Object>(outFile,reader);
    int i = 0;
    for (Object datum : fileReader) {
      assertEquals(i, datum);
      i++;
    }
    assertEquals(COUNT, i);
    assertEquals(schema, fileReader.getSchema());
    String codecStr = fileReader.getMetaString("avro.codec");
    if (null == codecStr) {
      codecStr = "null";
    }
    assertEquals(expectedCodec, codecStr);
  }
  
  @Test
  public void testFailureOnWritingPartialJSONValues() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    try {
      new DataFileWriteTool().run(
          new StringInputStream("{"),
          new PrintStream(out), // stdout
          null, // stderr          
          Arrays.asList("-schema", "{ \"type\":\"record\", \"fields\":" +
                        "[{\"name\":\"foo\", \"type\":\"string\"}], " +
                        "\"name\":\"boring\" }", "-"));
      fail("Expected exception.");
    } catch (IOException expected) {
      // expected
    }
  }
  
  @Test
  public void testWritingZeroJsonValues() throws Exception {
    File outFile = writeToAvroFile("zerojsonvalues",
        schema.toString(),
        "");
    assertEquals(0, countRecords(outFile));
  }
  
  private int countRecords(File outFile) throws IOException {
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    DataFileReader<Object> fileReader = 
      new DataFileReader<Object>(outFile,reader);
    int i = 0;
    for (@SuppressWarnings("unused") Object datum : fileReader) {
      i++;
    }
    return i;
  }

  @Test
  public void testDifferentSeparatorsBetweenJsonRecords() throws Exception {
    File outFile = writeToAvroFile(
        "seperators", 
        "{ \"type\":\"array\", \"items\":\"int\" }", 
        "[]    [] []\n[][3]     ");
    assertEquals(5, countRecords(outFile));
  }
  
  public File writeToAvroFile(String testName, String schema, String json) throws Exception {
    File outFile = AvroTestUtil.tempFile(
        TestDataFileTools.class + "." + testName + ".avro");
    FileOutputStream fout = new FileOutputStream(outFile);
    PrintStream out = new PrintStream(fout);
    new DataFileWriteTool().run(
        new StringInputStream(json),
        new PrintStream(out), // stdout
        null, // stderr
        Arrays.asList("-schema", schema, "-"));
    out.close();
    fout.close();
    return outFile;
  }

  @Test
  public void testReadJsonInputText() throws Exception {
    String schema = "{ \"type\":\"record\", \"name\":\"TestRecord\", \"fields\": [ "+
        "{\"name\":\"intval\",\"type\":\"int\"}, " +
        "{\"name\":\"strval\",\"type\":[\"string\", \"null\"]}]}";
    String jsonData = "{\"intval\":12}\n{\"intval\":-73,\"strval\":\"hello, there!!\"}\n";
    File schemaFile =  AvroTestUtil.tempFile(
        TestDataFileTools.class + ".testReadJsonInputText.avsc");
    PrintWriter writer = new PrintWriter(new FileWriter(schemaFile));
    writer.print(schema);
    writer.close();
    
    for (String codec : new String[] { null, "deflate" }) {
      File outFile = AvroTestUtil.tempFile(
          TestDataFileTools.class + ".testReadJsonInputText.avro");
      FileOutputStream fout = new FileOutputStream(outFile);
      PrintStream out = new PrintStream(fout);
      List<String> args = new ArrayList<String>(Arrays.asList("-", "-", "-schema", schemaFile.getPath()));
      if (codec != null) {
        args.add("-codec");
        args.add(codec);
        args.add("-level");
        args.add("5");
      }
      new FromJsonTextTool().run(
          new StringInputStream(jsonData),
          new PrintStream(out), // stdout
          null, // stderr
          args );
      out.close();
      fout.close();
      
      // Read it back, and make sure it's valid.
      GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
      DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(outFile,reader);
      int i = 0;
      for (GenericRecord datum : fileReader) {
        Object str = datum.get("strval");
        int val = (Integer)datum.get("intval");
        if (i==0) {
          assertNull(str);
          assertEquals(12, val);
        } else {
          assertEquals(new Utf8("hello, there!!"), str);
          assertEquals(-73, val);
        }
        i++;
      }
      assertEquals(2, i);
      assertEquals(Schema.parse(schema), fileReader.getSchema());
      String codecStr = fileReader.getMetaString("avro.codec");
      if (null == codecStr) {
        codecStr = "null";
      }
      assertEquals((codec==null ? "null" : codec), codecStr);
    }
  }
}

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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Reads a text file into an Avro data file.
 * 
 * Can accept a file name, and HDFS file URI, or stdin. Can write to a file name, an HDFS URI, or stdout.
 */
public class FromJsonTextTool implements Tool {
    private static final String TEXT_FILE_SCHEMA = "\"bytes\"";

    @Override
    public String getName() {
        return "fromtext";
    }

    @Override
    public String getShortDescription() {
        return "Imports a text file into an avro data file.";
    }

    @Override
    public int run(InputStream stdin, PrintStream out, PrintStream err, List<String> args) throws Exception {

        OptionParser p = new OptionParser();
        OptionSpec<Integer> level = p.accepts("level", "compression level").withOptionalArg().ofType(Integer.class);

        OptionSpec<String> file = p.accepts("schema", "Schema File").withRequiredArg().ofType(String.class);

        OptionSet opts = p.parse(args.toArray(new String[0]));

        String schemaFile = file.value(opts);        
        if (opts.nonOptionArguments().size() != 2 || schemaFile == null) {
            err.println("Expected 2 args: from_file to_file (local filenames," + " Hadoop URI's, or '-' for stdin/stdout) -schema <file>");
            p.printHelpOn(err);
            return 1;
        }

        BufferedInputStream inStream = Util.fileOrStdin(args.get(0), stdin);
        BufferedOutputStream outStream = Util.fileOrStdout(args.get(1), out);

        int compressionLevel = 1; // Default compression level
        if (opts.hasArgument(level)) {
            compressionLevel = level.value(opts);
        }

        Schema schema = Schema.parse(readSchemaFromFile(schemaFile));
        
        DataFileWriter<Object> writer = new DataFileWriter<Object>(new GenericDatumWriter<Object>());
        writer.setCodec(CodecFactory.deflateCodec(compressionLevel));
        writer.create(schema, outStream);

        ObjectMapper m = new ObjectMapper();
        Reader reader = new InputStreamReader(inStream);
        BufferedReader br = new BufferedReader(reader);
        //JsonFactory jsonFactory = new JsonFactory();
        
        for (;;) {
            JsonNode rootNode;
            String line = null;
            try {
                //Jackson doesn't like trying to parse a series of independent JSON objects as an array, so we instead rely on parsing one per line and don't allow line breaks within
                line = br.readLine();
                if (line == null)
                    break;
//                JsonParser parser = jsonFactory.createJsonParser(reader);
//                parser.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
//                rootNode = m.readValue(parser, TypeFactory.type(JsonNode.class));
                rootNode = m.readTree(line);
            } catch (IOException e) {
                System.err.println("Exception parsing: "+line);
                e.printStackTrace();
                break;
            }
    
            if (rootNode.isArray()) {
                for (int i=0; i<rootNode.size(); i++) {
                    GenericRecord r = recordFromNode(rootNode.get(i), schema, line);
                    writer.append(r);
                }
            } else if (rootNode.isObject()) {
                GenericRecord r = recordFromNode(rootNode, schema, line);
                writer.append(r);
            } else {
                System.err.println("no container?");
            }
        }

        writer.flush();
        writer.close();
        inStream.close();
        return 0;
    }

    private GenericRecord recordFromNode(JsonNode node, Schema schema, String container) {
        schema = getObject(schema);
        GenericRecord r = new GenericData.Record(schema);
        Iterator<JsonNode> it = node.getElements();
        Iterator<String> itn = node.getFieldNames();
        while (it.hasNext()) {
            JsonNode child = it.next();
            String name = itn.next();
            Field field = schema.getField(name);
            if (field == null) {
                System.err.println("Warning: skipping unmapped field "+name+" contained in "+container);
                continue;
            }
            Schema childSchema = field.schema();
            if (child.isArray()) {
                GenericArray o = recordFromArrayNode(child, childSchema, name);
                r.put(name, o);
                
            }
            else if (child.isObject()) {
                GenericRecord o = recordFromNode(child, childSchema, name);
                r.put(name, o);
            }
            else if (child.isInt()) {
                if (acceptsInt(childSchema)) {
                    r.put(name, child.getIntValue());
                } else if (acceptsLong(childSchema)) {
                    r.put(name, (long)child.getIntValue());                    
                } else if (acceptsFloat(childSchema)) {
                    r.put(name, (float)child.getIntValue());
                } else if (acceptsDouble(childSchema)) {
                    r.put(name, (double)child.getIntValue());
                } else {
                    System.err.println("Can't store an int in field "+name);
                }
            }
            else if (child.isLong()) {
                if (acceptsLong(childSchema)) {
                    r.put(name, (long)child.getLongValue());                    
                } else if (acceptsFloat(childSchema)) {
                    r.put(name, (float)child.getLongValue());
                } else if (acceptsDouble(childSchema)) {
                    r.put(name, (double)child.getLongValue());
                } else {
                    System.err.println("Can't store a long in field "+name);
                }
            }
            else if (child.isBinary()) {
                try {
                    r.put(name, child.getBinaryValue());
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            else if (child.isBoolean()) {
                r.put(name, child.getBooleanValue());
            }
            else if (child.isDouble()) {
                if (acceptsDouble(childSchema)) {
                    r.put(name, child.getDoubleValue());
                } else if (acceptsFloat(childSchema)) {
                    r.put(name, (float)child.getDoubleValue());
                } else {
                    System.err.println("Can't store a double in field "+name);
                }
            }
            else if (child.isTextual()) {
                r.put(name, new org.apache.avro.util.Utf8(child.getTextValue()));
            }
        }
        return r;
    }
    
    private GenericArray recordFromArrayNode(JsonNode node, Schema schema, String container) {
        schema = getArray(schema);
        GenericArray r = new GenericData.Array(node.size(), schema);
        Schema childSchema = schema.getElementType();
        for (int i=0; i<node.size(); i++) {
            JsonNode child = node.get(i);
            if (child.isArray()) {
                GenericArray o = recordFromArrayNode(child, childSchema, container);
                r.add(o);
            }
            else if (child.isObject()) {
                GenericRecord o = recordFromNode(child, childSchema, container);
                r.add(o);
            }
            else if (child.isInt()) {
                if (acceptsInt(childSchema)) {
                    r.add(child.getIntValue());
                } else if (acceptsLong(childSchema)) {
                    r.add((long)child.getIntValue());                    
                } else if (acceptsFloat(childSchema)) {
                    r.add((float)child.getIntValue());
                } else if (acceptsDouble(childSchema)) {
                    r.add((double)child.getIntValue());
                } else {
                    System.err.println("Can't store an int in field "+childSchema);
                }
            }
            else if (child.isLong()) {
                if (acceptsLong(childSchema)) {
                    r.add((long)child.getLongValue());                    
                } else if (acceptsFloat(childSchema)) {
                    r.add((float)child.getLongValue());
                } else if (acceptsDouble(childSchema)) {
                    r.add((double)child.getLongValue());
                } else {
                    System.err.println("Can't store a long in field "+childSchema);
                }
            }
            else if (child.isBinary()) {
                try {
                    r.add(child.getBinaryValue());
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            else if (child.isBoolean()) {
                r.add(child.getBooleanValue());
            }
            else if (child.isDouble()) {
                if (acceptsDouble(childSchema)) {
                    r.add(child.getDoubleValue());
                } else if (acceptsFloat(childSchema)) {
                    r.add((float)child.getDoubleValue());
                } else {
                    System.err.println("Can't store a double in field "+childSchema);
                }
            }
            else if (child.isTextual()) {
                r.add(new org.apache.avro.util.Utf8(child.getTextValue()));
            }
        }
        return r;
    }

    private Schema getObject(Schema schema) {
        if (schema.getType() == Schema.Type.RECORD) return schema;
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() == Schema.Type.RECORD) {
                    return s;
                }
            }
        }
        return null;
    }

    private Schema getArray(Schema schema) {
        if (schema.getType() == Schema.Type.ARRAY) return schema;
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema s : schema.getTypes()) {
                if (s.getType() == Schema.Type.ARRAY) {
                    return s;
                }
            }
        }
        return null;
    }
    
    private boolean accepts(Schema childSchema, Schema.Type type) {
        if (childSchema.getType() == type) return true;
        if (childSchema.getType() == Schema.Type.UNION) {
            for (Schema s : childSchema.getTypes()) {
                if (s.getType() == type) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean acceptsFloat(Schema childSchema) {
        return accepts(childSchema, Schema.Type.FLOAT);
    }
    
    private boolean acceptsDouble(Schema childSchema) {
        return accepts(childSchema, Schema.Type.DOUBLE);
    }
        
    private boolean acceptsLong(Schema childSchema) {
        return accepts(childSchema, Schema.Type.LONG);
    }

    private boolean acceptsInt(Schema childSchema) {
        return accepts(childSchema, Schema.Type.INT);
    }
    
    public static void main(String args[]) throws Exception {
        new FromJsonTextTool().run(System.in, System.out, System.err, Arrays.asList(args));
    }
    
    public static String readSchemaFromFile(String schemafile) throws FileNotFoundException, IOException {
        String schemastr;
        StringBuilder b = new StringBuilder();
        FileReader r = new FileReader(schemafile);
        try {
            char[] buf = new char[64*1024];
            for(;;) {
                int read = r.read(buf);
                if (read==-1) break;
                b.append(buf, 0, read);
            }
            schemastr = b.toString();
        } finally {
            r.close();
        }
        return schemastr;
      }
}

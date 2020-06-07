/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ebyhr.presto.flex;

import com.google.common.base.Strings;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import com.google.common.io.Resources;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import org.apache.poi.extractor.POIOLE2TextExtractor;
import org.apache.poi.extractor.POITextExtractor;
import org.apache.poi.hsmf.exceptions.ChunkNotFoundException;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.ooxml.extractor.ExtractorFactory;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.hslf.extractor.PowerPointExtractor;
import org.apache.xmlbeans.XmlException;
import org.ebyhr.presto.flex.operator.FilePlugin;
import org.ebyhr.presto.flex.operator.PluginFactory;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;
import com.auxilii.msgparser.Message;
import com.auxilii.msgparser.MsgParser;

import java.io.*;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;

public class FlexRecordCursor
        implements RecordCursor
{
    private final List<FlexColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator lines;
    private final long totalBytes;
    private final FilePlugin plugin;

    private List<String> fields;

    public FlexRecordCursor(List<FlexColumnHandle> columnHandles, SchemaTableName schemaTableName)
    {
        this.columnHandles = columnHandles;
        this.plugin = PluginFactory.create(schemaTableName.getSchemaName());

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            FlexColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        String tblName = schemaTableName.getTableName();
        URI uri = null;
        ByteSource byteSource;

        try {
            byteSource = Resources.asByteSource(URI.create(tblName).toURL());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e.getMessage());
        }

        //powerpoint
        if (tblName.endsWith(".ppt") || tblName.endsWith(".pptx") || tblName.contains(".ppt?") || tblName.contains(".pptx?")) {
            PowerPointExtractor powerPointExtractor = null;
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
                try {
                    powerPointExtractor = new PowerPointExtractor(input);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                ArrayList<String> lst = new ArrayList<>();
                lst.add(powerPointExtractor.getText());
                String ntes = powerPointExtractor.getNotes();
                if (ntes != null && ntes.length() > 0)
                    lst.add(ntes);
                lines = lst.iterator();
                totalBytes = input.getCount();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                try { if (powerPointExtractor != null)
                        powerPointExtractor.close();
                    } catch (Exception e) { /* ignored */ }
            }
        }
        //word doc
        else if (tblName.endsWith(".doc") || tblName.endsWith(".docx") || tblName.contains(".doc?") || tblName.contains(".docx?")) {
            WordExtractor wordExtractor = null;
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
                try {
                    wordExtractor = new WordExtractor(input);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                ArrayList<String> lst = new ArrayList<>();
                String[] paragraphText = wordExtractor.getParagraphText();
                for (String paragraph : paragraphText) {
                    lst.add(paragraph.replaceAll("\u0007", "").replaceAll("\f", "").replaceAll("\r", "").replaceAll("\n", "").replaceAll("\u0015", ""));
                }
                String hdr = wordExtractor.getHeaderText();
                if (hdr != null && hdr.length() > 0)
                    lst.add(hdr);
                String ftr = wordExtractor.getFooterText();
                if (ftr != null && ftr.length() > 0)
                    lst.add(ftr);
                lines = lst.iterator();
                totalBytes = input.getCount();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                try { if (wordExtractor != null)
                    wordExtractor.close();
                } catch (Exception e) { /* ignored */ }
            }
        }
        //pdf
        else if (tblName.endsWith(".pdf") || tblName.contains(".pdf?")) {
            PDDocument pddDocument = null;
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
                try {
                    pddDocument = PDDocument.load(input);
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e.getMessage());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                PDFTextStripper textStripper = null;
                try {
                    textStripper = new PDFTextStripper();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                String rawText = null;
                try {
                    rawText = textStripper.getText(pddDocument);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

                // split by whitespace
                String rawLines[] = rawText.split("\\r?\\n");
                ArrayList<String> lst = new ArrayList<>();
                for (String line : rawLines) {
                    lst.add(line);
                }
                lines = lst.iterator();
                totalBytes = input.getCount();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                try { if (pddDocument != null)
                    pddDocument.close();
                } catch (Exception e) { /* ignored */ }
            }
        }
        //outlook email (unlikely to get from http though!)
        else if (tblName.endsWith(".msg")) {
            MsgParser msgp = null;
            Message msg = null;
            String from_email = null;
            String from_name = null;
            String subject = null;
            String body = null;
            String to_list = null;
            String cc_list = null;
            String bcc_list = null;
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
                try {
                    msgp = new MsgParser();
                    msg = msgp.parseMsg(input);
                    from_email = msg.getFromEmail();
                    from_name = msg.getFromName();
                    subject = msg.getSubject();
                    body = msg.getBodyText();
                    to_list = msg.getDisplayTo();
                    cc_list = msg.getDisplayCc();
                    bcc_list = msg.getDisplayBcc();
                } catch (Exception e) {
                    throw new RuntimeException(e.getMessage());
                }
                ArrayList<String> lst = new ArrayList<>();
                lst.add("Attachments -" + msg.getAttachments().size());
                lst.add("from_email " + from_email);
                lst.add("from_name " + from_name);
                lst.add("to_list " + to_list);
                lst.add("cc_list " + cc_list);
                lst.add("bcc_list " + bcc_list);
                lst.add("subject " + subject);
                lst.add("body " + body);
                lines = lst.iterator();
                totalBytes = input.getCount();

            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        //zip
        else if (tblName.endsWith(".zip") || tblName.contains(".zip?")) {
            ArrayList<String> lst = new ArrayList<>();
            BufferedReader bufferedeReader = null;
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream());
                 ZipInputStream zin = new ZipInputStream(input);
                 InputStreamReader isr = new InputStreamReader(zin)) {

                    ZipEntry entry;
                    bufferedeReader = new BufferedReader(isr);
                    while ((entry = zin.getNextEntry()) != null) {
                        String line = bufferedeReader.readLine();
                        while (line != null) {
                            lst.add(line);
                            line = bufferedeReader.readLine();
                        }
                    }
                    lines = lst.iterator();
                    totalBytes = input.getCount();
            }  catch (IOException e) {
                throw new UncheckedIOException(e);
            }  catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            } finally {
                try { if (bufferedeReader != null)
                    bufferedeReader.close();
                } catch (Exception e) { /* ignored */ }
            }
        }
        //gz
        else if (tblName.endsWith(".gz") || tblName.endsWith(".gzip") || tblName.contains(".gz?") || tblName.contains(".gzip?")) {
            //getting unreadable compressed data back right now!
            //todo need to fix with https://stackoverflow.com/a/11093226/8874837 https://www.rgagnon.com/javadetails/java-HttpUrlConnection-with-GZIP-encoding.html
            ArrayList<String> lst = new ArrayList<>();
            BufferedReader in = null;
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
                in = new BufferedReader(new InputStreamReader(new GZIPInputStream(input)));
                String inputLine;
                while ((inputLine = in.readLine()) != null){
                    lst.add(inputLine);
                }
                lines = lst.iterator();
                totalBytes = input.getCount();
            }  catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                try { if (in != null)
                    in.close();
                } catch (Exception e) { /* ignored */ }
            }
        }
        //bz2
        else if (tblName.endsWith(".bz2") || tblName.endsWith(".bzip2") || tblName.contains(".bz2?") || tblName.contains(".bzip2?")) {
            ArrayList<String> lst = new ArrayList<>();
            BufferedReader in = null;
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
                in = new BufferedReader(new InputStreamReader(new MultiStreamBZip2InputStream(input)));
                String inputLine;
                while ((inputLine = in.readLine()) != null){
                    lst.add(inputLine);
                }
                lines = lst.iterator();
                totalBytes = input.getCount();
            }  catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                try { if (in != null)
                    in.close();
                } catch (Exception e) { /* ignored */ }
            }
        }
        else {
            //text/csv..etc
            try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
                lines = plugin.getIterator(byteSource);
                if (plugin.skipFirstLine()) {
                    lines.next();
                }
                totalBytes = input.getCount();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        fields = plugin.splitToList(lines);
        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}

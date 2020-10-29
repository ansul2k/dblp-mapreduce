package map_reduce;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;


/**
 * An implementation of XML input format, Using Mahout Standard Implementation and from example from :https://github.com/Mohammed-siddiq/hadoop-XMLInputFormatWithMultipleTags
 */
public class XmlInputFormatWithMultipleTags extends TextInputFormat {

    public static final String START_TAG_KEYS = "xmlinput.start";
    public static final String END_TAG_KEYS = "xmlinput.end";


    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) {
        return new XmlRecordReader();
    }

    public static class XmlRecordReader extends RecordReader<LongWritable, Text> {
        private byte[][] startTags;
        private byte[][] endTags;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();


        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;

            String[] sTags = tac.getConfiguration().get(START_TAG_KEYS).split(",");

            String[] eTags = tac.getConfiguration().get(END_TAG_KEYS).split(",");
            startTags = new byte[sTags.length][];
            endTags = new byte[sTags.length][];
            for (int i = 0; i < sTags.length; i++) {
                startTags[i] = sTags[i].getBytes(StandardCharsets.UTF_8);
                endTags[i] = eTags[i].getBytes(StandardCharsets.UTF_8);

            }
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();

            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);


        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                // The code transfer from here to findtag to pick one of the start tags value.
                int res = findTag(startTags, false);
                if (res != -1) {
                    try {

                        buffer.write(startTags[res - 1]);
                        //Reading the Content of XML until one of the end tags are encountered
                        int res1 = findTag(endTags, true);
                        if (res1 != -1) {
                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;


        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }

        private int findTag(byte[][] match, boolean withinBlock) throws IOException {
            int xmltag1 = 0, xmltag2 = 0, xmltag3 = 0, xmltag4 = 0, xmltag5 = 0, xmltag6 = 0, xmltag7 = 0, xmltag8 = 0,xmltag9 = 0,xmltag10 = 0;
            while (true) {
                int readbits = fsin.read();
                if (readbits == -1) return -1;
                if (withinBlock) buffer.write(readbits);


                if (readbits == match[0][xmltag1]) {
                    xmltag1++;
                    if (xmltag1 >= match[0].length) return 1;
                } else xmltag1 = 0;

                if (readbits == match[1][xmltag2]) {
                    xmltag2++;
                    if (xmltag2 >= match[1].length) return 2;
                } else xmltag2 = 0;

                if (readbits == match[2][xmltag3]) {
                    xmltag3++;
                    if (xmltag3 >= match[2].length) return 3;
                } else xmltag3 = 0;

                if (readbits == match[3][xmltag4]) {
                    xmltag4++;
                    if (xmltag4 >= match[3].length) return 4;
                } else xmltag4 = 0;

                if (readbits == match[4][xmltag5]) {
                    xmltag5++;
                    if (xmltag5 >= match[4].length) return 5;
                } else xmltag5 = 0;

                if (readbits == match[5][xmltag6]) {
                    xmltag6++;
                    if (xmltag6 >= match[5].length) return 6;
                } else xmltag6 = 0;

                if (readbits == match[6][xmltag7]) {
                    xmltag7++;
                    if (xmltag7 >= match[6].length) return 7;
                } else xmltag7 = 0;
                // added support for more tags
                if (readbits == match[7][xmltag8]) {
                    xmltag8++;
                    if (xmltag8 >= match[7].length) return 8;
                } else xmltag8 = 0;

                if (readbits == match[8][xmltag9]) {
                    xmltag9++;
                    if (xmltag9 >= match[8].length) return 9;
                } else xmltag9 = 0;

                if (readbits == match[9][xmltag10]) {
                    xmltag10++;
                    if (xmltag10 >= match[9].length) return 10;
                } else xmltag10 = 0;

                // untill eof
                if (!withinBlock && (xmltag1 == 0 && xmltag2 == 0 && xmltag3 == 0 && xmltag4 == 0 && xmltag5 == 0 && xmltag6 == 0 && xmltag7 == 0 && xmltag8 == 0 && xmltag9 == 0 && xmltag10 == 0) && fsin.getPos() >= end)
                    return -1;
            }
        }

    }


}
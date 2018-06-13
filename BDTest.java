package test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class BDTest extends Configured implements Tool {

    public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

        private static final FloatWritable realValue = new FloatWritable(1);
        private final Text solverKey = new Text();

        private final int SOLVER_IDX = 0;
        private final int REAL_IDX = 11;
        private final int RESULT_IDX = 14;

        private final String SOLVED_STATUS = "solved";

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> outputCollector, Reporter reporter) throws IOException {

            String[] lines = value.toString().split("\n");

            for (String line : lines) {
                String[] row = line.split("\t");

                String result = row[RESULT_IDX];

                if (result.equals(SOLVED_STATUS)) {
                    System.out.println(row[SOLVER_IDX] + " -- " + row[REAL_IDX]);

                    float real = Float.parseFloat(row[REAL_IDX]);

                    solverKey.set(row[SOLVER_IDX]);
                    realValue.set(real);

                    outputCollector.collect(solverKey, realValue);
                }
            }
        }
    }

    public static class MyReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            ArrayList sortedValues = new ArrayList<FloatWritable>();

            while(values.hasNext()) {
                sortedValues.add(values.next().get());
            }

            Collections.sort(sortedValues);

            result.set(StringUtils.join(' ', sortedValues));

            outputCollector.collect(key, result);
        }
    }

    public static class TransposeMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> outputCollector, Reporter reporter) throws IOException {

        }
    }

    public static class TransposeReducer extends MapReduceBase implements Reducer<LongWritable, Text, Text, NullWritable> {
        @Override
        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<Text, NullWritable> outputCollector, Reporter reporter) throws IOException {

        }
    }

        @Override
    public int run(String[] args) throws Exception {
        System.out.println("Run program");

        Configuration conf = getConf();
        JobConf job = new JobConf(conf, BigDataTest.class);
        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setJobName("BDTest");
        job.setJarByClass(BigDataTest.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setInputFormat(TextInputFormat.class);

        job.setOutputFormat(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.set("key.value.separator.in.input.line", ",");


        JobClient.runJob(job);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BigDataTest(), args);
        System.exit(res);
    }
}

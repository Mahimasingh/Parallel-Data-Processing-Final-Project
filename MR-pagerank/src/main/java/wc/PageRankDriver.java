package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class PageRankDriver extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PageRankDriver.class);

    @Override
    public int run(final String[] args) throws Exception {
        boolean success;
        String originalInput = args[0];
        String outputFolder = args[1];
        String adjacencyListOutput = outputFolder + "/adj";


        success = runAdjacencyListJob(originalInput, adjacencyListOutput);
        if (!success) {
            System.out.println("Failed Generating Adjacency List");
            return 1;
        }

        return 0;
    }

    private boolean runAdjacencyListJob(String inputDir, String outputDir) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Follower Count");
        job.setJarByClass(PageRankDriver.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        job.setMapperClass(AdjacencyListJob.AdjacencyListMapper.class);
        job.setReducerClass(AdjacencyListJob.AdjacencyListReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir>");
        }

        try {
            ToolRunner.run(new PageRankDriver(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}

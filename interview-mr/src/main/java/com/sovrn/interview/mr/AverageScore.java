package com.sovrn.interview.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * A quick application to exercise some map reduce fundamentals.
 * Requirements:
 * 1.) Average the score for each domain
 * 2.) The score is the 2nd column of the data file
 * 3.) The normalized URL is the 4th column of the data file.
 * 
 * The tab seperated data file can be found under src/main/data/data.tsv
 * 
 * Example data:
 *
 * technology      0.840115        http://411mania.com/games/dragon-fantasy-book-one-psn-review/   http://411mania.com/games/dragon-fantasy-book-one-psn-review/   Sovrn
 * games   0.82643 http://411mania.com/games/dragon-fantasy-book-one-psn-review/   http://411mania.com/games/dragon-fantasy-book-one-psn-review/   Sovrn
 * video-gaming    0.756304        http://411mania.com/games/dragon-fantasy-book-one-psn-review/   http://411mania.com/games/dragon-fantasy-book-one-psn-review/   Sovrn
 * consumer-electronics    0.712243        http://411mania.com/games/dragon-fantasy-book-one-psn-review/   http://411mania.com/games/dragon-fantasy-book-one-psn-review/   Sovrn
 * technology-companies    0.651904        http://411mania.com/games/dragon-fantasy-book-one-psn-review/   http://411mania.com/games/dragon-fantasy-book-one-psn-review/   Sovrn
 * navigation      0.615594        http://411mania.com/games/dragon-fantasy-book-one-psn-review/   http://411mania.com/games/dragon-fantasy-book-one-psn-review/   Sovrn
 */
public class AverageScore {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Interview Averaging");
        job.setJarByClass(AverageScore.class);
        job.setMapperClass(AverageScoreMapper.class);
        job.setReducerClass(AverageScoreReducer.class);
        // TODO: Finish the key and output setup

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class AverageScoreMapper extends Mapper<Object, Text, Text, FloatWritable> {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO : Implement the mapping phase
            
        }

    }

    public static class AverageScoreReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values,
                Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException,
                InterruptedException {
            // TODO: Implemnet the reducer phase
        }

    }
}

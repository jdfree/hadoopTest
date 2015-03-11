package com.sovrn.interview.mr;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
 * The tab separated data file can be found under src/main/data/data.tsv
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
        if(args.length < 2)
            System.exit(2);
        System.exit(mapReduce(args[0], args[1]));
    }

    public static int mapReduce(String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Interview Averaging");
        job.setJarByClass(AverageScore.class);
        job.setMapperClass(AverageScoreMapper.class);
        job.setReducerClass(AverageScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AverageScoreMapper extends Mapper<Object, Text, Text, FloatWritable> {

        private final FloatWritable number = new FloatWritable();
        private final Text domainText = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            
            if(values.length < 4)
            	return;

            try {
                String domain = new URI(values[3]).getHost();
                number.set(Float.parseFloat(values[1]));

                if(domain != null) {
                    domainText.set(domain);
                    context.write(domainText, number);
                }

            } catch (URISyntaxException e) {
                // Invalid data - not a clean URL. No error reporting requirement, so ignore for now.
            } catch (NumberFormatException e) {
                // Invalid data - value wasn't a number (as in the column header line). 
            	// No error reporting requirement, so ignore for now.
            }
        }

    }

    public static class AverageScoreReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private final FloatWritable average = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values,
                Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException,
                InterruptedException {
            float sum = 0f;
            int count = 0;

            for (FloatWritable value : values) {
                sum += value.get();
                count++;
            }

            average.set(count > 0 ? sum / count : 0f);
            context.write(key, average);
        }

    }
}

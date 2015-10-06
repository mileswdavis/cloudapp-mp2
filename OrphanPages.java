import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
        //TODO
        Configuration conf = this.getConf();

        Job jobA = Job.getInstance(conf, "Orphan Pages");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(OrphanPages.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //TODO
            int new_key_int = 0;
            int token_key = 0;
            String line = value.toString();
            StringTokenizer tokenizer_split = new StringTokenizer(line, ":");
            new_key_int = Integer.parseInt(tokenizer_split.nextToken());
            IntWritable new_key = new IntWritable(new_key_int);
            context.write(new_key, new IntWritable(0));
            System.out.println("Map: Key-" + new_key_int + "  Value-0");

            String new_string_value = tokenizer_split.nextToken();
            StringTokenizer tokenizer = new StringTokenizer(new_string_value, " ");
            while (tokenizer.hasMoreTokens()) {
                String nextToken = tokenizer.nextToken();
                token_key = Integer.parseInt(nextToken);
                context.write(new IntWritable(token_key), new IntWritable(1));
                System.out.println("Map: Key-" + token_key + "  Value-1");
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum == 0) {
                context.write(key, NullWritable.get());
                System.out.println("Reduce: Sum-" + sum);
                System.out.println("Reduce: Key-" + key + "Value-Null");
            }
        }
    }
}

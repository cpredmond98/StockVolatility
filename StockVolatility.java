import java.io.IOException;
import java.util.StringTokenizer;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockVolatility {

    public static class StockMapper
            extends Mapper<Object, Text, Text, DoubleWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {

                String[] lineIn = itr.nextToken().split(",");
                // average all given price values to get one stock price
                String name = lineIn[7];
                //double ratio = (Double.parseDouble(lineIn[9]));
                double ratio = (Double.parseDouble(lineIn[4]));
                double dailyLogReturn = Math.log(ratio);
                double dailyVariance = dailyLogReturn * dailyLogReturn;

                context.write(new Text(name), new DoubleWritable(dailyVariance));
            }
        }
    }

    public static class StockReducer
            extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {


        public void reduce(Text key, Iterable<DoubleWritable> values, Context context
        ) throws IOException, InterruptedException {

            int size = 0;
            double sumDailyVariance = 0;
            for (DoubleWritable val : values) {
                size++;
                sumDailyVariance += val.get();
            }
            double meanDailyVariance = sumDailyVariance / size;
            double dailyVolatility = Math.sqrt(meanDailyVariance);
            double anualizedVolatility = dailyVolatility * Math.sqrt(252);

            context.write(key, new DoubleWritable(anualizedVolatility));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "vind variance");
        job.setJarByClass(StockVolatility.class);
        job.setMapperClass(StockMapper.class);
        job.setCombinerClass(StockReducer.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

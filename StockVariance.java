import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockVariance {

    public static class StockMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String[] lineIn = itr.nextToken().split(",");
			// average all given price values to get one stock price
			String name = lineIn[7].trim();
			double price = (Double.parseDouble(lineIn[2]) + Double.parseDouble(lineIn[3]) + 
			   Double.parseDouble(lineIn[4]) + Double.parseDouble(lineIn[5])) / 4;
			context.write(new Text(name), new DoubleWritable(price));
		}
        }
    }

    public static class VarianceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            double sumSquared = 0;
            double n = 0;

            for (DoubleWritable v : values) {
                double value = v.get();
                n ++;
                sum += value;
                sumSquared += value * value;
            }

            double variance = (sumSquared - sum * sum / n) / (n - 1);
            context.write(key, new DoubleWritable(variance));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "vind variance");

        job.setJarByClass(StockVariance.class);

        job.setMapperClass(StockMapper.class);
	// do not set the combiner as variance is not commutative
        job.setReducerClass(VarianceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

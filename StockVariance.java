import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockVariance {

    public static class StockMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {

                String[] lineIn = itr.nextToken().split(",");
                // average all given price values to get one stock price
                String name = lineIn[6];
                double price = (Double.parseDouble(lineIn[1]) + Double.parseDouble(lineIn[2]) + 
                    Double.parseDouble(lineIn[3]) + Double.parseDouble(lineIn[4])) / 4;
                double mean = price;
                double s2 = 0;
                double popSize = 1;
                String lineOut = String.format("%f,%f,%f", popSize, mean, s2);

                context.write(new Text(name), new Text(lineOut));
            }
        }
    }

    public static class StockReducer
            extends Reducer<Text,Text,Text,Text> {


        public void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            double pop;
            double mean;
            double s2;

            boolean first = true;
            for (Text val : values) {
                String[] line = val.toString().split(",");
                double popNext = Double.parseDouble(line[0]);
                double meanNext = Double.parseDouble(line[1]);
                double s2Next = Double.parseDouble(line[2]);
                
                if (first) {
                    pop = popNext;
                    mean = meanNext;
                    s2 = s2Next;

                    first = false;
                } else {
                    double meanDiff = meanNext - mean;
                    mean = (pop * mean + popNext * meanNext) / (pop + popNext);
                    s2 = s2 + s2Next + meanDiff * meanDiff * pop * popNext / (pop + popNext);
                    pop = pop + popNext;
                }
            }
            String lineOut = String.format("%f,%f,%f", pop, mean, s2);
            context.write(key, new Text(lineOut));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "vind variance");
        job.setJarByClass(StockVariance.class);
        job.setMapperClass(StockMapper.class);
        job.setCombinerClass(StockReducer.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

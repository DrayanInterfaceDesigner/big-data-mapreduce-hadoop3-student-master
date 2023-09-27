package marseloddois;

import marseloddois.CustomWritables.OccurrenceUsdValueWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class QuestionFour {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        FolderCounter fc = new FolderCounter("output/", "penyss");
        System.out.println(fc);

        Path input = new Path("in/sexo.csv");

        Path output = new Path("output/penyss" + (fc.count()+1));

        Job j = new Job(c, "QuestionFour");

        j.setJarByClass(QuestionFour.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(IntWritable.class);
        j.setMapOutputValueClass(OccurrenceUsdValueWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, IntWritable, OccurrenceUsdValueWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String[] cols = value.toString().split(";");

            if(!cols[1].equalsIgnoreCase("year")){

                int year = Integer.parseInt(cols[1]);
                float trade_usd = Float.parseFloat(cols[5]);
                con.write(new IntWritable(year), new OccurrenceUsdValueWritable(trade_usd, 1));
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, OccurrenceUsdValueWritable, Text, FloatWritable> {

        public void reduce(IntWritable key, Iterable<OccurrenceUsdValueWritable> values, Context con)
                throws IOException, InterruptedException {

            float sum = 0;
            int occrs = 0;
            for(OccurrenceUsdValueWritable v : values) {
                sum += v.getUsdValue();
                occrs += 1;
            }

            float avg = occrs > 0 ? (sum / occrs) : sum;
            con.write(new Text("AveragePerYear: " + key.toString()), new FloatWritable(avg));
        }
    }
}

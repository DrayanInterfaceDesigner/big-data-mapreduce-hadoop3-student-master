package marseloddois;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class QuestionOne {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        FolderCounter fc = new FolderCounter("output/", "penys");
        System.out.println(fc);

        Path input = new Path("in/sexo.csv");

        Path output = new Path("output/penys" + (fc.count()+1));

        Job j = new Job(c, "QuestionOne");

        j.setJarByClass(QuestionOne.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String[] cols = value.toString().split(";");

            if(!cols[0].equalsIgnoreCase("country_or_area")){
                String country_or_area = cols[0];

                if (country_or_area.equalsIgnoreCase("brazil")) {
                    con.write(new Text("Brasil"), new IntWritable(1));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int count = 0;
            for(IntWritable v : values) {
                count += v.get();
            }

            con.write(new Text(key.toString()), new IntWritable(count));
        }
    }
}

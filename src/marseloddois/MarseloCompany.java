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

public class MarseloCompany {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        FolderCounter fc = new FolderCounter("output/", "penys");
        System.out.println(fc);

        // input
        Path input = new Path("in/sexo.csv");

        // output
        Path output = new Path("output/penys" + (fc.count()+1));


        Job j = new Job(c, "teste");


        j.setJarByClass(MarseloCompany.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String[] cols = value.toString().split(";");

            if(!cols[0].equalsIgnoreCase("country_or_area")) {

                String country_or_area = cols[0];

                //Da erro converter ano pra int pq tem 1 ano que se chama "year" na primeira row
                Integer year = Integer.parseInt(cols[1]);
                Integer comm_code = Integer.parseInt(cols[2]);

                //exemplo: Animals, live, except farm animals
                String commodity = cols[3];

                String flow = cols[4];
                Float trade_usd = Float.parseFloat(cols[5]);
                Float weight_kg = Float.parseFloat(cols[6]);
                String quantity_name = cols[7];
                Float quantity = Float.parseFloat(cols[8]);
                String category = cols[9];

                //exemplo: ["Animals", "live", "except farm animals"]
                String[] commodity_parts = value.toString().split(",");

                con.write(new Text("Big Data"), new Text(" eh legal"));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context con)
                throws IOException, InterruptedException {

            con.write(new Text(key.toString() + values.iterator().next().toString()),
                    new IntWritable(10));
        }
    }
}

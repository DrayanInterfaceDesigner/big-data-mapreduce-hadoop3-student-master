package marseloddois;

import marseloddois.CustomWritables.OccurrenceUsdValueWritable;
import marseloddois.CustomWritables.YearCategoryUnitTypeWritable;
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

public class QuestionFive {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        FolderCounter fc = new FolderCounter("output/", "penys3_");
        System.out.println(fc);

        // input
        Path input = new Path("in/sexo.csv");

        // output
        Path output = new Path("output/penys3_" + (fc.count()+1));


        Job j = new Job(c, "QuestionThree");


        j.setJarByClass(QuestionFive.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        j.setMapOutputKeyClass(YearCategoryUnitTypeWritable.class);
        j.setMapOutputValueClass(OccurrenceUsdValueWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, YearCategoryUnitTypeWritable, OccurrenceUsdValueWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String[] cols = value.toString().split(";");

            // A CHAVE É COMPOSTA!!!
            // <Ano, Categoria, Unidade>, <usd, occr>
            if(!cols[0].equalsIgnoreCase("country_or_area")) {

                String country_or_area = cols[0];
                int year = Integer.parseInt(cols[1]);
                String flow = cols[4];
                String quantity_name = cols[7];
                String category = cols[9];
                float trade_usd = Float.parseFloat(cols[5]);

                if(
                      flow.equalsIgnoreCase("export") &&
                      country_or_area.equalsIgnoreCase("brazil")
                ) {
                    con.write(
                            new YearCategoryUnitTypeWritable(
                                    year,
                                    category,
                                    quantity_name
                            ),
                            new OccurrenceUsdValueWritable(
                                    trade_usd,
                                    1
                            ));
                }
            }
        }
    }

    public static class Reduce extends Reducer<YearCategoryUnitTypeWritable, OccurrenceUsdValueWritable, Text, FloatWritable> {
        public void reduce(YearCategoryUnitTypeWritable key, Iterable<OccurrenceUsdValueWritable> values, Context con)
                throws IOException, InterruptedException {

            float sum = 0;
            int occrs = 0;
            for(OccurrenceUsdValueWritable v : values) {
                sum += v.getUsdValue();
                occrs += 1;
            }

            float avg = occrs > 0 ? (sum / occrs) : sum;
            con.write(new Text(
                    key.getYear() + ";"
                    + key.getCategory() + ";"
                    + key.getUnitType()
                    ), new FloatWritable(avg));
        }
    }
}

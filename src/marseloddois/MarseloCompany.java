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
        // arquivo de entrada
        Path input = new Path("in/sexo.csv");

        // arquivo de saida
        Path output = new Path("output/penys" + (fc.count()+1));

        // criacao do job e seu nome
        Job j = new Job(c, "teste");

        // registro das classes
        j.setJarByClass(MarseloCompany.class);
        j.setMapperClass(Map.class);
        j.setReducerClass(Reduce.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //
            String[] cols = value.toString().split(";");

            float country_or_area = Float.parseFloat(cols[0]);
            float year = Float.parseFloat(cols[1]);
            float comm_code = Float.parseFloat(cols[2]);
            float commodity = Float.parseFloat(cols[3]);
            float flow = Float.parseFloat(cols[4]);
            float trade_usd = Float.parseFloat(cols[5]);
            float weight_kg = Float.parseFloat(cols[6]);
            float quantity_name = Float.parseFloat(cols[7]);
            float quantity = Float.parseFloat(cols[8]);
            float category = Float.parseFloat(cols[9]);

            con.write(new Text("Big Data"), new Text(" eh legal"));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<Text> values, Context con)
                throws IOException, InterruptedException {

            con.write(new Text(key.toString() + values.iterator().next().toString()),
                    new IntWritable(10));
        }
    }
}

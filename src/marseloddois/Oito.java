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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.time.Year;
import java.util.Objects;

import java.io.IOException;

public class Treis {

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
        j.setMapOutputKeyClass(EightWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class Map extends Mapper<LongWritable, Text, EightWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //
            String[] cols = value.toString().split(";");

            if(!cols[0].equalsIgnoreCase("country_or_area")) {

                String year = cols[1];
                String commodity = cols[3];
                Float trade_usd = Float.parseFloat(cols[5]);
                String quantity_name = cols[7];

                // Descrição e valor da commodity com o maior preço por tipo de unidade (unit) e ano.

                con.write(new EightWritable(year, quantity_name), new IntWritable(1));

            }
        }
    }

    public static class Reduce extends Reducer<YearFlowWritable, IntWritable, Text, IntWritable> {

        public void reduce(YearFlowWritable key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }

            IntWritable valorSaida = new IntWritable(soma);

            con.write(new Text(key.toString()), valorSaida);
        }
    }

    public static class EightWritable implements WritableComparable<EightWritable> {
        private String year;
        private String quantity_name;

        public EightWritable() {

        }

        public EightWritable(String year, String quantity_name) {
            this.year = year;
            this.quantity_name = quantity_name;
        }

        public String getYear() {
            return year;
        }

        public void setYear(String year) {
            this.year = year;
        }

        public String getQuantity_name() {
            return quantity_name;
        }

        public void setQuantity_name(String commodity) {
            this.quantity_name = quantity_name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EightWritable that = (EightWritable) o;
            return Objects.equals(that.year, year) && Objects.equals(quantity_name, that.quantity_name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(year, quantity_name);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(year);
            dataOutput.writeUTF(quantity_name);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            year = dataInput.readUTF();
            quantity_name = dataInput.readUTF();
        }

        @Override
        public String toString() {
            return year +", " + quantity_name ;
        }

        @Override
        public int compareTo(EightWritable o) {
            if(this.hashCode() < o.hashCode()) {
                return -1;
            } else if (this.hashCode() > o.hashCode()){
                return 1;
            }
            return 0;
        }
    }


}

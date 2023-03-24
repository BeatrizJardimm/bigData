package basic;

import java.io.IOException;

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


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        //registro de classes
        j.setJarByClass(WordCount.class); //classe que tem o metodo main
        j.setMapperClass(MapForWordCount.class); //classe do MAP
        j.setReducerClass(ReduceForWordCount.class); //classe do REDUCE

        // tipos de saida
        j.setMapOutputKeyClass(Text.class); //tipo da chave de saida do map
        j.setMapOutputValueClass(IntWritable.class); //tipo do valor de saida do map
        j.setOutputKeyClass(Text.class); // tipo de chave de saida do reduce
        j.setOutputValueClass(IntWritable.class); // tipo de valor de saida do reduce

        // definir arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); // adicionando o caminho do input no job
        FileOutputFormat.setOutputPath(j, output); // adicionando o caminho do output no job

        // rodar :)
        j.waitForCompletion(false);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String palavras[] = linha.split(" ");

            //loop para gerar chaves/valor
            for (String p : palavras){
                Text chave = new Text(p);
                IntWritable valor = new IntWritable(1);
                con.write(chave, valor);
            }
        }
    }


    /*
    * 1 tipo: chave de entrada do reduce (tipo da chave de saida do map)
    * 2 tipo: valor de entrada do reduce (tipo do valor de saida do map)
    * 3 tipo: chave de saida do reduce
    * 4 tipo: valor de saida do reduce
    * */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;

            for (IntWritable v: values){
                soma += v.get();
            }

            con.write(key, new IntWritable(soma));
        }
    }

}

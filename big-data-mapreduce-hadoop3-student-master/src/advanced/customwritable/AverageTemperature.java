package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // regsitro de classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        // tipos de saida
        j.setMapOutputKeyClass(Text.class); //tipo da chave de saida do map
        j.setMapOutputValueClass(FireAvgTempWritable.class); //tipo do valor de saida do map
        j.setOutputKeyClass(Text.class); // tipo de chave de saida do reduce
        j.setOutputValueClass(FloatWritable.class); // tipo de valor de saida do reduce

        // arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); // adicionando o caminho do input no job
        FileOutputFormat.setOutputPath(j, output); // adicionando o caminho do output no job

        // rodar :)
        j.waitForCompletion(false);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String colunas[] = linha.split(",");

            float temperatura = Float.parseFloat(colunas[8]);

            Text chave = new Text("global");

            // criando (chave = "global", valor=(soma=temperatura,  n=1))
            con.write(chave, new FireAvgTempWritable(temperatura, 1));
        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
        }
    }


    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            // somando as somas de temperatura e a soma das quantidades
            float somaTemperatura = 0.0f;
            int somaQuantidades = 0;

            for(FireAvgTempWritable o : values){
                somaTemperatura += o.getSomaTemperatura();
                somaQuantidades += o.getN();
            }

            //calculando a media
            float media = somaTemperatura / somaQuantidades;

            // salvando o resultado (chave = "global", valor = media)
            con.write(key, new FloatWritable(media));

        }
    }

}

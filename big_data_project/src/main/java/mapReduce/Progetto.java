package mapReduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * - per lanciare:
 *      + aprire cmd in root del progetto e lanciare:
 *          1) $ gradle wrapper
 *          2) $ gradle build
 *      + copiare su desktop VM file 'big_data_project.jar' contenuto in '/big_data_project/build/libs/'
 *      + assicurarsi che tutto funzioni su VM e lanciare comando:
 *          $ hadoop jar big_data_project.jar mapReduce.Progetto /bigdata/dataset/projectTest mapreduce/esameMR/temp mapreduce/esameMR/output
 *
 * - Il passaggio delle informazioni tra i due job avviene attraverso i file contenuti in 'mapreduce/esameMR/temp':
 *      + il (reducer del) job1 scrive i propri risultati su tale file
 *      + il (mapper del) job2 legge i risultati della fase1 da tale file
 *
 * Divisione dei compiti:
 * - Mapper1    => estrapola coppie (email,psw) dai file di testo in imput
 * - Reducer1   => calcola media delle edit distance normalizzate delle password associate alla stessa mail
 * - Mapper2    => estrapola coppie (#nPassword,media edit distance normalizzate) dal file di testo in output al primo job
 * - Reducer2   => calcola per le diverse lunghezze delle password il 'grado di variabilità medio'
 *
 * Appunto sui reducer
 * - Reducer1: lavora su password raggruppate in base alla 'email'
 * - Reducer2: lavora su medie raggruppate in base a 'il numero di password associate ad una mail'
 */
public class Progetto {
    public static void main(String[] args) throws IOException {
        JobConf conf1 = new JobConf(Progetto.class);
        conf1.setJobName("Calcolo media edit distance normalizzate per ogni diversa email");

        Path inputPath1 = new Path(args[0]),
                outputPath1 = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if(fs.exists(outputPath1)) {
            fs.delete(outputPath1, true);
        }

        FileInputFormat.addInputPath(conf1, inputPath1);
        FileOutputFormat.setOutputPath(conf1, outputPath1);

        conf1.setMapperClass(Mapper1.class);
        conf1.setReducerClass(Reducer1.class);

        if(args.length>3 && Integer.parseInt(args[3])>=0){
            conf1.setNumReduceTasks(Integer.parseInt(args[3]));
        }
        else{
            conf1.setNumReduceTasks(1);
        }

        conf1.setMapOutputKeyClass(Text.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setOutputKeyClass(IntWritable.class);
        conf1.setOutputValueClass(DoubleWritable.class);

        JobClient.runJob(conf1);

        //------------- Calcolo grado di variabilità medio---------------

        JobConf conf2 = new JobConf(Progetto.class);
        conf2.setJobName("Calcolo grado di variabilità medio");

        Path inputPath2 = new Path(args[1]),
                outputPath2 = new Path(args[2]);


        if(fs.exists(outputPath2)) {
            fs.delete(outputPath2, true);
        }

        FileInputFormat.addInputPath(conf2, inputPath2);
        FileOutputFormat.setOutputPath(conf2, outputPath2);

        conf2.setMapperClass(Mapper2.class);
        conf2.setReducerClass(Reducer2.class);

        if(args.length>3 && Integer.parseInt(args[3])>=0){
            conf2.setNumReduceTasks(Integer.parseInt(args[3]));
        }
        else{
            conf2.setNumReduceTasks(1);
        }

        conf2.setMapOutputKeyClass(IntWritable.class);
        conf2.setMapOutputValueClass(DoubleWritable.class);
        conf2.setOutputKeyClass(IntWritable.class);
        conf2.setOutputValueClass(DoubleWritable.class);

        JobClient.runJob(conf2);

    }
}

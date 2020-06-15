package exercise1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * conta quante volte compaiono le parole
 *
 * $ hadoop jar big_data_project.jar exercise1.WordCount /bigdata/dataset/sample-input mapreduce/wordcount/output
 */

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{ //nota questa estensione: devi sempre estendere
        //i 4 tipi sono <Tipo_chiave_input, Tipo_valore_input, Tipo_chiave_output, Tipo_valore_output>
        //nota che sono tutte classi che estendono writable: non poso usare una classe String ad esempio: devo usare Text

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        //prende: Tipo_chiave_input, Tipo_valore_input, Context che rappresenta framework usato per scrivere in output
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one); //nota che non posso mettere un integer: devo mettere un IntWritable
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> { //anche qui estendo => i tipi sono come quelli detti sopra
        private IntWritable result = new IntWritable();

        //Iterable<IntWritable> perchè in input prendo una chiave e un insieme dei valori corrispondenti a quella chiave
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
            //cancello la directory in output se c'è: il risultato sarà sempre sovrascritto
            fs.delete(outputPath, true);
        }

        //faccio impostazione del job attraverso oggetto job
        job.setJarByClass(WordCount.class); //=> dico quale classe contiene funzione di map
        job.setMapperClass(TokenizerMapper.class); //=> dico quale classe contiene funzione di reduce

        //eventualmente prende un 3° parametro che imposta il numero di task di reduce
        //il numero di task di map corrisponde al numero di blocchi dei file in input
        if(args.length>2){
            if(Integer.parseInt(args[2])>=0){
                job.setNumReduceTasks(Integer.parseInt(args[2])); //sto comando imposta numero task reduce
            }
        }

        /**
         * combiner fa lo stesso lavoro di un reducer (estende classe Reducer)
         * => il tipo di dati preso in input deve essere consistente con quello in output alla map
         * ma lavora lato map
         *
         * in sto caso riutilizzo perfettamente il combiner ma non è detto vada bene
         */
        job.setCombinerClass(IntSumReducer.class); //togli sta linea se vuoi far senza combiner
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class); //indico classe relativa alla chiave in output alld map
        job.setOutputValueClass(IntWritable.class); //indico classe relativa al valore in output alla map

        FileInputFormat.addInputPath(job, inputPath); //imposta inputpath
        FileOutputFormat.setOutputPath(job, outputPath); //imposta outputpath

        //job.waitForCompletion(true) lancia effettivamente il job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
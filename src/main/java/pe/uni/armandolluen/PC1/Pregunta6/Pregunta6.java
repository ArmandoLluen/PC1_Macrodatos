package pe.uni.armandolluen.PC1.Pregunta6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pregunta6 {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: KMeansJob <input path> <output path for iteration 1> <output path for iteration 2>");
            System.exit(-1);
        }

        // Configurar la primera iteración del trabajo de MapReduce
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "KMeans Iteration 1");
        job1.setJarByClass(Pregunta6.class);
        job1.setMapperClass(KMeansMapper1.class);
        job1.setReducerClass(KMeansReducer1.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // Ejecutar la primera iteración
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Configurar la segunda iteración del trabajo de MapReduce
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "KMeans Iteration 2");
        job2.setJarByClass(Pregunta6.class);
        job2.setMapperClass(KMeansMapper2.class);
        job2.setReducerClass(KMeansReducer2.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // Ejecutar la segunda iteración
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

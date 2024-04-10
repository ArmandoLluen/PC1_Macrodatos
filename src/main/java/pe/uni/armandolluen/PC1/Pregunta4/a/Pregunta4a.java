package pe.uni.armandolluen.PC1.Pregunta4.a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pe.uni.armandolluen.PC1.Pregunta4.AnimeData;

public class Pregunta4a {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: RegressionMain <inputPath> <outputPath>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Regression Model");
        job.setJarByClass(Pregunta4a.class);
        job.setMapperClass(RegressionMapper.class);
        job.setReducerClass(RegressionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AnimeData.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

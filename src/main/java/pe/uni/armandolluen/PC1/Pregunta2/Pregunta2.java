package pe.uni.armandolluen.PC1.Pregunta2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pregunta2 {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("entra: <input path> <output path>");
            System.exit(-1);
        }

        // Configuración de Hadoop
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pregunta 2");

        // Configuración de la clase principal y las clases de mapper y reducer
        job.setJarByClass(Pregunta2.class);
        job.setMapperClass(BirthdayMapper.class);
        job.setReducerClass(BirthdayReducer.class);

        // Salida de los tipos clave-valor del mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Salida de los tipos clave-valor del reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Archivos de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Ejecución del trabajo MapReduce
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

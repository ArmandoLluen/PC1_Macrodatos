package pe.uni.armandolluen.PC1.Pregunta3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pregunta3 {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: SynopsisMR <input_path> <output_path>");
            System.exit(-1);
        }

        // Crear una configuración para el trabajo
        Configuration conf = new Configuration();
        // Crear el objeto de trabajo
        Job job = Job.getInstance(conf, "Synopsis MapReduce");

        // Especificar clases de main y reducer
        job.setJarByClass(Pregunta3.class);
        job.setMapperClass(SynopsisMapper.class);
        job.setReducerClass(SynopsisReducer.class);

        // Configurar tipos de salida
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Especificar rutas de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Esperar a que el trabajo termine e imprimir si tuvo éxito
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

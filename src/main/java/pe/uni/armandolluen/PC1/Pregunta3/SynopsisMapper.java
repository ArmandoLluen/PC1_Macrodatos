package pe.uni.armandolluen.PC1.Pregunta3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SynopsisMapper extends Mapper<Object, Text, Text, Text> {

    private final Text animeId = new Text();
    private final Text nameSynopsis = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String animeIdStr = fields[0];
        String name = fields[1];
        String synopsis = fields[5];

        if (synopsis.toLowerCase().contains("hokage")) {
            animeId.set(animeIdStr);
            nameSynopsis.set(name + "\t" + synopsis);
            context.write(animeId, nameSynopsis);
        }
    }
}

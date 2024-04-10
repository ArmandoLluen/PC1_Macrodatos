package pe.uni.armandolluen.PC1.Pregunta4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AnimeData implements Writable {

    private double score;
    private double popularity;
    private double rank;

    public AnimeData() {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        score = in.readDouble();
        popularity = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(score);
        out.writeDouble(popularity);
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public double getPopularity() {
        return popularity;
    }

    public void setPopularity(double popularity) {
        this.popularity = popularity;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public double getRank() {
        return this.rank;
    }
}
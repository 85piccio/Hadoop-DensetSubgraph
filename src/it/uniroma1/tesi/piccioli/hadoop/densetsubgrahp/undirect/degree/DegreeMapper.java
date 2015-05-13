package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.degree;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DegreeMapper  extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(key, value);
	}
}


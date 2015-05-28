package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.degree;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DegreeMapperInverso  extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		context.write(value, key);
	}
}


package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.delete;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeleteMapperT extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		Long totEdges = context.getConfiguration().getLong("EDGES", 1);
		Long totVerticiT = context.getConfiguration().getLong("VERTICIT", 1);
		Double epsilon = context.getConfiguration().getDouble("EPSILON", 1);

		double soglia = (1 + epsilon) * (totEdges.doubleValue() / totVerticiT.doubleValue());
		System.out.println("graph T: " +totVerticiT+" "+totEdges+ "soglia: " + soglia);
		if (new Double(Double.parseDouble(value.toString())) < soglia)
			context.write(key, new Text("$"));// $ val sentinella
	}
}

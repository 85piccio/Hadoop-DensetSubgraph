package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.delete;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeleteMapper extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		long totEdges = context.getConfiguration().getLong("EDGES", 1);
		long totVertici = context.getConfiguration().getLong("VERTICI", 1);


		double soglia = 2 * (1 + 0.01) * (totEdges / totVertici);
		System.out.println("graph: " +totVertici+" "+totEdges+ "soglia: " + soglia);
		if (new Long(Long.parseLong(value.toString())) < soglia)
			context.write(key, new Text("$"));// $ val sentinella
	}
}

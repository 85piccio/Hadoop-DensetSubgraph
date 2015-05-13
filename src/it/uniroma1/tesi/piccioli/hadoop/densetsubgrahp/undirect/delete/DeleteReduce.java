package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.delete;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeleteReduce extends Reducer<Text, Text, Text, Text> {
	protected void reduce(Text key, Iterable<Text> edges, Context context) throws IOException, InterruptedException {
		
		ArrayList<String> listaEdges = new ArrayList<String>();
		for (Text edge : edges) {
			if (edge.toString().compareTo("$") == 0)
				return;
			listaEdges.add(edge.toString());
		}

		for (String edge : listaEdges) {
			context.write(key,new Text(edge));
		}
	}
}

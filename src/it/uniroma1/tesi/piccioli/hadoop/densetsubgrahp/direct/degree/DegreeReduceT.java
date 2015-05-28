package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.degree;




import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.VERTICIT;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DegreeReduceT extends Reducer<Text, Text, Text, Text> {
//	IntWritable degree = new IntWritable();
	String keyVal;
	int keepAbove = -1;
	@Override
	protected void reduce(Text key, Iterable<Text> edges, Context context) throws IOException,
			InterruptedException {
		Integer size = 0;
		keyVal = key.toString();
		for (Text edge : edges) {
			if (!edge.toString().equals(keyVal)) { // remove self-loops
				size++;
			}
		}
		//incremento contatori vertici e edges
		context.getCounter(VERTICIT.VERTICIT).increment(1);	
		
		context.write(key, new Text(size.toString()));
	}
}

package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect;

import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.degree.DegreeMapper;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.degree.DegreeMapper2;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.degree.DegreeReduce;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.delete.DeleteMapper;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.delete.DeleteReduce;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.undirect.delete.DeleteReduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool {

	long sEdge = Long.MAX_VALUE;

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		if (args.length > 2) {
			// passaggio paramentri da linea di comando
		}

		double bestDensita = Long.MIN_VALUE;

		conf.setDouble("EPSILON", 0.001);

		// path job1
		Path pathGrafoIniziale = new Path(args[0]);
		Path pathResultTmpDegree = new Path("/tmp_1");
		// Path pathResultTmpDelete = new Path("/tmp_2");
		Path pathResultTmpDelete2 = new Path("/tmp_3");
		Path pathResultTmpDelete = pathGrafoIniziale;

		while (true) {

			FileSystem fs = FileSystem.get(conf);

			/*
			 * Primo Job Input Calcolo degre nodi, totale nodi e totale archi
			 */

			Job degreeJob = Job.getInstance(conf);

			degreeJob.setJobName("calcolo degree");

			degreeJob.setMapperClass(DegreeMapper.class);
			degreeJob.setReducerClass(DegreeReduce.class);

			degreeJob.setJarByClass(App.class);

			degreeJob.setInputFormatClass(KeyValueTextInputFormat.class);
			degreeJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(degreeJob, pathResultTmpDelete);
			FileOutputFormat.setOutputPath(degreeJob, pathResultTmpDegree);

			degreeJob.setMapOutputKeyClass(Text.class);
			degreeJob.setMapOutputValueClass(Text.class);

			fs.delete(pathResultTmpDegree, true);

			// boolean res = degreeJob.waitForCompletion(true);
			degreeJob.waitForCompletion(true);

			// edges diviso 2 --> arco undirect rappresentato da 2 archi direct
			Long totEdge = degreeJob.getCounters().findCounter(EDGE.EDGES).getValue() / 2;
			Long totVertici = degreeJob.getCounters().findCounter(VERTICI.VERTICI).getValue();
			conf.setLong("EDGES", totEdge);
			conf.setLong("VERTICI", totVertici);

			System.out.println(totEdge + " " + totVertici);

			// se finiscono edge o non ci sono cambiamenti
			if (totVertici == 0 || totEdge == 0 || totEdge == sEdge) {
				// Stampo best
				System.out.println("best " + bestDensita);
				return 0;
			}

			double currDensita = totEdge.doubleValue() / totVertici.doubleValue();
			if (bestDensita < currDensita) {
				bestDensita = currDensita;
				// TODO: salvare lista edge best
			}
			sEdge = totEdge;// aggiorno var controllo num edge

			/*
			 * secondo Job elimino nodi "prima colonna" con degree sotto soglia
			 */

			Job deleteJob = Job.getInstance(conf);

			MultipleInputs.addInputPath(deleteJob, pathResultTmpDelete, KeyValueTextInputFormat.class,
					DegreeMapper.class);
			MultipleInputs.addInputPath(deleteJob, pathResultTmpDegree, KeyValueTextInputFormat.class,
					DeleteMapper.class);

			deleteJob.setReducerClass(DeleteReduce.class);
			deleteJob.setJarByClass(App.class);
			deleteJob.setOutputFormatClass(TextOutputFormat.class);

			// FileInputFormat.addInputPath(deleteJob, pathResultTmpDegree);
			FileOutputFormat.setOutputPath(deleteJob, pathResultTmpDelete2);

			deleteJob.setMapOutputKeyClass(Text.class);
			deleteJob.setMapOutputValueClass(Text.class);

			fs.delete(pathResultTmpDelete2, true);

			// boolean res = degreeJob.waitForCompletion(true);
			deleteJob.waitForCompletion(true);

			// switch delete tmp
			Path tmp = pathResultTmpDelete2;
			pathResultTmpDelete2 = pathResultTmpDelete;
			pathResultTmpDelete = tmp;

			if (pathResultTmpDelete2.compareTo(pathGrafoIniziale) == 0) {
				// fixSync dopo prima iterazione
				pathResultTmpDelete = new Path("/tmp_3");
				pathResultTmpDelete2 = new Path("/tmp_2");
			}

			/*
			 * Terzo Job elimino nodi "prima colonna" con degree sotto soglia
			 */

			Job deleteJob2 = Job.getInstance(conf);
			// cambio prima volta

			MultipleInputs.addInputPath(deleteJob2, pathResultTmpDelete, KeyValueTextInputFormat.class,
					DegreeMapper2.class);
			MultipleInputs.addInputPath(deleteJob2, pathResultTmpDegree, KeyValueTextInputFormat.class,
					DeleteMapper.class);

			deleteJob2.setReducerClass(DeleteReduce2.class);
			deleteJob2.setJarByClass(App.class);
			deleteJob2.setOutputFormatClass(TextOutputFormat.class);

			// FileInputFormat.addInputPath(deleteJob, pathResultTmpDegree);
			FileOutputFormat.setOutputPath(deleteJob2, pathResultTmpDelete2);

			deleteJob2.setMapOutputKeyClass(Text.class);
			deleteJob2.setMapOutputValueClass(Text.class);

			fs.delete(pathResultTmpDelete2, true);
			deleteJob2.waitForCompletion(true);

			// switch delete tmp
			tmp = pathResultTmpDelete2;
			pathResultTmpDelete2 = pathResultTmpDelete;
			pathResultTmpDelete = tmp;

		}

	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.exit(printUsage());
		}

		Configuration conf = new Configuration();

		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		conf.set("mapreduce.output.textoutputformat.separator", " ");

		App dc = new App();
		dc.setConf(conf);
		int res = ToolRunner.run(dc, args);

		System.exit(res);

	}

	static int printUsage() {
		System.out.println("DegreeCalculator <input> <output> [<Source Index>]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
}

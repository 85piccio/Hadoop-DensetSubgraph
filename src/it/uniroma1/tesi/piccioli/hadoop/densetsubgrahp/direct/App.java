package it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct;

import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.degree.DegreeMapper;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.degree.DegreeMapperInverso;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.degree.DegreeReduceS;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.degree.DegreeReduceT;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.delete.DeleteMapperS;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.delete.DeleteMapperT;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.delete.DeleteReduce;
import it.uniroma1.tesi.piccioli.hadoop.densetsubgrahp.direct.delete.DeleteReduce2;

import org.apache.hadoop.fs.FileUtil;
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

/*
 * 
 * Densest Subgraph in Streaming and MapReduce
 * Bahman Bahmani, Ravi Kumar, Sergei, Vassilvitskii
 * 
 * */

public class App extends Configured implements Tool {

	long sEdge = Long.MAX_VALUE;
	double epsilon = 0.001;
	double cCostant = 1.0;

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		if (args.length > 2) {
			// passaggio paramentri da linea di comando
		}

		double bestDensita = Long.MIN_VALUE;

		conf.setDouble("EPSILON", epsilon);
		conf.setDouble("CCOSTANT", cCostant);

		// path job1
		Path pathGrafoIniziale = new Path(args[0]);
		Path pathNodiDenesetS = new Path(args[1] + "/partitionT");
		Path pathNodiDenesetT = new Path(args[1] + "/partitionS");
		Path pathResultTmpDegreeS = new Path("/tmp_degreeS");
		Path pathResultTmpDegreeT = new Path("/tmp_degreet");
		Path pathResultTmpDelete = new Path("/tmp_1");
		Path pathResultTmpDelete2 = new Path("/tmp_2");

		// solo per la prima iterazione
		pathResultTmpDelete = pathGrafoIniziale;

		FileSystem fs = FileSystem.get(conf);

		while (true) {

			/*
			 * Primo Job Input Calcolo degree nodi nella partizione S, totale
			 * nodi in S e totale archi
			 */

			Job degreeSJob = Job.getInstance(conf);

			degreeSJob.setJobName("calcolo degree S");

			degreeSJob.setMapperClass(DegreeMapper.class);
			degreeSJob.setReducerClass(DegreeReduceS.class);

			degreeSJob.setJarByClass(App.class);

			degreeSJob.setInputFormatClass(KeyValueTextInputFormat.class);
			degreeSJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(degreeSJob, pathResultTmpDelete);
			FileOutputFormat.setOutputPath(degreeSJob, pathResultTmpDegreeS);

			degreeSJob.setMapOutputKeyClass(Text.class);
			degreeSJob.setMapOutputValueClass(Text.class);

			fs.delete(pathResultTmpDegreeS, true);

			// boolean res = degreeJob.waitForCompletion(true);
			degreeSJob.waitForCompletion(true);

			Long totEdge = degreeSJob.getCounters().findCounter(EDGE.EDGES).getValue();
			Long totVerticiS = degreeSJob.getCounters().findCounter(VERTICIS.VERTICIS).getValue();

			/*
			 * Secondo Job Input Calcolo degree nodi nella partizione T, totale
			 * nodi in T
			 */

			Job degreeTJob = Job.getInstance(conf);

			degreeTJob.setJobName("calcolo degree T");

			degreeTJob.setMapperClass(DegreeMapperInverso.class);
			degreeTJob.setReducerClass(DegreeReduceT.class);

			degreeTJob.setJarByClass(App.class);

			degreeTJob.setInputFormatClass(KeyValueTextInputFormat.class);
			degreeTJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(degreeTJob, pathResultTmpDelete);
			FileOutputFormat.setOutputPath(degreeTJob, pathResultTmpDegreeT);

			degreeTJob.setMapOutputKeyClass(Text.class);
			degreeTJob.setMapOutputValueClass(Text.class);

			fs.delete(pathResultTmpDegreeT, true);

			// boolean res = degreeJob.waitForCompletion(true);
			degreeTJob.waitForCompletion(true);
			
			Long totVerticiT = degreeTJob.getCounters().findCounter(VERTICIT.VERTICIT).getValue();
			
			
			conf.setLong("EDGES", totEdge);
			conf.setLong("VERTICIS", totVerticiS);
			conf.setLong("VERTICIT", totVerticiT);

			System.out.println("info: " + totEdge + " " + totVerticiS + " " + totVerticiT);

			// se finiscono edge o non ci sono cambiamenti
			if ((totVerticiT == 0 && totVerticiS == 0) || totEdge == 0 || totEdge == sEdge) {
				// Stampo best
				System.out.println("best " + bestDensita);

				fs.delete(pathResultTmpDegreeS, true);
				fs.delete(pathResultTmpDegreeT, true);
				fs.delete(pathResultTmpDelete, true);
				fs.delete(pathResultTmpDelete2, true);
				return 0;
			}
			sEdge = totEdge;// aggiorno var controllo num edge per step
							// successivo

			double currDensita = totEdge.doubleValue()
					/ Math.sqrt(totVerticiS.doubleValue() * totVerticiT.doubleValue());

			System.out.println("Densità "+ currDensita);
			if (bestDensita < currDensita) {
				bestDensita = currDensita;
				// salvare lista edge best
				fs.delete(pathNodiDenesetS, true);
				FileUtil.copy(fs, pathResultTmpDegreeS, fs, pathNodiDenesetS, false, true, conf);
				fs.delete(pathNodiDenesetT, true);
				FileUtil.copy(fs, pathResultTmpDegreeT, fs, pathNodiDenesetT, false, true, conf);

			}
 
			boolean partitionS = (totVerticiS.doubleValue() / totVerticiT.doubleValue()) >= cCostant;
			

			/*
			 * secondo Job elimino nodi "prima colonna" con degree sotto soglia
			 */

			Job deleteJob = Job.getInstance(conf);

			if (partitionS) {
				MultipleInputs.addInputPath(deleteJob, pathResultTmpDelete, KeyValueTextInputFormat.class,
						DegreeMapper.class);
				MultipleInputs.addInputPath(deleteJob, pathResultTmpDegreeS, KeyValueTextInputFormat.class,
						DeleteMapperS.class);
				deleteJob.setReducerClass(DeleteReduce.class);
			} else {
				MultipleInputs.addInputPath(deleteJob, pathResultTmpDelete, KeyValueTextInputFormat.class,
						DegreeMapperInverso.class);
				MultipleInputs.addInputPath(deleteJob, pathResultTmpDegreeT, KeyValueTextInputFormat.class,
						DeleteMapperT.class);
				deleteJob.setReducerClass(DeleteReduce2.class);
			}

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
				pathResultTmpDelete = new Path("/tmp_2");
				pathResultTmpDelete2 = new Path("/tmp_1");
			}
		}

	}

	public static void main(String[] args) throws Exception {
		// if (args.length < 2) {
		// System.exit(printUsage());
		// }

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

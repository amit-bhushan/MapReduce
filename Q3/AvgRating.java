package bdc2;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import utilities.SortUtility;


public class AvgRating {
	// Mapper for user 

	public static class MapUserClass extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		private Map<String, String> ocMap = new HashMap<String, String>();
		public void setup(Context context) throws java.io.IOException, InterruptedException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			
			for (Path p : files) {
				if (p.getName().equals("occupation.txt")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String oid = tokens[0];
						String name = tokens[1];
						ocMap.put(oid, name);
						line = reader.readLine();
					}
				}
			}
			if (ocMap.isEmpty()) {
				throw new IOException("Unable to load user data.");
			}
		}
		public void map(LongWritable key, Text value, Context context) {
			try {
				String[] str = value.toString().split("::");
				String uid = str[0];
				String oid = str[3];
				String name = ocMap.get(oid);
				context.write(new IntWritable(Integer.parseInt(uid)), new Text(
						"u\t" + str[2] + "\t" + name));
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	// Mapper for Ratings
	public static class MapRatingsClass extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		private Map<String, String> mvMap = new HashMap<String, String>();
		
		public void setup(Context context) throws java.io.IOException, InterruptedException{
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			
			for (Path p : files) {
				if (p.getName().equals("movies.dat")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("::");
						String mid = tokens[0];
						String genre = tokens[2];
						mvMap.put(mid, genre);
						line = reader.readLine();
					}
				}
			}
			if (mvMap.isEmpty()) {
				throw new IOException("Unable to load movies data.");
			}
		}
		public void map(LongWritable key, Text value, Context context) {
			try {
				String[] str = value.toString().split("::");
				String uid = str[0];
				String mk= str[1];
				String gen = mvMap.get(mk);
				context.write(new IntWritable(Integer.parseInt(uid)), new Text(
						"r\t" + str[1] + "\t" + str[2] +"\t"+ gen));
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class ReduceJoinReducer extends
			Reducer<IntWritable, Text,Text, Text> {
		ListMultimap<String, Integer> map1 = ArrayListMultimap.create();
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String ud = "";
			String rd = "";
			int rt = 0;
			for (Text t : values) {
				String parts[] = t.toString().split("\t");
				String agegrp ="";
				if (parts[0].equals("u")) {
					int age = Integer.parseInt(parts[1]);
					if (age >= 18 && age <= 35) {
						agegrp = "18-35";
					} else if (age >= 36 && age <= 50) {
						agegrp = "36-50";
					} else if (age >= 50) {
						agegrp = "50+";
					}
					else{
						agegrp = "18-";
					}
						
					ud =  parts[2]+"\t"+  agegrp;
					//sum += Float.parseFloat(parts[1]);
				} else if (parts[0].equals("r")) {
					rd =  parts[3];
					rt = Integer.parseInt(parts[2]);
				}
			}
			map1.put(ud +"\t"+ rd, rt);
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			Map<String, Double> sMap = new HashMap<String,Double>();
	
			for (String s : map1.keySet()) {
				String parts[] = s.split("\t");
				if(!parts[1].equals("18-"))
				{
					List<Integer> ratings = map1.get(s);
					int sum = 0;
					for(Integer r : ratings)
					{
						sum += r;
					}
					double avg = ((double)sum/ratings.size());
					sMap.put(s,Double.parseDouble(String.format("%.2f",avg)));
				}
			}
			Map <String,Double> tmap = new TreeMap<String, Double>(sMap);
			Map <String,Double> tmap1 = new TreeMap<String, Double>();
			for (String K : tmap.keySet()) {
				String parts[] = K.split("\t");
				tmap1.put(parts[0]+"\t"+parts[1], tmap.get(K));
			}
			for (String L : tmap1.keySet()) {
			}
			Map<String, Double> hMap = new HashMap<String,Double>();
			for(String N : tmap1.keySet())
			{
				String partsN[] = N.split("\t");
				for (String M : tmap.keySet()) {
					String partsM[] = M.split("\t");
					if(partsM[0].equals(partsN[0]) && partsM[1].equals(partsN[1]))
					{
						hMap.put(M, tmap.get(M));
						
					}
				}
				Map<String, Double> sortedMap = SortUtility.sortByValues(hMap);
				for (String O : sortedMap.keySet()) {
					context.write(new Text(O) , new Text(sortedMap.get(O).toString()));
				}
				sortedMap.clear();
				hMap.clear();
			}
	}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(AvgRating.class);
		try{
		    DistributedCache.addCacheFile(new URI("/home/cloudera/workspace/Hadoop/input/movies.dat"), job.getConfiguration());
		    DistributedCache.addCacheFile(new URI("/home/cloudera/workspace/Hadoop/input/occupation.txt"), job.getConfiguration());
		    }catch(Exception e){
		    	System.out.println(e);
		    }
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, MapUserClass.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, MapRatingsClass.class);
		Path outputPath = new Path(args[2]);

		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}
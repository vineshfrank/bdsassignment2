package org.bds.assignment;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/*
 * 1.	Average unit_price by country for a given item type in a certain year
 */
public class SalesAvgUnitPrice {
	public static String country = "";
	public static String itemType = "";
	public static String year = "";
	public static int count = 0;
	public static double tAvgUnitPrice = 0.0;
	private static final Logger logger = LoggerFactory.getLogger(SalesAvgUnitPrice.class);

	/*
	 * Mapper Class to identify the records based on given inputs such as country , item type and year and pass to reducer 
	 */
	public static class AvgMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private Text word = new Text();
		final String DELIMITER = ",";
		Double unitPrice = 0.0;
		String tUniquekey = "";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {

				if (value.toString().contains("country")) {
					return;
				} else {
					String[] sales = value.toString().split(DELIMITER);

					String countryName = sales[2].toLowerCase();
					String tItemType = sales[3].toLowerCase();
					String orderDate = sales[6];
					String tUnitPrice = sales[10];
					Double unitPrice = Double.parseDouble(tUnitPrice);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					LocalDate localDate = LocalDate.parse(orderDate, formatter);
					int lYear = localDate.getYear();
					tUniquekey = countryName + "-" + tItemType + "-" + lYear;
					String givenValues = country + "-" + itemType + "-" + year;

					if (tUniquekey.equalsIgnoreCase(givenValues)) {
						word.set(tUniquekey);
						context.write(word, new DoubleWritable(unitPrice));

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
	
	/*
	 * Reducer Class to calculate the average based on the input from Mapper
	 */

	public static class AvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text term, Iterable<DoubleWritable> mappedUnitPrices, Context context)
				throws IOException, InterruptedException {
			for (DoubleWritable val : mappedUnitPrices) {
				count++;
				double value = val.get();
				tAvgUnitPrice = tAvgUnitPrice + value;
			}
			double finalAvgUnitPrice = tAvgUnitPrice / count;
			System.out.println("##########################");
			System.out.println("Average Unit Price In " + country + "  for Item Type :" + itemType + " and Year : "
					+ year + " is :: " + finalAvgUnitPrice);
			System.out.println("##########################");

			DoubleWritable output = new DoubleWritable(finalAvgUnitPrice);

			context.write(term, output);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 5) {
			System.out.println("pls pass requeried arguments");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Avg Unit Price");
		job.setJarByClass(SalesAvgUnitPrice.class);
		job.setMapperClass(AvgMapper.class);
		job.setReducerClass(AvgReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		country = args[0].toLowerCase();
		itemType = args[1].toLowerCase();
		year = args[2];
		if (year == null || year.length() < 4) {
			System.out.println("Please enter a valid year");
		}
		if (country.isEmpty()) {
			System.out.println("Please enter a Country");
		}
		if (itemType.isEmpty()) {
			System.out.println("Please enter a itemType");
		}
		FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[4] + Math.random()));

		boolean status = job.waitForCompletion(true);

		if (status) {
			System.exit(0); // Exit with Success code
			System.out.println("Completed the Map Reduce Process Successfully");
		} else {
			System.exit(1); // Exit with Failure code # 1
			System.out.println("exiting with failure");
		}

	}

}

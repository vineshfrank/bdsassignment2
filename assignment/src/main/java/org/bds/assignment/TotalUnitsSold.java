package org.bds.assignment;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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

/*
 * 	2.	Total units_sold by year for a given country and a given item type
 */
public class TotalUnitsSold {

	public static String country = "";
	public static String itemType = "";
	public static String year = "";
	public static int count = 0;
	public static double totalUnitSold = 0.0;

	/*
	 * Mapper Class to identify the records based on given inputs such as
	 * country , item type and year and pass to reducer
	 */

	public static class UnitsSoldMapper extends Mapper<Object, Text, Text, DoubleWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		final String DELIMITER = ",";
		Double unitPrice = 0.0;
		String tUniquekey = "";
		List index_Sales = new ArrayList<Integer>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				if (value.toString().contains("country")) {
					return;
				} else {
					String[] sales = value.toString().split(DELIMITER);

					index_Sales.add(sales[0]);
					String countryName = sales[2].toLowerCase();
					String tItemType = sales[3].toLowerCase();
					String orderDate = sales[6];
					String tUnitSold = sales[9];
					Double unitsSold = Double.parseDouble(tUnitSold);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					LocalDate localDate = LocalDate.parse(orderDate, formatter);
					int lYear = localDate.getYear();
					tUniquekey = countryName + "-" + tItemType + "-" + lYear;
					String givenValues = country + "-" + itemType + "-" + year;

					if (tUniquekey.equalsIgnoreCase(givenValues)) {

						// System.out.println(unitsSold);

						word.set(tUniquekey);
						context.write(word, new DoubleWritable(unitsSold));

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	/*
	 * Reducer Class to calculate the total units sold based on the input from
	 * Mapper
	 */
	public static class UnitSoldReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text term, Iterable<DoubleWritable> mappedUnitSold, Context context)
				throws IOException, InterruptedException {
			for (DoubleWritable val : mappedUnitSold) {

				double value = val.get();

				totalUnitSold = totalUnitSold + value;
			}

			System.out.println("##########################");
			System.out.println("Total Units Sold By a Year " + year + "  for Country :" + country + " and Item Type : "
					+ itemType + " is :: " + totalUnitSold);
			System.out.println("##########################");

			DoubleWritable output = new DoubleWritable(totalUnitSold);

			context.write(term, output);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 5) {
			System.err.println("* * * Needs five arguments....usage : AvgUnitPrice <input_file> <output_folder>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Total Unit Sold");
		job.setJarByClass(TotalUnitsSold.class);
		job.setMapperClass(UnitsSoldMapper.class);
		job.setReducerClass(UnitSoldReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		country = args[0].toLowerCase();
		itemType = args[1].toLowerCase();
		year = args[2];
		if (year == null || year.length() < 4) {
			System.out.println("Please enter a valid year");
		}
		FileInputFormat.addInputPath(job, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[4] + Math.random()));

		boolean status = job.waitForCompletion(true);

		if (status) {
			System.exit(0); // Exit with Success code
			System.out.println("Completed the Map Reduce Functionalities Successfully");
		} else {
			System.exit(1); // Exit with Failure code # 1
			System.out.println("exiting with failure");
		}

	}

}

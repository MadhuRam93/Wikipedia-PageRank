/*
 * PageRank.java
 * @author Madhu Ramachandra
 * Email: mramach2@uncc.edu
 * 
*/

package org.myorg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Program to compute Page Rank of all pages in the given set of corpus of hyperlinks
 * Formula for PageRank: PR(A) = (1-d) + d (PR(T1)/C(T1) + ... + PR(Tn)/C(Tn)); where PR(P) is pagerank of the page P, d is damping factor, C(P) is the number of outlinks from page P
 * 
 * Here, one MapReduce job is used to count the number of input links in the file
 * Second MapReduce job is used to calculate link graph
 * Third MapReduce job is recursively called to compute page ranks for each page
 * Fourth MapReduce job sorts the page ranks in descending order
 * Output of each phase is the input to next phase
 * 
*/

public class PageRank extends Configured implements Tool {
	
   public static void main( String[] args) throws  Exception {
	   	   
	   int res  = ToolRunner .run( new Configuration(), new PageRank(), args);
	   System .exit(res);
   }
   
   public int run( String[] args) throws  Exception {
	   
	 //Instantiating & configuring the job1 variable to count the number of links
	   Configuration conf1 = new Configuration();
	   
	   String intermediate_dir = "intermediate_dir";
	   Path output_path = new Path(args[1]);
	   Path intermediate_path = new Path(intermediate_dir);
	   
	   FileSystem hdfs = FileSystem.get(conf1);
	   
	   try {
		   if(hdfs.exists(output_path)){
			   hdfs.delete(output_path, true);
		   } if(hdfs.exists(intermediate_path)){
			   hdfs.delete(intermediate_path, true);
		   }
			hdfs.mkdirs(intermediate_path);
				
		} catch (IOException e) {
				e.printStackTrace();
		}
	   
	   Path links_count = new Path(intermediate_path, "links_count");
	   
	   Job job1  = Job .getInstance(conf1, "CountNumLines");
	   job1.setJarByClass(PageRank.class);

	   FileInputFormat.addInputPaths(job1, args[0]);
	   FileOutputFormat.setOutputPath(job1, links_count);
	   
	   job1.setMapperClass( MapCount .class);
	   job1.setReducerClass( ReduceCount .class);
	      
	   job1.setOutputKeyClass( Text .class);
	   job1.setOutputValueClass( IntWritable .class);
	   
	   int success1 =  job1.waitForCompletion( true)  ? 0 : 1;
	   
	   if(success1 == 0){
		   
		 //Instantiating & configuring the job2 variable to perform Map & Reduce jobs for computing link graph
		   int num_nodes = 1;
		   int pr = 1;
		   
		   Configuration conf2 = new Configuration();
		   Path link_graph_input = new Path(intermediate_path, "link_graph" );
		   
		   FileSystem fs2 = FileSystem.get(conf2);
		   Path p = new Path(links_count, "part-r-00000");
		   BufferedReader bf = new BufferedReader(new InputStreamReader(fs2.open(p)));
		   
		   String line;
		   while((line = bf.readLine()) != null) {
	   			if(line.trim().length() > 0) {
	   				System.out.println(line);
	   				
	   				String[] parts = line.split("\\s+");
	   				num_nodes = Integer.parseInt(parts[1]);
	   			}
		   }
		   bf.close();
	   			
		   Job job2  = Job .getInstance(conf2, "CalculateLinkGraph");
		   job2.setJarByClass(PageRank.class);

		   FileInputFormat.addInputPaths(job2, args[0]);
		   FileOutputFormat.setOutputPath(job2, link_graph_input);

		   job2.setMapperClass( MapLinkGraph .class);
		   job2.setReducerClass( ReduceLinkGraph .class);
		      
		   job2.setMapOutputKeyClass(Text.class);
		   job2.setMapOutputValueClass(Text.class);
		      
		   job2.setOutputKeyClass( Text .class);
		   job2.setOutputValueClass( Text .class);
		   
		   System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>NUM_NODES:" + num_nodes);
		   job2.getConfiguration().setInt("num_nodes1", num_nodes);
		   		           
		   int success2 =  job2.waitForCompletion( true)  ? 0 : 1;
		   
		   if(success2 == 0){
			   
			 //Instantiating & configuring the job3 variable to perform Map & Reduce jobs for computing page rank. This job is called iteratively for 10 times
			   for(int iteration=1; iteration<11; iteration++){
				   
				   Configuration conf3 = new Configuration();
				   
				   Job job3  = Job .getInstance(conf3, "CalculatePageRank");
				   job3.setJarByClass(PageRank.class);
				   
				   System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>PAGERANK: ITERATION " + iteration);
				   Path intermed = new Path(intermediate_path, "file"+iteration );

				   FileInputFormat.addInputPath(job3, link_graph_input);
				   FileOutputFormat.setOutputPath(job3, intermed);
				   
				   job3.setInputFormatClass(KeyValueTextInputFormat.class);
				   job3.setOutputFormatClass(TextOutputFormat.class);

				   job3.setMapperClass( Map_PR .class);
				   job3.setReducerClass( Reduce_PR .class);
				      
				   job3.setMapOutputKeyClass(Text.class);
				   job3.setMapOutputValueClass(Text.class);
				      
				   job3.setOutputKeyClass( Text .class);
				   job3.setOutputValueClass( Text .class);
				   			           				   
				   pr = job3.waitForCompletion( true)  ? 0 : 1;
				   
				   link_graph_input = intermed;
				   
			   }
			   
			   hdfs.delete(intermediate_path, true);
			   
		   if(pr == 0){
			   
			 //Instantiating & configuring the job4 variable to perform Map & Reduce jobs for sorting page ranks
			   Configuration conf4 = new Configuration();
			   
			   Job job4  = Job .getInstance(conf4, "sortPageRank");
			   job4.setJarByClass(PageRank.class);

			   System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> SORTING <<<<<<<<<<<<<<<<<<<");
			   
			   FileInputFormat.addInputPath(job4, link_graph_input);
			   FileOutputFormat.setOutputPath(job4, output_path);
			   job4.setNumReduceTasks(1);
			   
			   job4.setInputFormatClass(KeyValueTextInputFormat.class);
			   job4.setOutputFormatClass(TextOutputFormat.class);
			   
			   job4.setMapperClass( Map_Sort .class);
			   job4.setReducerClass( Reduce_Sort .class);
			   
			   job4.setSortComparatorClass(DescDoubleWritableCompare.class);
			   	      
			   job4.setMapOutputKeyClass(DoubleWritable.class);
			   job4.setMapOutputValueClass(Text.class);
			   
			   job4.setOutputKeyClass( Text .class);
			   job4.setOutputValueClass( DoubleWritable .class);
			   	           
			   return job4.waitForCompletion( true)  ? 0 : 1;
		   }   
		 }
	   }
	  
	   return 0;
   }
   
   /*
    * Mapper function 1 -  To count number of pages
    * input: offset & content (lineText) of every line in the input file.
    * output: <key, value> pairs -> <"TotalLines", 1>   
    * Mapper function reads the input file, one line at a time, and writes 1 to the output if the line is non-empty.
   */
   
   public static class MapCount extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
	   
	      private final static IntWritable one  = new IntWritable( 1);
	      private Text lineNumber = new Text("TotalLines");

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	    	  if(lineText != null){
	    		  if(lineText.toString().trim().length() > 0)
	    			  context.write(lineNumber, one);
	    	  }
	            
	         }
	     }
	   
   /*
    * Reducer function 1
    * input: output <key, value> pairs from Mapper 1 -> <"TotalLines", [list of occurrences]>
    * output: <key, value> pairs -> <"TotalLines", sum_of_occurrences>
    * Reducer function iterates over the list of values for the key and sums the values. 
   */
   
   public static class ReduceCount extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
	      @Override 
	      public void reduce( Text key,  Iterable<IntWritable > values,  Context context)
	         throws IOException,  InterruptedException {
	    	  
	         int sum  = 0;
	         
	         for ( IntWritable count  : values) {
	            sum  += count.get();
	         }
	         
	         context.write(key,  new IntWritable(sum));
	         System.out.println("Number of nodes = " + sum);
	         }
	   }

   /*
    * Mapper function 2 - To compute Link Graph
    * input: offset & content (lineText) of every line in the input file.
    * output: <key, value> pairs -> <title, url>   
    * Mapper function reads the input file, one line at a time, extracts title and outgoing links and writes it to the output
   */
   
   public static class MapLinkGraph extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
	   
	   final Pattern title = Pattern .compile("<title>(.+?)</title>");
	   final Pattern outlinks = Pattern .compile("<text(.+?)</text>");
	   final Pattern brackets = Pattern.compile("\\[(.*?])\\]");

	      public void map( LongWritable offset,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {
	    	  
	    	  System.out.println(">>>>>>>>>>>>>>>>>>>>>>>> MAP LINK GRAPH <<<<<<<<<<<<<<<<<<<<<<<<<<");

	    	  String line = lineText.toString();
	    	  System.out.println(line);
	    	  
	    	  if(line != null){
	    		  if(line.trim().length() > 0){
	    			  
	    			  try{
	    				  final Matcher match1 = title.matcher(line);
		    	   	      match1.find();
		    	   	      
		    	   	      System.out.println("title: " + match1.group(1));
		    	   	      String source = match1.group(1);
		    	   	          
		    	   	      if(source != null && !source.isEmpty()){
		    	   	    	final Matcher match2 = outlinks.matcher(line);
			    	   	      match2.find();
			    	   	      
			    	   	      System.out.println("links: " + match2.group(1));
			    	   	      String links = match2.group(1);
			    	   	          
			    	   	      final Matcher match3 = brackets.matcher(links);
			    	   	          
			    	   	      while(match3.find()){
			    	   	    	  
			    	   	    	 String url = match3.group().replace("[[", "").replace("]]", "");
			    	   	    	 
			    	   	    	 if(!url.isEmpty()){
			    	   	    		context.write(new Text("<title>" + source + "</title>"), new Text("[" + url + "]"));
				    	   	      	System.out.println("outlinks: : " + url);
			    	   	    	 }
			    	   	      }
			    		  }
	    			  } catch(Exception e){
	    				  System.out.println("exception in link graph generator mapper"+e);
	    			  }
	    			  
	    		  }
	    	  }
	      }
   }
   
   /*
    * Reducer function 2
    * input: output <key, value> pairs from Mapper 2 -> <title, [list of urls]>
    * output: <key, value> pairs -> <title, pageRank_url_concatenated_list>
    * Reducer function iterates over the list of urls for the key and concatenates the urls to the initial page rank
   */
   
   public static class ReduceLinkGraph extends Reducer<Text ,  Text ,  Text ,  Text > {
	      @Override 
	      public void reduce( Text key,  Iterable<Text > values,  Context context)
	         throws IOException,  InterruptedException {
	    	  
	    	  int rank = context.getConfiguration().getInt("num_nodes1", 1);
	    	  double rank_initial = 1/(double) rank;
	    	  System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>PAGERANK: FROM REDUCE " + rank_initial);
	    	  String out_value = rank_initial + ",";
	    	  
	    	  for(Text value : values){
	    		  out_value = out_value + value.toString() + "####";
	    	  }
	    	  
	    	  context.write(key, new Text("<text>" + out_value + "</text>"));
	    	  System.out.println(key.toString() + "		" + out_value);
	      }
	   }
   
   /*
    * Mapper function 3 - To compute Page Rank
    * input: key & content (lineText) of every line in the input file.
    * output: <key, value> pairs -> <title, url_list_for_title_page>, <url_in_title_page, new_page_rank>
    * Mapper function reads the input file, one line at a time, writes to output title of the page and its [page rank & out going links].
    * For each input page with the title, it extracts number of out going links. 
    * Calculate new page rank: page rank of title page / number of out going links
    * For each out going link, write out going link & the new page rank to the disc
   */
   
   public static class Map_PR extends Mapper<Text ,  Text ,  Text ,  Text > {
	   		
	      public void map( Text key,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException { 
	    	  
	    	  try {
	    		  
	    		  System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>In MAP<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		    	  System.out.println("KEY: " + key);
		    	  System.out.println(lineText);
		    	  
		    	  String key_str = key.toString();
		    	  String line = lineText.toString();
		    	            
		    	  String title = StringUtils.substringBetween(key_str, "<title>", "</title>");
		    	  String urls = StringUtils.substringBetween(line, "<text>", "</text>");
		          context.write(new Text(title), new Text(urls));
		          System.out.println(title + "	" + urls);
		          
		          String[] line_parts = urls.split(",");
		          int num_parts = line_parts.length;
		          System.out.println("\n Line parts Length: " + num_parts );
		          
		          double page_rank = Double.parseDouble(line_parts[0]);
		          
		          if(num_parts > 1){
		        	  String links = line_parts[1];
			          
			          if(!links.isEmpty()){
			        	  String[] out_links = links.split("####");
			  	  		  int num_out_links = out_links.length;
			  	  		  System.out.println("\n Num out links: " + num_out_links);
			  	  		  
			  	  		  double newPageRank = page_rank / (double) num_out_links;
			  	  		  System.out.println(newPageRank);
			  	  		  
			  	  		  for(String adjNode : out_links){
			  	  			  String out = StringUtils.substringBetween(adjNode, "[", "]");
			  	  			  context.write(new Text(out), new Text(newPageRank + ""));
			  	  			  
			  	  			System.out.println(out + "	" + newPageRank);
			  	  		  }
			          }
		          }
	    	   } catch(Exception e){
	    		   System.out.println("exception in link graph generator mapper"+e);
	    	   }
	      }
	   }
	   
   /*
    * Reducer function 3
    * input: output <key, value> pairs from Mapper 3 -> <title, [list of page ranks with or without urls>
    * output: <key, value> pairs -> <title_page, new_page_rank__outgoing_links>
    * Reducer function iterates over the list of values for a given title page. It sums all values of page ranks and extracts out going links from value of the title page.
    * Calculate new page rank for title page: (1 - d)+(d * sum_of_page_rank_values); where d is damping factor (d = 0.85).
    * Write to output title_page, new_page_rank & outgoing links
   */
   
   public static class Reduce_PR extends Reducer<Text ,  Text ,  Text ,  Text > {
		   
		   private static double DAMPING_FACTOR = 0.85;

		      @Override 
		      public void reduce( Text adjNode,  Iterable<Text > values,  Context context)
		         throws IOException,  InterruptedException {
		    	  
		    	  System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>In REDUCE<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
		    	  System.out.println("Source: " + adjNode);
		    	 	    	  
		    	  double sum_PR = 0.0;
		    	  String original_links = "";
		    	  boolean originalPage = false;
		    	  
		         for(Text value: values){
		        	 
		        	 String node = value.toString();
		        	 System.out.println("NODE: " + node);
		        	 
		        	 if(node.contains("####")){			//Original Page
		        		 if(node.contains(",")){
		        			 
		        			 String[] line_parts = node.split(",");
		        			 original_links = line_parts[1];
		        			 
		        			 System.out.println("Original Node");
		        			 originalPage = true;
		        		 }
		        	 } else {
		        		 if(node.contains(",")){
		        			 String[] line_parts = node.split(",");
		        			 node = line_parts[0];
		        		 }
		        			 
		        		 sum_PR = sum_PR + Double.parseDouble(node);
		        		 }		        		 
		        	 }
		         
		         if(originalPage){
		        	 double new_PR = (1.0 - DAMPING_FACTOR) + (DAMPING_FACTOR * sum_PR);
         	         
			         String original_node = "<text>" + new_PR + "," + original_links + "</text>";
			         String title = "<title>" + adjNode.toString() + "</title>";
			         context.write(new Text(title), new Text(original_node));
			         
			         System.out.println("Improved Original Node : " + original_node);
		         }	         
		      
		   }
	   }

   /*
    * Custom comparator to sort the output of Mapper 4 in decreasing order 
   */
   public static class DescDoubleWritableCompare extends WritableComparator {
	   
	   protected DescDoubleWritableCompare() {
		   super(DoubleWritable.class, true);
	   }
	   		   
	   public int compare (@SuppressWarnings("rawtypes") WritableComparable key1, @SuppressWarnings("rawtypes") WritableComparable key2){
		   
		   DoubleWritable a = (DoubleWritable) key1;
		   DoubleWritable b = (DoubleWritable) key2;
		   
		   int comp = a.compareTo(b);
		   
		   return -1*comp;
	   }
	   
   }
   
   /*
    * Mapper function 4 - To sort the ranks in decreasing order
    * input: key & content (lineText) of every line in the input file.
    * output: <key, value> pairs -> <page_rank, title>
    * Mapper function reads the input file, one line at a time, strips off all data except the title of the page & its page rank
    * Mapper writes page rank & title to the disc
   */
   
   public static class Map_Sort extends Mapper<Text ,  Text ,  DoubleWritable, Text  > {
	   
	      public void map( Text key,  Text lineText,  Context context)
	        throws  IOException,  InterruptedException {

	    	  String key_str = key.toString();
	    	  String line = lineText.toString();
	    	  
	    	  String title = StringUtils.substringBetween(key_str, "<title>", "</title>");
	    	  String urls = StringUtils.substringBetween(line, "<text>", "</text>");
	    	  
	    	  String[] line_parts = urls.split(",");
	          int num_parts = line_parts.length;
	          System.out.println("\n Line parts Length: " + num_parts );
	          
	          double page_rank = Double.parseDouble(line_parts[0]);
	          
	          context.write(new DoubleWritable(page_rank), new Text(title));
	         }
	      }
	 
   /*
    * Reducer function 4
    * input: output <key, value> pairs from Mapper 4 -> <page_rank, page_title>
    * output: <key, value> pairs -> <page_title, page_rank> in reverse sorted order
   */
   
   public static class Reduce_Sort extends Reducer<DoubleWritable, Text ,  Text ,  DoubleWritable > {
	      @Override 
	      public void reduce( DoubleWritable rank,  Iterable<Text > pages,  Context context)
	         throws IOException,  InterruptedException {
	    	  
	    	  for (Text page : pages){
	    		  context.write(page, rank);
	    	  } 
	      }
	   }
}
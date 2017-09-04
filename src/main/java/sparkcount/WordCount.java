package sparkcount;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
/** 
 * 利用Spark框架读取HDFS文件，实现WordCount示例 
 *  
 * 执行命令：spark-submit --class sparkcount.WordCount 
 * ./sparkcount-0.0.1-SNAPSHOT-jar-with-dependencies.jar 
 * spark://ulucu-ay-dev-sh022.int.uops.cn:7077 
 * hdfs://192.168.1.35:9000/logs/access.log 
 * hdfs://192.168.1.35:9000/logs/wordcount/succ
 *  
 * @author xiaodongfang 
 * 
 */  
public class WordCount {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args){
		
		
		if (args.length < 3) {  
            System.err.println("Usage: JavaWordCount <file>");  
            System.exit(1);  
        }
		
		 String Masterurl = args[0];
		 String inputSparkFile = args[1];
	     String outputSparkFile = args[2];
	     System.out.print(outputSparkFile);
		
		SparkConf conf= new SparkConf().setAppName("Spark WordCount writen by jaba").setMaster(Masterurl);
		conf.set("spark.executor.memory","512m");
		
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//sc.addJar("F:\\大数据\\jar包\\wordcount.jar");
		
		//JavaRDD<String> lines = sc.textFile("hdfs://****:9000/input/input.txt");
		JavaRDD<String> lines = sc.textFile(inputSparkFile);
		//conf.set("spark.yarn.dist.files", "src\\yarn-site.xml");
		
		 JavaRDD<String> words =lines.flatMap(new FlatMapFunction<String, String>(){

			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
			//public Iterable<String> call(String line){
				 return Arrays.asList(SPACE.split(line)).iterator();
				//return Arrays.asList(SPACE.split(line));
			}
		});
		 
		 
		JavaPairRDD<String,Integer> pairs=words.mapToPair(new PairFunction<String,String,Integer>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String,Integer> call(String word)throws Exception{
				return new Tuple2<String,Integer>(word,1);
			}
		});
		
		
		
		JavaPairRDD<String,Integer> WordsCount = pairs.reduceByKey(new Function2<Integer,Integer,Integer>(){
			private static final long serialVersionUID = 1L;

			public Integer call(Integer v1,Integer v2)throws Exception{
				System.out.print("v1:"+v1);
				System.out.print("v2:"+v2);
				return v1+v2;
			}
		});
		
		
		WordsCount.map(new Function<Tuple2<String,Integer>,String>(){
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<String,Integer>pairs)throws Exception{
				//System.out.println(pairs._1+":"+pairs._2);
				return pairs._1+":"+pairs._2;
			}
		}).saveAsTextFile(outputSparkFile);
		
		sc.close();
	}
}

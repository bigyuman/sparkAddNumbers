package Bug.pruebaGit;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Hello world!
 *
 */
public class SumNumbersFromFile 
{
    public static void main( String[] args )
    {
    	//1.-
    	SparkConf conf = new SparkConf();
    	
    	//2.-
    	JavaSparkContext sparkContext = new JavaSparkContext(conf);

    	JavaRDD<String> lines = sparkContext.textFile(args[0]);
    	
    	JavaRDD<Integer> enteros = lines.map(s -> Integer.valueOf(s));
    	
    	int sum = enteros.reduce((integer1,integer2) -> (integer1 + integer2));
    	
        System.out.println( "La suma es: " + sum );
        
        sparkContext.close();
        
        sparkContext.stop();
    }
}

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
public class SumNumbers 
{
    public static void main( String[] args )
    {
    	//1.-
    	SparkConf conf = new SparkConf();
    	
    	//2.-
    	JavaSparkContext sparkContext = new JavaSparkContext(conf);

    	//Inicializamos con un aray que es mas cómodo
    	Integer [] values= new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    	//pasamos a list que es como trabaja spark
    	List<Integer> data= Arrays.asList(values);
    	
    	//3.-
    	JavaRDD<Integer> distributedData = sparkContext.parallelize(data);
    	
    	int sum = distributedData.reduce((integer1,integer2) -> (integer1 + integer2));
    	
        System.out.println( "La suma es: " + sum );
        
        sparkContext.close();
        sparkContext.stop();
    }
}

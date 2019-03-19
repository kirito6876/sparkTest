package cn.edu360.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Java spark word count
 * @author kirito
 * @date 2019-03-19 15:13
 */
public class JavaWordCount {

    public static void main(String[] args) {
        //1、打包发布到spark的时候
//        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        //2、本地调试的时候,开四个线程
        SparkConf conf = new SparkConf().setAppName("JavaWordCount").setMaster("local[4]");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据（如果是本地调试，要配置好configuration中的program argument）
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //将单词和一组合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(w -> new Tuple2<>(w, 1));
        //聚合
        JavaPairRDD<String, Integer> reduced = wordAndOne.reduceByKey((m, n) -> m + n);
        //调整顺序
        JavaPairRDD<Integer, String> swaped = reduced.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);
        //调整顺序
        JavaPairRDD<String, Integer> result = sorted.mapToPair(tp -> tp.swap());
        //将结果保存到指定位置
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();
    }
}

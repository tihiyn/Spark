import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class Main {
    private static List<String> seqOp(List<String> accumulator, String element) {
        accumulator.add(element);
        return accumulator;
    }

    private static List<String> combOp(List<String> accumulator1, List<String> accumulator2) {
        accumulator1.addAll(accumulator2);
        return accumulator1;
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\hadoop\\");
        SparkConf conf = new SparkConf();
        conf.setAppName("TestSpark")
                .setMaster("local[*]")
                .set("spark.ui.enabled", "false");
//                .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:9870");
////        conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
////        conf.set("spark.hadoop.fs.defaultFS", "file:///");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        SparkConf conf = new SparkConf().setAppName("Spark_HW6")
//                .setMaster("local")  // yarn
//                .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:9870");;
//        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> myData1 = List.of("Henry, 42, M", "Jessica, 16, F",
                "Sharon, 21, F", "Jonathan, 27, M",
                "Shaun, 11, M", "Jasmine, 62, F"
        );

        JavaRDD<String> myRdd = sc.parallelize(myData1);
        for (String string : myRdd.take(5)) {
            System.out.println(string);
        }

        JavaRDD<String[]> mySplitData1 = myRdd.map(string -> string.split(","));
        for (String[] string : mySplitData1.take(5)) {
            for (String word : string) {
                System.out.print(word);
            }
            System.out.println();
        }

        JavaPairRDD<String, String[]> javaPairRDD = mySplitData1.keyBy(array -> array[0]);
        for (Tuple2<String, String[]> tuple : javaPairRDD.take(5)) {
            System.out.println(tuple._1 + " : " + Arrays.toString(tuple._2));
        }

        JavaRDD<Tuple2<String, String[]>> mappedTupleRDD = myRdd.map(string -> string.split(","))
                .map(array -> new Tuple2<>(array[0], array));
        for (Tuple2<String, String[]> tuple : mappedTupleRDD.take(5)) {
            System.out.println(tuple._1 + " : " + Arrays.toString(tuple._2));
        }

        JavaPairRDD<String, String[]> mappedPairRDD = myRdd.map(string -> string.split(","))
                .mapToPair(array -> new Tuple2<>(array[0], array));
        for (Tuple2<String, String[]> tuple : mappedPairRDD.take(5)) {
            System.out.println(tuple._1 + " : " + Arrays.toString(tuple._2));
        }

        List<String> myData2 = List.of("Henry,red:blue", "Jessica,pink:turquoise", "Sharon,blue:pink",
                "Jonathan,blue:green", "Shaun,sky blue:red", "Jasmine,yellow:orange");

        JavaRDD<String> myData2RDD = sc.parallelize(myData2);

        JavaPairRDD<String, String> favColorRDD = myData2RDD.map(string -> string.split(","))
                .mapToPair(array -> new Tuple2<>(array[0], array[1]))
                .flatMapValues(colors -> Arrays.stream(colors.split(":")).iterator());
        for (Tuple2<String, String> tuple : favColorRDD.take(5)) {
            System.out.println(tuple._1 + " : " + tuple._2);
        }

        JavaPairRDD<String, Integer> genderSumAge = myRdd.map(string -> string.split(", "))
                .mapToPair(array -> new Tuple2<>(array[2], Integer.valueOf(array[1])))
                .reduceByKey(Integer::sum);

        for (Tuple2<String, Integer> tuple : genderSumAge.collect()) {
            System.out.println(tuple._1 + " : " + tuple._2);
        }

        JavaPairRDD<String, Integer> maxAge = myRdd.map(string -> string.split(", "))
                .mapToPair(array -> new Tuple2<>(array[2], Integer.valueOf(array[1])))
                .reduceByKey(Integer::max);
        maxAge.foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2));

        Map<String, Long> maxAgeMap = myRdd.map(string -> string.split(", "))
                .mapToPair(array -> new Tuple2<>(array[2], Integer.valueOf(array[1])))
                .countByKey();
        for (Map.Entry<String, Long> entry : maxAgeMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }

        JavaPairRDD<String, Integer> olderPeople = myRdd.map(string -> string.split(", "))
                .mapToPair(array -> new Tuple2<>(Integer.valueOf(array[1]), array[0]))
                .sortByKey(false)
                .mapToPair(Tuple2::swap);
        for (Tuple2<String, Integer> tuple : olderPeople.take(3)) {
            System.out.println(tuple._1 + " : " + tuple._2);
        }

        JavaPairRDD<String, Iterable<String>> colorLikers = myData2RDD.map(string -> string.split(","))
                .mapToPair(array -> new Tuple2<>(array[0], array[1]))
                .flatMapValues(colors -> Arrays.stream(colors.split(":")).iterator())
                .mapToPair(Tuple2::swap)
                .groupByKey();

        for (Tuple2<String, Iterable<String>> tuple : colorLikers.collect()) {
            System.out.println(tuple._1);
            for (String name : tuple._2) {
                System.out.println("    " + name);
            }
        }

        List<String> zeroValue = new ArrayList<>();

        JavaRDD<String> myData2With2PartRDD = sc.parallelize(myData2, 2);

        JavaPairRDD<String, List<String>> colorLikers2 = myData2With2PartRDD.map(string -> string.split(","))
                .mapToPair(array -> new Tuple2<>(array[0], array[1]))
                .flatMapValues(colors -> Arrays.stream(colors.split(":")).iterator())
                .mapToPair(Tuple2::swap)
                .aggregateByKey(zeroValue, Main::seqOp, Main::combOp);

        for (Tuple2<String, List<String>> tuple : colorLikers2.collect()) {
            System.out.println(tuple._1);
            for (String name : tuple._2) {
                System.out.println("    " + name);
            }
        }

        JavaPairRDD<String, List<String>> data1RDD = myRdd.map(string -> string.split(", "))
                .mapToPair(array -> new Tuple2<>(array[0], List.of(array[1], array[2])));
        for (Tuple2<String, List<String>> tuple : data1RDD.collect()) {
            System.out.println(tuple._1 + " : " + tuple._2);
        }

        JavaPairRDD<String, List<String>> data2RDD = myData2RDD.map(string -> string.split(","))
                .mapToPair(array -> new Tuple2<>(array[0], List.of(array[1].split(":"))));
        for (Tuple2<String, List<String>> tuple : data2RDD.collect()) {
            System.out.println(tuple._1 + " : " + tuple._2);
        }

        JavaPairRDD<String, Tuple2<List<String>, List<String>>> joinedRDD = data1RDD.join(data2RDD);

        JavaRDD<String> outRdd = joinedRDD.map(tuple -> List.of(tuple._1, tuple._2._1.get(0), tuple._2._1.get(1),
                        tuple._2._2.get(0), tuple._2._2.get(1)))
                .map(list -> String.join(", ", list));
        for (String string : outRdd.take(5)) {
            System.out.println(string);
        }

//        outRdd.saveAsTextFile("file:/C:\\Users\\tihom\\IdeaProjects\\Spark_HW6\\src\\main\\resources\\persons\\");

        outRdd.saveAsTextFile("hdfs://namenode:9870/");



//        List<String> l = List.of("teststr");
//        JavaRDD<String> testRDD = sc.parallelize(l);
//
//
//        testRDD.saveAsTextFile("hdfs://localhost:9870/htest");
        sc.stop();

    }
}


package bdtc.lab2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static bdtc.lab2.NewsInteractionsCounter.countNewsInteractions;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class SparkTest {

    final String testRow1 = "1,10,13,2,16495\n";
    final String testRow2 = "2,10,4,3,16496\n";
    final String testRow3 = "3,10,4,2,16455\n";
    final String testRow4 = "4,69,12,2,16499\n";

    SparkSession ss = SparkSession
            .builder()
            .master("local")
            .appName("SparkSQLApplication")
            .getOrCreate();

//    StructType schema = DataTypes.createStructType(
//            new StructField[]{DataTypes.createStructField("id", StringType, false),
//                    DataTypes.createStructField("newsId", StringType, false),
//                    DataTypes.createStructField("person_id", StringType, false),
//                    DataTypes.createStructField("action", StringType, false),
//                    DataTypes.createStructField("last_updated", StringType, false)});

    @Test
    public void testOneInteractions() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> rowRDD = sc.parallelize(Arrays.asList(testRow1));
        JavaRDD<Row> result = countNewsInteractions(ss.createDataset(rowRDD.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getString(0).equals("10");
        assert rowList.iterator().next().getString(1).equals("2");
        assert rowList.iterator().next().getLong(2) == 1;
    }

    @Test
    public void testTwoInteractionsSameNewsIdDifferentAction() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> rowRDD = sc.parallelize(Arrays.asList(testRow1, testRow2));
        JavaRDD<Row> result = countNewsInteractions(ss.createDataset(rowRDD.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getString(0).equals("10");
        assert firstRow.getString(1).equals("2");
        assert firstRow.getLong(2) == 1L;

        assert secondRow.getString(0).equals("10");
        assert secondRow.getString(1).equals("3");
        assert secondRow.getLong(2) == 1L;

    }

    @Test
    public void testTwoInteractionsSameNewsIdAndAction() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> rowRDD = sc.parallelize(Arrays.asList(testRow1, testRow3));
        JavaRDD<Row> result = countNewsInteractions(ss.createDataset(rowRDD.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();

        assert rowList.iterator().next().getString(0).equals("10");
        assert rowList.iterator().next().getString(1).equals("2");
        assert rowList.iterator().next().getLong(2) == 2L;
    }

    @Test
    public void testTwoInteractionsDifferentNewsId() {

        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> rowRDD = sc.parallelize(Arrays.asList(testRow1, testRow4));
        JavaRDD<Row> result = countNewsInteractions(ss.createDataset(rowRDD.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);

        assert firstRow.getString(0).equals("10");
        assert firstRow.getString(1).equals("2");
        assert firstRow.getLong(2) == 1L;

        assert secondRow.getString(0).equals("69");
        assert secondRow.getString(1).equals("2");
        assert secondRow.getLong(2) == 1L;
    }

    @Test
    public void testFourInteractions() {


        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());
        JavaRDD<String> rowRDD = sc.parallelize(Arrays.asList(testRow1, testRow2, testRow3,testRow4));
        JavaRDD<Row> result = countNewsInteractions(ss.createDataset(rowRDD.rdd(), Encoders.STRING()));
        List<Row> rowList = result.collect();
        Row firstRow = rowList.get(0);
        Row secondRow = rowList.get(1);
        Row thirdRow = rowList.get(2);

        assert firstRow.getString(0).equals("10");
        assert firstRow.getString(1).equals("2");
        assert firstRow.getLong(2) == 2L;

        assert secondRow.getString(0).equals("10");
        assert secondRow.getString(1).equals("3");
        assert secondRow.getLong(2) == 1L;

        assert thirdRow.getString(0).equals("69");
        assert thirdRow.getString(1).equals("2");
        assert thirdRow.getLong(2) == 1L;
    }

}

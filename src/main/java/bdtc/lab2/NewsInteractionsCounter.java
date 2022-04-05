package bdtc.lab2;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;

import static java.time.temporal.ChronoField.YEAR;

@AllArgsConstructor
@Slf4j
public class NewsInteractionsCounter {

    /**
     * Функция подсчета количества взаимодействий пользователей с новостной лентой для каждой новости за все время.
     * На выходе - идентификатор новости, тип взаимодействия, количество типов взаимодействия с новостью
     *
     * @param inputDataset - входной DataSet для анализа
     * @return результат подсчета в формате JavaRDD
     */
    public static JavaRDD<Row> countNewsInteractions(Dataset<String> inputDataset) {

        Dataset<String> news = inputDataset.map(s -> Arrays.toString(s.split("\n")), Encoders.STRING());
        Dataset<CountInteractions> newsIdActionDataset = news.map(s -> {
            String[] fields = s.split(",");
            return new CountInteractions(fields[1], fields[3]);
                }, Encoders.bean(CountInteractions.class))
                .coalesce(1);

        // Группирует по значениям идентификатора новости и типа взаимодействия
        Dataset<Row> t = newsIdActionDataset.groupBy("newsId", "action")
                .count()
                .toDF("newsId", "action", "count")
                // сортируем
                .sort(functions.asc("newsId"),functions.asc("action"));
        log.info("===========RESULT=========== ");
        t.show();
        return t.toJavaRDD();
    }
}

package main.java.csx55.spark.Prompts;

import javax.xml.crypto.Data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import main.java.csx55.spark.DataSingleton;

/*
 * This just encapsulates the code for the first prompt. Nothing special here. You'll find the same unremarkable design in the other prompt classes. It's just a way to organize the code.
 */
public class MoviesByYearQ1 {

    public static String OUTPUT = "output/movies_by_year";

    public MoviesByYearQ1() {
        
    }

    public void run() {
        DataSingleton ds = DataSingleton.getInstance();
        Dataset<Row> movies = ds.get("movies");

        String regex = "\\(\\d{4}\\)"; // matches (year) in title

        movies = movies.filter(
            movies .col("title") .rlike(regex)
        );
        
        movies = movies.withColumn(
            "year",
            functions.regexp_extract( movies.col("title"), regex, 1)
         );

        Dataset<Row> moviesByYear = movies
            .groupBy("year")
            .count()
            .sort("year");

        String path = ds.getPath() + OUTPUT;
        moviesByYear.write().format("csv").save(path);
    }
    
}

package csx55.spark.Prompts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import csx55.spark.DataSingleton;

/*
 * This just encapsulates the code for the fifth prompt. Nothing special here. You'll find the same unremarkable design in the other prompt classes. It's just a way to organize the code.
 */
public class ComedyQ5 {
   
    public ComedyQ5() {
        
    }

    public void run() {
        DataSingleton ds = DataSingleton.getInstance();
        Dataset<Row> movies = ds.get("movies");

        movies = movies.filter(
            movies.col("genres").rlike("Comedy|comedy")
        );

        long count = movies.count();
        System.out.println("Number of movies tagged as Comedy or comedy: " + count);
    }
}

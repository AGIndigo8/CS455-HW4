package csx55.spark.Prompts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import csx55.spark.DataSingleton;

/*
 * This just encapsulates the code for the second prompt. Nothing special here. You'll find the same unremarkable design in the other prompt classes. It's just a way to organize the code.
 */
public class AvgNumGenresQ2 {
   
    public static String OUTPUT = "output/avg_num_genres";

    public AvgNumGenresQ2() {
        
    }

    public void run() {
        DataSingleton ds = DataSingleton.getInstance();
        Dataset<Row> movies = ds.get("movies");

        movies = movies.withColumn( "num_genres", functions.size( functions.split(
                movies.col("genres"),
                "\\|"
            )
        ));

        Dataset<Row> avgNumGenres = movies.agg(
            functions.avg( "num_genres")
        );

        String path = ds.getPath() + OUTPUT;
        avgNumGenres.write().format("csv").save(path);
    }
    
}

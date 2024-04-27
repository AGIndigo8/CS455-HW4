package csx55.spark.Prompts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import csx55.spark.DataSingleton;

/*
 * This just encapsulates the code for the third prompt. Nothing special here. You'll find the same unremarkable design in the other prompt classes. It's just a way to organize the code.
 */
public class GenresRank {
    public final String OUTPUT = "output/genre_rank";

    public GenresRank(){

    }

    public void run(){
        DataSingleton ds = DataSingleton.getInstance();
        Dataset<Row> movies = ds.get("movies");
        Dataset<Row> ratings = ds.get("ratings");

        // joinSets
        Dataset<Row> joined = movies.join(
            ratings,
            movies.col("movieId").equalTo(ratings.col("movieId"))
        );
        
        // spitGenres
        Dataset<Row> exploded = joined.withColumn(
            "genre",
            functions.explode(
                functions.split( joined.col("genres"), "\\|")
            )
        );

        // groupByGenre
        Dataset<Row> genreRank = exploded.groupBy("genre").agg(functions.avg("rating").as("avg_rating"));
        genreRank = genreRank.sort(functions.desc("avg_rating"));
        

        String path = ds.getPath() + OUTPUT;
        genreRank.write().format("csv").save(path);
    }
    
}

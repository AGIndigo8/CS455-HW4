package csx55.spark.Prompts;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import csx55.spark.DataSingleton;

/*
 * This just encapsulates the code for the fourth prompt. Nothing special here. You'll find the same unremarkable design in the other prompt classes. It's just a way to organize the code.
The question is: How many movies are there in each genre, remembering that a movie can have multiple genres and they should be counted for each genre they are tagged with.
 */
public class MoviesPerGenre {

    public final String OUTPUT = "output/movies_per_genre";

    public MoviesPerGenre(){

    }

    public void run(){
        DataSingleton ds = DataSingleton.getInstance();
        Dataset<Row> movies = ds.get("movies");

        // spitGenres
        Dataset<Row> exploded = movies.withColumn(
            "genre",
            functions.explode(
                functions.split( movies.col("genres"), "\\|")
            )
        );

        // groupByGenre
        Dataset<Row> genreRank = exploded.groupBy("genre").agg(functions.count("movieId").as("count"));
        genreRank = genreRank.sort(functions.desc("count"));
        

        String path = ds.getPath() + OUTPUT;
        genreRank.write().format("csv").save(path);
    }
    
}

package csx55.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;

public class DataSingleton {

    public static final String PATH = "hdfs://columbia.cs.colostate.edu:31175/H4/data";
    private static DataSingleton instance = null;

    private HashMap<String, Dataset<Row>> data = new HashMap<String, Dataset<Row>>();
    private JavaSparkContext sc;
    private SparkSession spark;


    private DataSingleton(JavaSparkContext sc, SparkSession spark) {
        this.sc = sc;
        this.spark = spark;
        loadData();
    }

    public Dataset<Row> get(String key) {
        return data.get(key);
    }

    public JavaSparkContext getContext() {
        return sc;
    }

    public String getPath() {
        return PATH;
    }

    public static DataSingleton getInstance(JavaSparkContext sc, SparkSession spark) {
        if (instance == null) {
            instance = new DataSingleton(sc, spark);
        }
        return instance;
    }

    public static DataSingleton getInstance() throws IllegalStateException {
        if (instance == null) {
            throw new IllegalStateException("DataSingleton not initialized. Use getInstance(JavaSparkContext sc) to initialize.");
        }
        return instance;
    }

    private void loadData() {
        data.put("tags", loadTags());
        data.put("ratings", loadRatings());
        data.put("movies", loadMovies());
        data.put("links", loadLinks());
        data.put("genome_scores", loadGenomeScores());
        data.put("genome_tags", loadGenomeTags());
    }

    private Dataset<Row> loadTags() {
        StructType schema = new StructType()
                .add("userId", DataTypes.IntegerType, false)
                .add("movieId", DataTypes.IntegerType, false)
                .add("tag", DataTypes.StringType, false)
                .add("timestamp", DataTypes.LongType, false);

        return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(PATH + "/tags.csv");
    }

    private Dataset<Row> loadRatings(){
        StructType schema = new StructType()
            .add("userId", DataTypes.IntegerType, false)
            .add("movieId", DataTypes.IntegerType, false)
            .add("rating", DataTypes.DoubleType, false)
            .add("timestamp", DataTypes.LongType, false);

            return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(PATH + "/ratings.csv");
    }

    private Dataset<Row> loadMovies(){
        StructType schema = new StructType()
            .add("movieId", DataTypes.IntegerType, false)
            .add("title", DataTypes.StringType, false)
            .add("genres", DataTypes.StringType, false);

            return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(PATH + "/movies.csv");
    }

    private Dataset<Row> loadLinks(){
        StructType schema = new StructType()
            .add("movieId", DataTypes.IntegerType, false)
            .add("imdbId", DataTypes.IntegerType, false)
            .add("tmdbId", DataTypes.IntegerType, false);

            return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(PATH + "/links.csv");
    }

    private Dataset<Row> loadGenomeScores(){
        StructType schema = new StructType()
            .add("movieId", DataTypes.IntegerType, false)
            .add("tagId", DataTypes.IntegerType, false)
            .add("relevance", DataTypes.DoubleType, false);

            return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(PATH + "/genome-scores.csv");
    }

    private Dataset<Row> loadGenomeTags(){
        StructType schema = new StructType()
            .add("tagId", DataTypes.IntegerType, false)
            .add("tag", DataTypes.StringType, false);

            return spark.read()
                .option("header", "true")
                .schema(schema)
                .csv(PATH + "/genome-tags.csv");
    }
     
    
}

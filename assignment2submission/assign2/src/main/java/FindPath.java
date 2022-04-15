// Matric Number:A0228565W
// Name:Yosua Muliawan

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.AggregateMessages;
import org.apache.hadoop.util.GenericOptionsParser;
import static org.apache.spark.sql.functions.udf;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.functions;
import org.apache.spark.api.java.function.ForeachFunction;
import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

public class FindPath {
    // From: https://stackoverflow.com/questions/3694380/calculating-distance-between-two-points-using-latitude-longitude
    private static double distance(double lat1, double lat2, double lon1, double lon2) {
        final int R = 6371; // Radius of the earth
        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distance = R * c * 1000; // convert to meters
        double height = 0; // For this assignment, we assume all locations have the same height.
        distance = Math.pow(distance, 2) + Math.pow(height, 2);
        return Math.sqrt(distance);
    }

    private static String add_path(String path, Long id) {
        return path + " "  + id.toString();
    }


    public static Dataset<Row> shortest_path(GraphFrame g, Long origin, Long destination, String column_name, SparkSession spark){

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        // if vertices doesn't contain destination, return
        if (g.vertices().filter(g.vertices().col("id").equalTo(destination)).count() == 0 ||
                g.vertices().filter(g.vertices().col("id").equalTo(origin)).count() == 0 ){
            return spark.emptyDataFrame().withColumn("path", functions.lit(null));
        }

        System.out.println("Origin, destination: "  + origin.toString() + " "  + destination);
        // set all visited  = false, and distance infinite
        Dataset<Row> vertices = g.vertices().withColumn("visited", functions.lit(false))
                .withColumn("distance", functions.when(g.vertices().col("id").equalTo(origin) , 0)
                        .otherwise(Float.POSITIVE_INFINITY)
                ).withColumn("path", functions.lit(null));

        // initialize g2 as all nodes, with infinite distance
        Dataset<Row> cached_vertices = AggregateMessages.getCachedDataFrame(vertices);
        GraphFrame g2 = GraphFrame.apply(cached_vertices, g.edges());
        UserDefinedFunction add_path_udf = udf(
                (path, id) -> add_path((String) path, (Long) id), DataTypes.StringType
        );


        spark.sqlContext().udf()
                .register( "add_path", (
                                WrappedArray<Long> path, Long id) -> {
                            List<Long> combined = new ArrayList<>();
                            for(int i=0; i<path.size(); i++){
                                combined.add((Long) path.apply(i));
                            }
                            combined.add(id);
                            return combined;
                        }, DataTypes.createArrayType(DataTypes.LongType)
                );


        // as long as all is not yet viist
        while(g2.vertices().filter(g2.vertices().col("visited").equalTo(false)).count() != 0){

            // get node with nearest distance
            Long current_node_id = g2.vertices().filter(g2.vertices().col("visited").equalTo(false)).sort("distance").first().getLong(0);
            System.out.println("Current node: " + current_node_id.toString());


            // to get new distances value
            Column msg_distance = AggregateMessages.edge().getField(column_name).plus(AggregateMessages.src().getField("distance")) ;

            Column msg_path = functions.when(AggregateMessages.src().getField("path").isNotNull(),
                            functions.callUDF("add_path",
                                    AggregateMessages.src().getField("path"),
                                    AggregateMessages.src().getField("id")))
                    .otherwise(functions.array(AggregateMessages.src().getField("id")));
            Column msg_for_dst = functions.when(
                    AggregateMessages.src().getField("id").equalTo(current_node_id),
                    functions.struct(
                            msg_distance,
                            msg_path
                    )
            );

            Dataset<Row> new_distances = g2.aggregateMessages()
                    .sendToDst(msg_for_dst)
                    .agg(functions.min(AggregateMessages.msg()).alias("aggMess")).coalesce(1).cache();

            // construct new visited
            Column new_visited_col = functions.when(
                    g2.vertices().col("id").equalTo(current_node_id)
                            .or(g2.vertices().col("visited").equalTo(true))
                    ,true
            ).otherwise(false);

            // construct new distance col
            Column new_distance_col = functions.when(
                    new_distances.col("aggMess").isNotNull().and((
                            new_distances.col("aggMess").getItem("col1").lt(g2.vertices().col("distance")))),
                    new_distances.col("aggMess").getItem("col1")
            ).otherwise(g2.vertices().col("distance"));

            // construct new path
            Column new_path_col = functions.when(
                    new_distances.col("aggMess").isNotNull().and(
                            new_distances.col("aggMess").getItem("col1").$less(g2.vertices().col("distance"))),
                    new_distances.col("aggMess").getItem("col2")
            ).otherwise(g2.vertices().col("path"));

            // construct new vertices
            Dataset<Row> new_vertices =
                    g2.vertices().join(new_distances, g2.vertices().col("id").equalTo(new_distances.col("id")), "left_outer")
                            .drop(new_distances.col("id"))
                            .withColumn("newVisited", new_visited_col)
                            .withColumn("newDistance", new_distance_col)
                            .withColumn("newPath", new_path_col)
                            .drop("aggMess", "visited", "distance", "path")
                            .withColumnRenamed("newVisited", "visited")
                            .withColumnRenamed("newDistance", "distance")
                            .withColumnRenamed("newPath", "path").coalesce(1);

            Dataset<Row> cached_new_vertices = AggregateMessages.getCachedDataFrame(new_vertices);
            cached_new_vertices.cache();
            g2 =  GraphFrame.apply(cached_new_vertices, g2.edges());

//            g2.vertices().sort(g2.vertices().col("distance")).show();

            // if destination is visited
//            g2.vertices().filter(g2.vertices().col("id").equalTo(destination)).show();
            if (g2.vertices().filter(g2.vertices().col("id").equalTo(destination)).first().getBoolean(1)){
                return g2.vertices().filter(g2.vertices().col("id").equalTo(destination))
                        .withColumn("newPath", functions.callUDF("add_path",
                                g2.vertices().col("path"), g2.vertices().col("id")
                        ))
                        .drop("visited", "path")
                        .withColumnRenamed("newPath", "path");
            }
        }
        return spark.emptyDataFrame().withColumn("path", functions.lit(null));
    }




    public static void main(String[] args) throws Exception {

        // parse args
        Configuration conf = new Configuration();
        String[] parsedArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (parsedArgs.length < 4) {
            System.out.println("Not enough arguments passed");
            return;
        }

        String inputFile1 = parsedArgs[0];
        String inputFile2 = parsedArgs[1];
        String outputFile1 = parsedArgs[2];
        String outputFile2 = parsedArgs[3];
        String inputDir = new File(inputFile1).getParentFile().getName();


        System.out.println("Parsed arguments");
        System.out.println(inputFile1);
        System.out.println(inputFile2);
        System.out.println(outputFile1);
        System.out.println(outputFile2);

        String OSM_PATH = inputFile1;
        String OSM_OUTPUT = outputFile1;
        String SHORTEST_PATH_QUERIES = inputFile2;
        String SHORTEST_PATH_OUTPUT = outputFile2;
        String INTER_OSM_OUTPUT = OSM_OUTPUT + "_INTER";


        File directory = new File(OSM_OUTPUT);
        directory.getParentFile().mkdirs();
        directory = new File(SHORTEST_PATH_OUTPUT);
        directory.getParentFile().mkdirs();
        directory = new File(INTER_OSM_OUTPUT);
        directory.getParentFile().mkdirs();


        // Load bfs queries
        Path stopwordsFilePath = new Path(SHORTEST_PATH_QUERIES);
        FileSystem fs = FileSystem.get(new Configuration());
        InputStreamReader is = new InputStreamReader(fs.open(stopwordsFilePath));
        BufferedReader br = new BufferedReader(is);
        String word = null;

        // Read line
        Vector<String[]> bfs_queries = new Vector<String[]>();
        while ((word = br.readLine()) != null) {
            String[] splited = word.split("\\s+");
            bfs_queries.add(splited);
            System.out.println("Shortest path queries " + splited[0] + " to " + splited[1]);
        }


        // create nodes table
        SparkSession spark = SparkSession.builder().config("spark.master", "local").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> df_nodes = spark.read()
                .format("com.databricks.spark.xml")
                .option("rootTag", "osm")
                .option("rowTag", "node")
                .load(OSM_PATH);
        df_nodes.select("_id", "_lat", "_lon").createOrReplaceTempView("nodes");

        // create ways table
        Dataset<Row> df_ways = spark.read()
                .format("com.databricks.spark.xml")
                .option("rootTag", "osm")
                .option("rowTag", "way")
                .load(OSM_PATH);
        df_ways.select("_id", "nd", "tag").createOrReplaceTempView("ways");

        // highways
        Dataset<Row> highways = df_ways.filter("array_contains(tag._k, 'highway')").select( "nd", "tag" );
        highways = highways.select(functions.monotonically_increasing_id().as("_id"),  highways.col("nd"), highways.col("tag"));


        // split into two
        Dataset<Row> oneway = highways.filter("array_contains(tag, named_struct('_VALUE', string(null), '_k', 'oneway', '_v', 'yes'))").select("_id", "nd._ref");
        Dataset<Row> biway = highways.filter("not array_contains(tag, named_struct('_VALUE', string(null), '_k', 'oneway', '_v', 'yes'))").select("_id", "nd._ref");

        System.out.println("Counts: " + highways.count() + " " + oneway.count() + " " + biway.count());
        // explode to create edge
        oneway = oneway.select(oneway.col("_id").as("_id"), functions.explode(oneway.col("_ref")).as("from"));
        biway = biway.select(biway.col("_id").as("_id"), functions.explode(biway.col("_ref")).as("from"));

        // create edge list
        WindowSpec window = Window.partitionBy("_id").orderBy("_id");
        oneway = oneway.select(oneway.col("from"), functions.lead(oneway.col("from"), 1).over(window).as("to")).coalesce(1);
        Dataset<Row> biway_forward = biway.select(biway.col("from"), functions.lead(biway.col("from"), 1).over(window).as("to")).coalesce(1);
        Dataset<Row> biway_backward = biway.select( functions.lead(biway.col("from"), 1).over(window).as("from"), biway.col("from").as("to")).coalesce(1);


        Dataset<Row> all_edges = oneway.union(biway_forward).union(biway_backward).filter("from IS NOT NULL");
        all_edges = all_edges.distinct().sort("from");
        all_edges.createOrReplaceTempView("all_edges");


        // create adj list
        Dataset<Row> adj_list = spark.sql(
                "SELECT concat(from, concat(' ', concat_ws(' ', array_sort(collect_set(to)))))  as adj FROM all_edges GROUP BY from SORT BY from"
        ).coalesce(1);


        adj_list.write().mode(SaveMode.Overwrite).format("txt").text(INTER_OSM_OUTPUT);
        Path oldPath = fs.globStatus(new Path(INTER_OSM_OUTPUT + "/part*"))[0].getPath();
        fs.rename(oldPath, new Path(OSM_OUTPUT));


        // compute distance between two nodes
        UserDefinedFunction coordinates_distance = udf(
                (Double lat1, Double lat2, Double lon1, Double lon2) -> distance(lat1, lat2, lon1, lon2), DataTypes.DoubleType
        );
        spark.udf().register("coordinates_distance", coordinates_distance);

        // compute distance between two nodes
        Dataset<Row> distance = spark.sql(
                "SELECT from as src, to as dst, coordinates_distance(lat_from, lon_from, _lat, _lon) as distance FROM" +
                        " (SELECT from, to, _lat as lat_from, _lon as lon_from FROM all_edges JOIN nodes WHERE nodes._id = from)" +
                        " JOIN nodes WHERE nodes._id = to"
        ).cache();
        df_nodes = df_nodes.select(df_nodes.col("_id")).withColumnRenamed("_id", "id").cache();


        System.out.println("Graph nodes and edges");
        df_nodes.printSchema();
        distance.printSchema();

//
//        System.out.println(df_nodes.count());
//        System.out.println(distance.count());
//        df_nodes.show();
//        distance.show();

        GraphFrame graph  = GraphFrame.apply(df_nodes, distance);
        Path output_path = new Path(SHORTEST_PATH_OUTPUT);
        FSDataOutputStream output =  fs.create(output_path);

        // Go to queries
        for (String[] query: bfs_queries){

            String src = query[0];
            String dst = query[1];
            Dataset<Row> path = shortest_path(graph, Long.parseLong(src), Long.parseLong(dst), "distance", spark);
            List<Long> paths = path.sort("distance").first().getList(2);

            // format path
            String formatted_path = src + " -> ";
            for (Long cur_node: paths){
                String node = String.valueOf(cur_node);
                if (node.equals(src) || node.equals(dst) || node.length() == 0 || node == " "){
                    continue;
                }
                formatted_path += node+ " -> " ;
            }
            formatted_path += dst + "\n";

            // save file
            System.out.println("Final path: "  + formatted_path);
            output.writeBytes(formatted_path);
            output.hflush();
        }
        output.close();

    }
}

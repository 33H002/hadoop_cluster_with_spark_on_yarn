package iitp;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;

//SPARK SQL GUIDE
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// json
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class App 
{
    SparkConf conf = new SparkConf().setMaster("yarn")
                                    .setAppName("spark")
                                    .set("spark.submit.deployMode", "client")
                                    ;

    JavaSparkContext sc = new JavaSparkContext(conf);

    /**
     * @param args
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void main( String[] args ) throws FileNotFoundException, IOException
    {
        //spark builder name build 
        SparkSession spark = SparkSession.builder()
                                         .appName("Java Spark")
                                         .getOrCreate();
        /* read Config */
        final Dataset<Row> config = spark.read().option("multiline","true").json(args[0]); // config.json path

        String[] reader = config.toJSON().collectAsList().toArray(new String[0]);
        
        JsonParser parser = new  JsonParser(); // gson before 2.8.6
        JsonObject jObj = parser.parse(reader[0]).getAsJsonObject(); // gson before 2.8.6

        //JsonObject jObj = JsonParser.parseString(reader[0]).getAsJsonObject(); // gson since 2.8.6

        /* get Path */
        String srcPath = (String) jObj.get("srcPath").getAsString();
        String destPath = (String) jObj.get("destPath").getAsString();

        /* get File Names */
        JsonArray jArray = new JsonArray();
        jArray = (JsonArray) jObj.get("files").getAsJsonArray();

        String leftTable = (String) jArray.get(0).getAsString();
        String rightTable = (String) jArray.get(1).getAsString();

        /* get Keys */
        String key = (String) jObj.get("key").getAsString();

        /* read CSV */
        Dataset<Row> leftDf = spark.read().option("header","true").option("encoding", "UTF-8").csv(srcPath.concat(leftTable));
        Dataset<Row> rightDf = spark.read().option("header","true").option("encoding", "UTF-8").csv(srcPath.concat(rightTable)).limit(0); // get columns

        List<String> leftCol = Arrays.asList(leftDf.columns());
        List<String> rightCol = Arrays.asList(rightDf.columns());

        /* (key 제외) 중복컬럼 있으면 이름 바꾸기 */
        List<String> duplicated = rightCol.stream()
            .filter(x -> !x.equals(key) && !(String.format("`%s`", x)).equals(key))
            .filter(x -> leftCol.contains(x))
            .collect(Collectors.toList());

        for(int i = 0; i < rightCol.size(); i++) {
            String col = rightCol.get(i);
            if (duplicated.contains(col)){
                rightCol.set(i, col+"_1");
            }
        }

        /* read CSV with new colaname*/
        String[] newCol = rightCol.toArray(new String[0]);
        rightDf = spark.read().option("header","false").option("encoding", "UTF-8").csv(srcPath.concat(rightTable)).toDF(newCol);
        
        /* create View */
        leftDf.createOrReplaceTempView("A");
        rightDf.createOrReplaceTempView("B");

        /* query */
        String query = String.format("SELECT * FROM A JOIN B USING(%s)", key);
        Dataset<Row> result = spark.sql(query);
        //System.out.println(query);

        //result.show(2);
        result.write().option("header","true").option("encoding", "UTF-8").csv(destPath);
        
        spark.stop();
    }
}

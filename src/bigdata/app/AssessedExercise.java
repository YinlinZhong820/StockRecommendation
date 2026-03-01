package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import bigdata.objects.*;
import bigdata.transformations.aggregation.*;
import bigdata.transformations.filters.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.pairing.AssetMetadataPairing;

public class AssessedExercise {

public static void main(String[] args) throws InterruptedException {
		
		//--------------------------------------------------------
	    // Static Configuration
	    //--------------------------------------------------------
		String datasetEndDate = "2020-04-01";
		double volatilityCeiling = 4;
		double peRatioThreshold = 25;
	
		long startTime = System.currentTimeMillis();
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}
		
		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the asset pricing data
		String pricesFile = System.getenv("BIGDATA_PRICES");
		if (pricesFile==null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries
		
		// Get the asset metadata
		String assetsFile = System.getenv("BIGDATA_ASSETS");
		if (assetsFile==null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries
		
		
    	//----------------------------------------
    	// Pre-provided code for loading the data 
    	//----------------------------------------
    	
    	// Create Datasets based on the input files
		
		// Load in the assets, this is a relatively small file
		Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
		//assetRows.printSchema();
		System.err.println(assetRows.first().toString());
		JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());
		
		// Load in the prices, this is a large file (not so much in data size, but in number of records)
    	Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
    	Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
    	Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects
		
	
		AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);
		
		System.out.println(finalRanking);
		
		System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");
		
		Thread.sleep(60000);
		
		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		
		
		
		long endTime = System.currentTimeMillis();
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			
			Instant sinstant = Instant.ofEpochSecond( startTime/1000 );
			Date sdate = Date.from( sinstant );
			
			Instant einstant = Instant.ofEpochSecond( endTime/1000 );
			Date edate = Date.from( einstant );
			
			writer.write("StartTime:"+sdate.toGMTString()+'\n');
			writer.write("EndTime:"+edate.toGMTString()+'\n');
			writer.write("Seconds: "+((endTime-startTime)/1000)+'\n');
			writer.write('\n');
			writer.write(finalRanking.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}


    public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata,
											   Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling,
											   double peRatioThreshold) {

    	//----------------------------------------
    	// Student's solution starts here
    	//----------------------------------------

		//----------------------------------------
		// Convert Dataset to RDD
		//----------------------------------------
		JavaRDD<StockPrice> priceRDD = prices.javaRDD();

		//----------------------------------------
		// Filter AssetMetadata that has null sector, industry and pe ratio
		//----------------------------------------
		JavaPairRDD<String, AssetMetadata> filterMetadata = assetMetadata
				.filter(new NullPeRatioFilter());

		//----------------------------------------
		//    1. Step1: map price to <ticker, StockPrice>
		//    2. Step2: groupByKey ticker - shuffle
		//    3. Step3: sort StockPrice by date
		//    			compute technical indicators(AssetFeatures)
		//    4. Step4: Filter AssetFeatures with volatilityCeiling
		//    5. Step5: Join AssetFeatures with AssetMetadata for the pe ratio
		//    6. Step6: Filter Join Output with PE ratio
		//    7. Step7: Map the Filter Output to Asset(Data Construction)
		//----------------------------------------

		JavaRDD<Asset> assets = priceRDD
				.mapToPair(new PriceToTickerPair())
				.groupByKey()
				.mapToPair(new ComputeIndicator(datasetEndDate))
				.filter(new VolatilityFilter(volatilityCeiling))
				.join(filterMetadata)
				.filter(new PeRatioFilter(peRatioThreshold))
				.map(new AssetBuilder());

		//----------------------------------------
		// Rank the assets based on returns
		// Return the top 5
		//----------------------------------------

		List<Asset> top5 = assets.takeOrdered(5, new AssetReturnComparator());

    	AssetRanking finalRanking = new AssetRanking(top5.toArray(new Asset[0])); // ...One of these is what your Spark program should collect

    	return finalRanking;



    }
	
}

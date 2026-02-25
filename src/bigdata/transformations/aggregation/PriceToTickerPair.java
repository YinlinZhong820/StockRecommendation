package bigdata.transformations.aggregation;

import bigdata.objects.StockPrice;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class PriceToTickerPair implements PairFunction<StockPrice, String, StockPrice> {
    @Override
    public Tuple2<String, StockPrice> call(StockPrice stockPrice){
        return new Tuple2<>(stockPrice.getStockTicker(), stockPrice);
    }
}

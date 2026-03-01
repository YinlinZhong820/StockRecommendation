package bigdata.transformations.aggregation;

import bigdata.objects.AssetFeatures;
import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volatility;
import bigdata.util.TimeUtil;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;


public class ComputeIndicator implements PairFunction<Tuple2<String, Iterable<StockPrice>>, String, AssetFeatures> {

    private final Instant endDate;
    private final Instant startDate;

    public ComputeIndicator(String datasetEndDate) {
        endDate = TimeUtil.fromDate(datasetEndDate);
        startDate = endDate.minus(365, ChronoUnit.DAYS);
    }

    @Override
    public Tuple2<String, AssetFeatures> call(Tuple2<String, Iterable<StockPrice>> tickerStockPair) throws Exception {
        // step 1 convert Iterable to List
        Iterable<StockPrice> priceIterable = tickerStockPair._2;
        String ticker = tickerStockPair._1;
        List<StockPrice> prices = new ArrayList<>();

        for (StockPrice stock: priceIterable) {
            prices.add(stock);
        }

        // Step2: filter by endDate and sort stock price by date
        List<StockPrice> filtered = new ArrayList<>();

        for (StockPrice price : prices) {
            Instant ts = TimeUtil.fromDate(price.getYear(), price.getMonth(), price.getDay());
            if (ts.isBefore(endDate) && ts.isAfter(startDate)) {
                filtered.add(price);
            }
        }

        filtered.sort((p1, p2) -> {
            if (p1.getYear() != p2.getYear())
                return Integer.compare(p1.getYear(), p2.getYear());
            if (p1.getMonth() != p2.getMonth())
                return Integer.compare(p1.getMonth(), p2.getMonth());
            return Integer.compare(p1.getDay(), p2.getDay());
        });

        prices = filtered;

        // step 3 compute indicators
        List<Double> lastPrices = new ArrayList<>();
        for (StockPrice price: prices) {
            double close = price.getClosePrice();
            lastPrices.add(close);
        }

        double returns = Returns.calculate(5, lastPrices);
        double volatility = Volatility.calculate(lastPrices);

        AssetFeatures assetFeatures = new AssetFeatures();
        assetFeatures.setAssetReturn(returns);
        assetFeatures.setAssetVolitility(volatility);

        // step 4 return features
        return new Tuple2<>(ticker, assetFeatures);
    }

}

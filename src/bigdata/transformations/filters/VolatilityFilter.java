package bigdata.transformations.filters;

import bigdata.objects.AssetFeatures;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class VolatilityFilter implements Function<Tuple2<String, AssetFeatures>, Boolean> {
    private final double volatilityCeiling;
    public VolatilityFilter(double volatilityCeiling) {
        this.volatilityCeiling = volatilityCeiling;
    }


    @Override
    public Boolean call(Tuple2<String, AssetFeatures> tickerFeaturePair) throws Exception {
        AssetFeatures assetFeatures = tickerFeaturePair._2;
        return assetFeatures.getAssetVolitility() < volatilityCeiling;
    }
}

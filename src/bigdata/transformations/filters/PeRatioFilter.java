package bigdata.transformations.filters;

import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class PeRatioFilter implements Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Boolean> {

    private final double peRatioThreshold;
    public PeRatioFilter(double peRatioThreshold) {
        this.peRatioThreshold = peRatioThreshold;
    }

    @Override
    public Boolean call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> featureMetadataPair) throws Exception {
        AssetMetadata metadata = featureMetadataPair._2._2;
        return metadata.getPriceEarningRatio() < peRatioThreshold;
    }
}
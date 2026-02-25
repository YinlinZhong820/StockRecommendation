package bigdata.transformations.filters;

import bigdata.objects.AssetMetadata;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Objects;

public class NullPeRatioFilter implements Function<Tuple2<String, AssetMetadata>, Boolean> {

    @Override
    public Boolean call(Tuple2<String, AssetMetadata> tickerMetadata) throws Exception {
        AssetMetadata metadata = tickerMetadata._2;
        return Objects.nonNull(metadata.getIndustry()) && Objects.nonNull(metadata.getSector()) && metadata.getPriceEarningRatio() != 0;
    }
}

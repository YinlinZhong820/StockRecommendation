package bigdata.transformations.aggregation;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class AssetBuilder implements Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Asset> {
    @Override
    public Asset call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> featureMetadataPair) throws Exception {
        AssetMetadata metadata = featureMetadataPair._2._2;
        AssetFeatures features = featureMetadataPair._2._1;
        features.setPeRatio(metadata.getPriceEarningRatio());

        Asset asset = new Asset();
        asset.setFeatures(featureMetadataPair._2._1);
        asset.setTicker(featureMetadataPair._1);
        asset.setName(metadata.getName());
        asset.setIndustry(metadata.getIndustry());
        asset.setSector(metadata.getSector());
        return asset;
    }
}

package bigdata.transformations.aggregation;

import bigdata.objects.Asset;

import java.io.Serializable;
import java.util.Comparator;

public class AssetReturnComparator implements Comparator<Asset>, Serializable {
    @Override
    public int compare(Asset o1, Asset o2) {
        return Double.compare(
                o2.getFeatures().getAssetReturn(),
                o1.getFeatures().getAssetReturn()
        );
    }
}

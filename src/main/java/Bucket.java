import java.io.Serializable;
import java.util.Vector;

public class Bucket implements Serializable
{
    // will change it later
    Vector<Object> clusteringKeyValues;
    int bucketNumber;
    Vector<String> overflow;
    Vector<Integer> sizes;
    public Bucket(int bucketNum)
    {
        bucketNumber = bucketNum;
        clusteringKeyValues = new Vector<Object>();
        overflow = new Vector<>();
        sizes = new Vector<>();
    }
}

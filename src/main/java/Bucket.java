import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Bucket implements Serializable
{
    Vector<Hashtable<String, Object>> IndexColumnValues;
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
        IndexColumnValues = new Vector<>();
    }
}

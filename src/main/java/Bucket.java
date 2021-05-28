import java.util.Vector;

public class Bucket
{
    Vector<Object> clusteringKeyValues;
    String overflow;
    public Bucket()
    {
        clusteringKeyValues = new Vector<Object>();
        overflow = null;
    }
}

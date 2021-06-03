import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;

public class Pair implements Serializable
{
    Object min, max;
    public Pair(Object min, Object max)
    {
        this.min = min;
        this.max = max;
    }
    public String toString()
    {
        return min+ " "+ max;
    }

}

import java.io.Serializable;

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

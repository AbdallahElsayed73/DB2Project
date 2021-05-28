import java.io.Serializable;

public class Grid implements Serializable
{
    Object[] references;
    Pair[] ranges;
    public Grid(Object[] references, Pair[] ranges)
    {
        this.references = references;
        this.ranges = ranges;
    }

}

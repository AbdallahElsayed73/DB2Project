import java.io.Serializable;

public class Grid implements Serializable
{
    Object[] references;
    Pair[] ranges;
    String columnName;
    int charIndex;
    public Grid( Pair[] ranges,String colName)
    {
        this.references =new Object[ranges.length];
        this.ranges = ranges;
        this.columnName=colName;
        this.charIndex = -1;
    }

}

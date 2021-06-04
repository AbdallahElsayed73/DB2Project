public class SQLTerm {
    String tableName;
    String columnName;
    String operator;
    Object value;

    public SQLTerm(String tableName, String columnName, String operator, Object value)
    {
        this.tableName = tableName;
        this.columnName = columnName;
        this.operator = operator;
        this.value = value;
    }
}

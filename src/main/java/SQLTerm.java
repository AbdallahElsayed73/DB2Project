public class SQLTerm {
    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;

    public SQLTerm()
    {

    }
    public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue)
    {
        this._strTableName = _strTableName;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._objValue = _objValue;
    }

    @Override
    public String toString() {
        return "SQLTerm{" +
                "_strTableName='" + _strTableName + '\'' +
                ", _strColumnName='" + _strColumnName + '\'' +
                ", _strOperator='" + _strOperator + '\'' +
                ", _objValue=" + _objValue +
                '}';
    }
}

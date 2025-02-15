using System;

namespace EasyDapper.Attributes
{
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class TableAttribute : Attribute
    {
        public string TableName { get; }
        public string Schema { get; }

        public TableAttribute(string tableName, string schema = null)
        {
            TableName = tableName;
            Schema = schema;
        }
    }
}
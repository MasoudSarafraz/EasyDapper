using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;
using EasyDapper.Interfaces;

namespace EasyDapper.Implementations
{
    internal class QueryBuilder<T> : IQueryBuilder<T>
    {
        private readonly string _connectionString;
        private readonly List<string> _filters = new List<string>();

        public QueryBuilder(string connectionString)
        {
            _connectionString = connectionString;
        }

        public IQueryBuilder<T> Where(Expression<Func<T, bool>> filter)
        {
            var expression = ParseExpression(filter.Body);
            _filters.Add(expression);
            return this;
        }

        public IEnumerable<T> Execute()
        {
            var query = BuildQuery();
            using (var connection = new SqlConnection(_connectionString))
            {
                return connection.Query<T>(query);
            }
        }

        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = BuildQuery();
            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                return await connection.QueryAsync<T>(query);
            }
        }

        private string BuildQuery()
        {
            var tableName = GetTableName<T>();
            var whereClause = _filters.Any() ? "WHERE " + string.Join(" AND ", _filters) : "";
            return $"SELECT * FROM {tableName} {whereClause}";
        }

        private string ParseExpression(Expression expression)
        {
            switch (expression.NodeType)
            {
                // Comparison Operators
                case ExpressionType.Equal:
                    if (expression is BinaryExpression binaryExpression && IsNullConstant(binaryExpression.Right))
                    {
                        return $"{ParseMemberExpression(binaryExpression.Left)} IS NULL";
                    }
                    return ParseBinaryExpression((BinaryExpression)expression, "=");
                case ExpressionType.NotEqual:
                    if (expression is BinaryExpression binaryExpressionNotEqual && IsNullConstant(binaryExpressionNotEqual.Right))
                    {
                        return $"{ParseMemberExpression(binaryExpressionNotEqual.Left)} IS NOT NULL";
                    }
                    return ParseBinaryExpression((BinaryExpression)expression, "<>");
                case ExpressionType.GreaterThan:
                    return ParseBinaryExpression((BinaryExpression)expression, ">");
                case ExpressionType.LessThan:
                    return ParseBinaryExpression((BinaryExpression)expression, "<");
                case ExpressionType.GreaterThanOrEqual:
                    return ParseBinaryExpression((BinaryExpression)expression, ">=");
                case ExpressionType.LessThanOrEqual:
                    return ParseBinaryExpression((BinaryExpression)expression, "<=");

                // Logical Operators
                case ExpressionType.AndAlso:
                    var leftAnd = ParseExpression(((BinaryExpression)expression).Left);
                    var rightAnd = ParseExpression(((BinaryExpression)expression).Right);
                    return $"({leftAnd} AND {rightAnd})";
                case ExpressionType.OrElse:
                    var leftOr = ParseExpression(((BinaryExpression)expression).Left);
                    var rightOr = ParseExpression(((BinaryExpression)expression).Right);
                    return $"({leftOr} OR {rightOr})";

                // Method Calls (LIKE, IN, BETWEEN, etc.)
                case ExpressionType.Call when ((MethodCallExpression)expression).Method.Name == "StartsWith":
                    return ParseMethodCallExpression((MethodCallExpression)expression, "LIKE", "%");
                case ExpressionType.Call when ((MethodCallExpression)expression).Method.Name == "EndsWith":
                    return ParseMethodCallExpression((MethodCallExpression)expression, "LIKE", "%");
                case ExpressionType.Call when ((MethodCallExpression)expression).Method.Name == "Contains":
                    if (((MethodCallExpression)expression).Arguments[0] is NewArrayExpression)
                        return ParseInExpression((MethodCallExpression)expression);
                    else
                        return ParseMethodCallExpression((MethodCallExpression)expression, "LIKE", "%", "%");
                case ExpressionType.Call when ((MethodCallExpression)expression).Method.Name == "IsNullOrEmpty":
                    return ParseIsNullOrEmptyExpression((MethodCallExpression)expression);
                case ExpressionType.Call when ((MethodCallExpression)expression).Method.Name == "Between":
                    return ParseBetweenExpression((MethodCallExpression)expression);

                default:
                    throw new NotSupportedException($"Expression type '{expression.NodeType}' is not supported.");
            }
        }

        private string ParseBinaryExpression(BinaryExpression expression, string @operator)
        {
            var propertyName = ParseMemberExpression(expression.Left);
            var value = ParseValueExpression(expression.Right);
            return $"{propertyName} {@operator} {value}";
        }

        private string ParseMethodCallExpression(MethodCallExpression expression, string @operator, string prefix = "", string suffix = "")
        {
            var propertyName = ParseMemberExpression(expression.Object);
            var value = ParseValueExpression(expression.Arguments[0]);
            return $"{propertyName} {@operator} '{prefix}{value}{suffix}'";
        }

        private string ParseInExpression(MethodCallExpression expression)
        {
            var propertyName = ParseMemberExpression(expression.Arguments[1]);
            var values = (IEnumerable<object>)Expression.Lambda(expression.Arguments[0]).Compile().DynamicInvoke();
            var formattedValues = string.Join(", ", values.Select(v => $"'{v}'"));
            return $"{propertyName} IN ({formattedValues})";
        }

        private string ParseIsNullOrEmptyExpression(MethodCallExpression expression)
        {
            var propertyName = ParseMemberExpression(expression.Arguments[0]);
            return $"{propertyName} IS NULL OR {propertyName} = ''";
        }

        private string ParseBetweenExpression(MethodCallExpression expression)
        {
            var propertyName = ParseMemberExpression(expression.Arguments[0]);
            var lowerBound = ParseValueExpression(expression.Arguments[1]);
            var upperBound = ParseValueExpression(expression.Arguments[2]);
            return $"{propertyName} BETWEEN {lowerBound} AND {upperBound}";
        }

        private string ParseMemberExpression(Expression expression)
        {
            if (expression is MemberExpression memberExpression)
            {
                var propertyInfo = memberExpression.Member as PropertyInfo;
                return GetColumnName(propertyInfo);
            }
            throw new NotSupportedException($"Unsupported member expression: {expression}");
        }

        private string ParseValueExpression(Expression expression)
        {
            var value = Expression.Lambda(expression).Compile().DynamicInvoke();
            if (value is string stringValue)
            {
                return $"'{stringValue}'";
            }
            return value.ToString();
        }

        private bool IsNullConstant(Expression expression)
        {
            return expression.NodeType == ExpressionType.Constant && ((ConstantExpression)expression).Value == null;
        }

        private string GetTableName<T>()
        {
            var tableAttribute = typeof(T).GetCustomAttribute<TableAttribute>();
            return tableAttribute?.TableName ?? typeof(T).Name;
        }

        private string GetColumnName(PropertyInfo property)
        {
            var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
            return columnAttribute?.ColumnName ?? property.Name;
        }
    }
}
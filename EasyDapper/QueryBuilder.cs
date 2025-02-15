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

    internal sealed class QueryBuilder<T> : IQueryBuilder<T>
    {
        private readonly string _connectionString;
        private readonly List<string> _filters = new List<string>();
        private readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();

        public QueryBuilder(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        public IQueryBuilder<T> Where(Expression<Func<T, bool>> filter)
        {
            if (filter == null)
            {
                throw new ArgumentNullException(nameof(filter));
            }                
            var expression = ParseExpression(filter.Body);
            _filters.Add(expression);
            return this;
        }

        public IEnumerable<T> Execute()
        {
            var query = BuildQuery();
            using (var connection = new SqlConnection(_connectionString))
            {
                return connection.Query<T>(query, _parameters);
            }
        }

        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = BuildQuery();
            using (var connection = new SqlConnection(_connectionString))
            {
                return await connection.QueryAsync<T>(query, _parameters);
            }
        }

        private string BuildQuery()
        {
            var tableName = GetTableName();
            var columns = GetColumns();
            var whereClause = _filters.Any() ? "WHERE " + string.Join(" AND ", _filters) : "";
            return $"SELECT {columns} FROM {tableName} {whereClause}";
        }

        private string ParseExpression(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    return HandleEqual((BinaryExpression)expression);
                case ExpressionType.NotEqual:
                    return HandleNotEqual((BinaryExpression)expression);
                case ExpressionType.GreaterThan:
                    return HandleBinary((BinaryExpression)expression, ">");
                case ExpressionType.LessThan:
                    return HandleBinary((BinaryExpression)expression, "<");
                case ExpressionType.GreaterThanOrEqual:
                    return HandleBinary((BinaryExpression)expression, ">=");
                case ExpressionType.LessThanOrEqual:
                    return HandleBinary((BinaryExpression)expression, "<=");
                case ExpressionType.AndAlso:
                    return HandleAnd((BinaryExpression)expression);
                case ExpressionType.OrElse:
                    return HandleOr((BinaryExpression)expression);
                case ExpressionType.Call:
                    return HandleMethodCall((MethodCallExpression)expression);
                default:
                    throw new NotSupportedException($"Expression type '{expression.NodeType}' is not supported.");
            }
        }

        private string HandleEqual(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
                return $"{ParseMember(expression.Left)} IS NULL";
            return HandleBinary(expression, "=");
        }

        private string HandleNotEqual(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
                return $"{ParseMember(expression.Left)} IS NOT NULL";
            return HandleBinary(expression, "<>");
        }

        private string HandleBinary(BinaryExpression expression, string op)
        {
            var left = ParseMember(expression.Left);
            var right = ParseValue(expression.Right);
            return $"{left} {op} {right}";
        }

        private string HandleAnd(BinaryExpression expression)
        {
            return Combine(expression.Left, expression.Right, "AND");
        }

        private string HandleOr(BinaryExpression expression)
        {
            return Combine(expression.Left, expression.Right, "OR");
        }

        private string Combine(Expression left, Expression right, string op)
        {
            return $"({ParseExpression(left)} {op} {ParseExpression(right)})";
        }

        private string HandleMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "StartsWith":
                    return HandleStartsWith(expression);
                case "EndsWith":
                    return HandleEndsWith(expression);
                case "Contains":
                    return HandleContains(expression);
                case "IsNullOrEmpty":
                    return HandleIsNullOrEmpty(expression);
                case "Between":
                    return HandleBetween(expression);
                default:
                    throw new NotSupportedException($"Method '{expression.Method.Name}' is not supported.");
            }

        }

        private string HandleStartsWith(MethodCallExpression expression)
        {
            return HandleLike(expression, "{0}%");
        }

        private string HandleEndsWith(MethodCallExpression expression)
        {
            return HandleLike(expression, "%{0}");
        }

        private string HandleContains(MethodCallExpression expression)
        {
            if (expression.Arguments[0] is NewArrayExpression)
            {
                return HandleIn(expression);
            }                
            return HandleLike(expression, "%{0}%");
        }

        private string HandleLike(MethodCallExpression expression, string format)
        {
            var property = ParseMember(expression.Object);
            var value = ParseValue(expression.Arguments[0], format);
            return $"{property} LIKE {value}";
        }

        private string HandleIn(MethodCallExpression expression)
        {
            var property = ParseMember(expression.Arguments[1]);
            var values = (IEnumerable<object>)Expression.Lambda(expression.Arguments[0]).Compile().DynamicInvoke();
            var parameters = values.Select(v => ParseValue(Expression.Constant(v))).ToList();
            return $"{property} IN ({string.Join(", ", parameters)})";
        }

        private string HandleIsNullOrEmpty(MethodCallExpression expression)
        {
            var property = ParseMember(expression.Arguments[0]);
            return $"({property} IS NULL OR {property} = '')";
        }

        private string HandleBetween(MethodCallExpression expression)
        {
            var property = ParseMember(expression.Arguments[0]);
            var lower = ParseValue(expression.Arguments[1]);
            var upper = ParseValue(expression.Arguments[2]);
            return $"{property} BETWEEN {lower} AND {upper}";
        }

        private string ParseMember(Expression expression)
        {
            if (expression is UnaryExpression unary)
            {
                return ParseMember(unary.Operand);
            }                
            if (expression is MemberExpression member)
            {
                var property = member.Member as PropertyInfo;
                return GetColumnName(property);
            }

            throw new NotSupportedException($"Unsupported member expression: {expression}");
        }

        private string ParseValue(Expression expression, string format = null)
        {
            var value = Expression.Lambda(expression).Compile().DynamicInvoke();
            var paramName = $"@p{_parameters.Count}";

            if (format != null && value is string str)
            {
                value = string.Format(format, str);
            }                
            _parameters[paramName] = value;
            return paramName;
        }

        private bool IsNullConstant(Expression expression)
        {
            return expression is ConstantExpression constant && constant.Value == null;
        }

        private string GetTableName()
        {
            var type = typeof(T);
            var table = type.GetCustomAttribute<TableAttribute>();
            var schema = table?.Schema;
            var name = table?.TableName ?? type.Name;

            return schema != null
                ? $"[{Escape(schema)}].[{Escape(name)}]"
                : $"[{Escape(name)}]";
        }

        private string GetColumns()
        {
            return string.Join(", ", typeof(T).GetProperties()
                .Select(p => $"{GetColumnName(p)} AS [{p.Name}]"));
        }

        private string GetColumnName(PropertyInfo property)
        {
            var column = property.GetCustomAttribute<ColumnAttribute>();
            return $"[{Escape(column?.ColumnName ?? property.Name)}]";
        }

        private static string Escape(string identifier)
        {
            return identifier?.Replace("]", "]]") ?? string.Empty;
        }
    }
}
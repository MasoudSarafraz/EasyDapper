using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal sealed class QueryBuilder<T> : IQueryBuilder<T>, IDisposable
    {
        private readonly List<string> _filters = new List<string>();
        private readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
        private readonly Lazy<IDbConnection> _lazyConnection;
        private string _orderByClause = string.Empty;
        private int? _limit = null;
        private int? _offset = null;
        private bool _isCountQuery = false;
        private string _selectedColumns = string.Empty;
        private readonly List<JoinInfo> _joins = new List<JoinInfo>();
        private readonly List<ApplyInfo> _applies = new List<ApplyInfo>();
        private string _rowNumberClause = string.Empty;
        private readonly List<string> _aggregateColumns = new List<string>();
        private readonly List<string> _groupByColumns = new List<string>();
        private string _havingClause = string.Empty;
        private int _timeOut;
        private string _distinctClause = string.Empty;
        private string _topClause = string.Empty;
        private string _unionClause = string.Empty;
        private string _intersectClause = string.Empty;
        private string _exceptClause = string.Empty;

        //internal QueryBuilder(string connectionString)
        //{
        //    if (string.IsNullOrEmpty(connectionString))
        //    {
        //        throw new ArgumentNullException("connectionString cannot be null or empty.");
        //    }
        //    _lazyConnection = new Lazy<IDbConnection>(() => new SqlConnection(connectionString));
        //}
        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection), "Connection cannot be null.");
            }
            _lazyConnection = new Lazy<IDbConnection>(() => connection);
            _timeOut = _lazyConnection.Value.ConnectionTimeout;
        }
        public IQueryBuilder<T> Where(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            var expression = ParseExpression(filter.Body);
            _filters.Add(expression);
            return this;
        }
        public IEnumerable<T> Execute()
        {
            var query = ((IQueryBuilder<T>)this).BuildQuery();
            var connection = GetOpenConnection();
            return connection.Query<T>(query, _parameters, commandTimeout: _timeOut);
        }
        public IEnumerable<TResult> Execute<TResult>()
        {
            var query = ((IQueryBuilder<T>)this).BuildQuery();
            var connection = GetOpenConnection();
            return connection.Query<TResult>(query, _parameters, commandTimeout: _timeOut);
        }
        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = ((IQueryBuilder<T>)this).BuildQuery();
            var connection = GetOpenConnection();
            return await connection.QueryAsync<T>(query, _parameters, commandTimeout: _timeOut);
        }
        public async Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            var query = ((IQueryBuilder<T>)this).BuildQuery();
            var connection = GetOpenConnection();
            return await connection.QueryAsync<TResult>(query, _parameters, commandTimeout: _timeOut);
        }
        //public IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns)
        //{
        //    var selectedColumns = new List<string>();
        //    foreach (var column in columns)
        //    {
        //        var memberExpression = column.Body as MemberExpression;
        //        if (memberExpression == null)
        //        {
        //            throw new ArgumentException("Each column must be a member expression.");
        //        }
        //        selectedColumns.Add(ParseMember(memberExpression));
        //    }
        //    _selectedColumns = string.Join(", ", selectedColumns);
        //    return this;
        //}
        public IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns)
        {
            var selectedColumns = new List<string>();
            foreach (var column in columns)
            {
                string columnName = ParseMember(column.Body);
                selectedColumns.Add(columnName);
            }
            _selectedColumns += string.Join(", ", selectedColumns);
            return this;
        }
        public IQueryBuilder<T> Select<TSource>(Expression<Func<TSource, object>>[] columns)
        {
            string currentColumns = _selectedColumns;
            foreach (var column in columns)
            {
                string columnName = ParseMember(column.Body);
                if (string.IsNullOrEmpty(currentColumns))
                    currentColumns = columnName;
                else
                    currentColumns += ", " + columnName;
            }
            _selectedColumns = currentColumns;
            return this;
        }
        public IQueryBuilder<T> Count()
        {
            _isCountQuery = true;
            return this;
        }
        public IQueryBuilder<T> OrderBy(string orderByClause)
        {
            _orderByClause = "ORDER BY " + orderByClause;
            return this;
        }
        public IQueryBuilder<T> Paging(int pageSize, int pageNumber = 1)
        {
            if (pageSize <= 0)
            {
                throw new ArgumentException("Page size must be greater than zero.");
            }
            if (pageNumber <= 0)
            {
                throw new ArgumentException("Page number must be greater than zero.");
            }
            _limit = pageSize;
            _offset = (pageNumber - 1) * pageSize; // محاسبه Offset بر اساس شماره صفحه
            return this;
        }
        public IQueryBuilder<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            AddJoin("INNER JOIN", onCondition);
            return this;
        }
        public IQueryBuilder<T> LeftJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            AddJoin("LEFT JOIN", onCondition);
            return this;
        }
        public IQueryBuilder<T> RightJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            AddJoin("RIGHT JOIN", onCondition);
            return this;
        }
        public IQueryBuilder<T> FullJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            AddJoin("FULL JOIN", onCondition);
            return this;
        }
        public IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder)
        {
            AddApply("CROSS APPLY", onCondition, subQueryBuilder);
            return this;
        }
        public IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder)
        {
            AddApply("OUTER APPLY", onCondition, subQueryBuilder);
            return this;
        }
        public IQueryBuilder<T> CustomJoin<TJoin>(string joinType, Expression<Func<T, TJoin, bool>> onCondition, params Expression<Func<TJoin, bool>>[] additionalConditions)
        {
            var parsedOnCondition = ParseExpression(onCondition.Body);
            var tableName = GetTableName(typeof(TJoin));
            var alias = "t" + (_joins.Count + 1);
            var onClause = parsedOnCondition.Replace("[", $"{alias}.");
            if (additionalConditions != null && additionalConditions.Length > 0)
            {
                foreach (var condition in additionalConditions)
                {
                    var parsedCondition = ParseExpression(condition.Body);
                    onClause += $" AND {parsedCondition}";
                }
            }
            _joins.Add(new JoinInfo
            {
                JoinType = joinType,
                TableName = tableName,
                Alias = alias,
                OnCondition = onClause
            });
            return this;
        }
        public IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy)
        {
            var partitionByColumn = ParseMember(partitionBy.Body);
            var orderByColumn = ParseMember(orderBy.Body);
            _rowNumberClause = $"ROW_NUMBER() OVER (PARTITION BY {partitionByColumn} ORDER BY {orderByColumn}) AS RowNumber";
            return this;
        }
        private void AddJoin<TLeft, TRight>(string joinType, Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            var leftTableName = GetTableName(typeof(TLeft));
            var rightTableName = GetTableName(typeof(TRight));

            var parsedOnCondition = ParseExpression(onCondition.Body);
            var tableName = GetTableName(typeof(TRight));

            _joins.Add(new JoinInfo
            {
                JoinType = joinType,
                TableName = tableName,
                OnCondition = parsedOnCondition
            });
        }
        private void AddApply<TSubQuery>(string applyType, Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subQueryBuilder)
        {
            var subQueryInstance = new QueryBuilder<TSubQuery>(_lazyConnection.Value) as IQueryBuilder<TSubQuery>;
            var subQuery = ((IQueryBuilder<TSubQuery>)subQueryBuilder(subQueryInstance)).BuildQuery(); // Explicit Call
            var parsedOnCondition = ParseExpression(onCondition.Body);
            var alias = "t" + (_applies.Count + 1);
            parsedOnCondition = parsedOnCondition.Replace("[", $"{alias}.");

            _applies.Add(new ApplyInfo
            {
                ApplyType = applyType,
                SubQuery = $"({subQuery}) AS {alias} ON {parsedOnCondition}"
            });
        }
        public IQueryBuilder<T> Sum(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = $"SUM({parsedColumn})";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += $" AS {alias}";
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }
        public IQueryBuilder<T> Avg(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = $"AVG({parsedColumn})";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += $" AS {alias}";
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }
        public IQueryBuilder<T> Min(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = $"MIN({parsedColumn})";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += $" AS {alias}";
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }
        public IQueryBuilder<T> Max(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = $"MAX({parsedColumn})";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += $" AS {alias}";
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }
        public IQueryBuilder<T> Count(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = $"COUNT({parsedColumn})";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += $" AS {alias}";
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }
        public IQueryBuilder<T> GroupBy(params Expression<Func<T, object>>[] groupByColumns)
        {
            foreach (var column in groupByColumns)
            {
                var parsedColumn = ParseMember(column.Body);
                _groupByColumns.Add(parsedColumn);
            }
            return this;
        }
        public IQueryBuilder<T> Having(Expression<Func<T, bool>> havingCondition)
        {
            _havingClause = ParseExpression(havingCondition.Body);
            return this;
        }
        public IQueryBuilder<T> Distinct()
        {
            _distinctClause = "DISTINCT";
            return this;
        }

        public IQueryBuilder<T> Top(int count)
        {
            if (count <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), "Top count must be greater than zero.");
            }
            _topClause = $"TOP {count}";
            return this;
        }

        public IQueryBuilder<T> Union(IQueryBuilder<T> queryBuilder)
        {
            if (queryBuilder == null) throw new ArgumentNullException(nameof(queryBuilder));
            _unionClause = $" UNION {queryBuilder.BuildQuery()}";
            return this;
        }

        public IQueryBuilder<T> UnionAll(IQueryBuilder<T> queryBuilder)
        {
            if (queryBuilder == null) throw new ArgumentNullException(nameof(queryBuilder));
            _unionClause = $" UNION ALL {queryBuilder.BuildQuery()}";
            return this;
        }

        public IQueryBuilder<T> Intersect(IQueryBuilder<T> queryBuilder)
        {
            if (queryBuilder == null) throw new ArgumentNullException(nameof(queryBuilder));
            _intersectClause = $" INTERSECT {queryBuilder.BuildQuery()}";
            return this;
        }

        public IQueryBuilder<T> Except(IQueryBuilder<T> queryBuilder)
        {
            if (queryBuilder == null) throw new ArgumentNullException(nameof(queryBuilder));
            _exceptClause = $" EXCEPT {queryBuilder.BuildQuery()}";
            return this;
        }


        string IQueryBuilder<T>.BuildQuery()
        {
            var selectClause = BuildSelectClause();
            var fromClause = BuildFromClause();
            var joinClauses = BuildJoinClauses();
            var applyClauses = BuildApplyClauses();
            var whereClause = BuildWhereClause();
            var orderByClause = BuildOrderByClause();
            var paginationClause = BuildPaginationClause();
            var groupByClause = BuildGroupByClause();
            var havingClause = BuildHavingClause();
            var sb = new StringBuilder();
            sb.Append(selectClause)
              .Append(fromClause)
              .Append(joinClauses)
              .Append(applyClauses);
            if (!string.IsNullOrEmpty(whereClause)) sb.Append(" ").Append(whereClause);
            if (!string.IsNullOrEmpty(groupByClause)) sb.Append(" ").Append(groupByClause);
            if (!string.IsNullOrEmpty(havingClause)) sb.Append(" ").Append(havingClause);
            if (!string.IsNullOrEmpty(orderByClause)) sb.Append(" ").Append(orderByClause);
            if (!string.IsNullOrEmpty(paginationClause)) sb.Append(" ").Append(paginationClause);
            sb.Append(_unionClause);
            sb.Append(_intersectClause);
            sb.Append(_exceptClause);

            return sb.ToString();
        }
        private string BuildGroupByClause()
        {
            return _groupByColumns.Any() ? "GROUP BY " + string.Join(", ", _groupByColumns) : "";
        }
        private string BuildHavingClause()
        {
            return !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : "";
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
                return ParseMember(expression.Left) + " IS NULL";
            return HandleBinary(expression, "=");
        }
        private string HandleNotEqual(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
                return ParseMember(expression.Left) + " IS NOT NULL";
            return HandleBinary(expression, "<>");
        }
        private string HandleBinary(BinaryExpression expression, string op)
        {
            var left = ParseMember(expression.Left);
            var right = ParseValue(expression.Right);
            return left + " " + op + " " + right;
        }
        private string HandleAnd(BinaryExpression expression)
        {
            return "(" + ParseExpression(expression.Left) + " AND " + ParseExpression(expression.Right) + ")";
        }
        private string HandleOr(BinaryExpression expression)
        {
            return "(" + ParseExpression(expression.Left) + " OR " + ParseExpression(expression.Right) + ")";
        }
        private string HandleMethodCall(MethodCallExpression expression)
        {
            switch (expression.Method.Name)
            {
                case "StartsWith":
                    return HandleLike(expression, "{0}%");
                case "EndsWith":
                    return HandleLike(expression, "%{0}");
                case "Contains":
                    return HandleLike(expression, "%{0}%");
                case "IsNullOrEmpty":
                    return HandleIsNullOrEmpty(expression);
                case "Between":
                    return HandleBetween(expression);
                default:
                    throw new NotSupportedException($"Method '{expression.Method.Name}' is not supported.");
            }
        }
        private string HandleLike(MethodCallExpression expression, string format)
        {
            var property = ParseMember(expression.Object);
            var value = ParseValue(expression.Arguments[0], format);
            return property + " LIKE " + value;
        }
        private string HandleIsNullOrEmpty(MethodCallExpression expression)
        {
            var property = ParseMember(expression.Arguments[0]);
            return "(" + property + " IS NULL OR " + property + " = '')";
        }
        private string HandleBetween(MethodCallExpression expression)
        {
            var property = ParseMember(expression.Arguments[0]);
            var lower = ParseValue(expression.Arguments[1]);
            var upper = ParseValue(expression.Arguments[2]);
            return property + " BETWEEN " + lower + " AND " + upper;
        }
        //private string ParseMember(Expression expression)
        //{
        //    if (expression is UnaryExpression unary)
        //    {
        //        return ParseMember(unary.Operand);
        //    }
        //    if (expression is MemberExpression member)
        //    {
        //        var property = member.Member as PropertyInfo;
        //        return GetColumnName(property);
        //    }
        //    throw new NotSupportedException($"Unsupported member expression: {expression}");
        //}
        private string ParseMember(Expression expression)
        {
            if (expression is UnaryExpression unary)
            {
                return ParseMember(unary.Operand);
            }
            if (expression is MemberExpression member)
            {
                var tableName = GetTableName(member.Expression.Type);
                return $"{tableName}.{GetColumnName(member.Member as PropertyInfo)}";
            }
            throw new NotSupportedException($"Unsupported expression: {expression}");
        }
        private string ParseValue(Expression expression, string format = null)
        {
            if (expression is ConstantExpression constant)
            {
                // اگر عبارت یک مقدار ثابت باشد
                var paramName = "@p" + _parameters.Count;
                var value = constant.Value;
                if (format != null && value is string str)
                {
                    value = string.Format(format, str);
                }
                _parameters[paramName] = value;
                return paramName;
            }
            else if (expression is MemberExpression member)
            {
                var tableAlias = GetTableName(member.Expression.Type);
                return $"{tableAlias}.{GetColumnName(member.Member as PropertyInfo)}";
            }
            else if (expression is BinaryExpression binary)
            {
                var left = ParseValue(binary.Left);
                var right = ParseValue(binary.Right);
                return $"{left} {GetOperator(binary.NodeType)} {right}";
            }
            else if (expression is UnaryExpression unary)
            {
                return ParseValue(unary.Operand);
            }

            throw new NotSupportedException($"Unsupported expression: {expression}");
        }
        private string GetOperator(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.Equal:
                    return "=";
                case ExpressionType.NotEqual:
                    return "<>";
                case ExpressionType.GreaterThan:
                    return ">";
                case ExpressionType.LessThan:
                    return "<";
                case ExpressionType.GreaterThanOrEqual:
                    return ">=";
                case ExpressionType.LessThanOrEqual:
                    return "<=";
                default:
                    throw new NotSupportedException($"Unsupported operator: {nodeType}");
            }
        }
        private bool IsNullConstant(Expression expression)
        {
            return expression is ConstantExpression constant && constant.Value == null;
        }
        private string GetTableName(Type type)
        {
            var table = type.GetCustomAttribute<TableAttribute>();
            var schema = table?.Schema;
            var name = table?.TableName ?? type.Name;
            return schema != null
                ? "[" + Escape(schema) + "].[" + Escape(name) + "]"
                : "[" + Escape(name) + "]";
        }
        private string GetColumnName(PropertyInfo property)
        {
            var column = property.GetCustomAttribute<ColumnAttribute>();
            return "[" + Escape(column?.ColumnName ?? property.Name) + "]";
        }
        private static string Escape(string identifier)
        {
            return identifier?.Replace("]", "]]") ?? string.Empty;
        }
        private IDbConnection GetOpenConnection()
        {
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
            return connection;

        }
        private string BuildSelectClause()
        {
            var columns = _isCountQuery ? "COUNT(*) AS TotalCount"
                : string.IsNullOrEmpty(_selectedColumns)
                ? string.Join(", ", typeof(T).GetProperties().Select(p => $"{GetTableName(typeof(T))}.{GetColumnName(p)} AS {p.Name}"))
                : _selectedColumns;
            if (!string.IsNullOrEmpty(_rowNumberClause))
            {
                columns = _rowNumberClause + ", " + columns;
            }
            if (_aggregateColumns.Any())
            {
                columns = string.Join(", ", _aggregateColumns) + (string.IsNullOrEmpty(columns) ? "" : ", " + columns);
            }
            var result = $"SELECT";
            if (!string.IsNullOrEmpty(_distinctClause))
            {
                result += $" {_distinctClause}";
            }
            if (!string.IsNullOrEmpty(_topClause))
            {
                result += $" {_topClause}";
            }
            if (!string.IsNullOrEmpty(columns))
            {
                result += $" {columns}"; ;
            }            
            return result;
        }
        private string BuildFromClause()
        {
            var tableName = GetTableName(typeof(T));
            //return $" FROM {tableName} AS {tableName}";
            return $" FROM {tableName} ";
        }
        private string BuildJoinClauses()
        {
            var sb = new StringBuilder();
            foreach (var join in _joins)
            {
                sb.Append(" ")
                  .Append(join.JoinType)
                  .Append(" ")
                  //.Append($"{join.TableName} AS {join.TableName}")
                  .Append($"{join.TableName}")
                  .Append(" ON ")
                  .Append(join.OnCondition);
            }
            return sb.ToString();
        }
        private string BuildApplyClauses()
        {
            var sb = new StringBuilder();
            foreach (var apply in _applies)
            {
                sb.Append(" ")
                  .Append(apply.ApplyType)
                  .Append(" ")
                  .Append(apply.SubQuery);
            }
            return sb.ToString();
        }
        private string BuildWhereClause()
        {
            return _filters.Any() ? "WHERE " + string.Join(" AND ", _filters) : "";
        }
        private string BuildOrderByClause()
        {
            return !string.IsNullOrEmpty(_orderByClause) ? "ORDER BY " + _orderByClause : "";
        }
        private string BuildPaginationClause()
        {
            if (_limit.HasValue)
            {
                return $"OFFSET {_offset} ROWS FETCH NEXT {_limit} ROWS ONLY";
            }
            return "";
        }
        public void Dispose()
        {
            if (_lazyConnection?.IsValueCreated == true)
            {
                _lazyConnection.Value.Dispose();
            }
        }
        // کلاس برای ذخیره اطلاعات JOIN
        private class JoinInfo
        {
            public string JoinType { get; set; }
            public string TableName { get; set; }
            public string Alias { get; set; }
            public string OnCondition { get; set; }
        }
        // کلاس برای ذخیره اطلاعات APPLY
        private class ApplyInfo
        {
            public string ApplyType { get; set; }
            public string SubQuery { get; set; }
        }
    }

}
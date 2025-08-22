using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
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
        private int _aliasCounter = 1;
        private string _defaultSchema = "dbo";
        private readonly object _lockObject = new object();
        private bool _disposed = false;
        private readonly Dictionary<string, string> _tableAliasMappings = new Dictionary<string, string>();
        private readonly Dictionary<Type, string> _typeAliasMappings = new Dictionary<Type, string>();
        private static readonly ConcurrentDictionary<Type, string> TableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<MemberInfo, string> ColumnNameCache = new ConcurrentDictionary<MemberInfo, string>();

        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection), "Connection cannot be null.");
            }
            _lazyConnection = new Lazy<IDbConnection>(() => connection);
            _timeOut = _lazyConnection.Value.ConnectionTimeout;

            // ثبت alias برای جدول اصلی
            var mainTableName = GetTableName(typeof(T));
            var mainAlias = GenerateAlias(mainTableName);
            _tableAliasMappings[mainTableName] = mainAlias;
            _typeAliasMappings[typeof(T)] = mainAlias;
        }

        public IQueryBuilder<T> WithTableAlias(string tableName, string customAlias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(customAlias))
            {
                throw new ArgumentNullException("Table name and alias cannot be null or empty.");
            }
            _tableAliasMappings[tableName] = customAlias;
            return this;
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
            var query = BuildQuery();
            var connection = GetOpenConnection();
            return connection.Query<T>(query, _parameters, commandTimeout: _timeOut);
        }

        public IEnumerable<TResult> Execute<TResult>()
        {
            var query = BuildQuery();
            var connection = GetOpenConnection();
            return connection.Query<TResult>(query, _parameters, commandTimeout: _timeOut);
        }

        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = BuildQuery();
            var connection = GetOpenConnection();
            return await connection.QueryAsync<T>(query, _parameters, commandTimeout: _timeOut);
        }

        public async Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            var query = BuildQuery();
            var connection = GetOpenConnection();
            return await connection.QueryAsync<TResult>(query, _parameters, commandTimeout: _timeOut);
        }

        public IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns)
        {
            var selectedColumns = new List<string>();
            foreach (var column in columns)
            {
                string columnName = ParseMember(column.Body);
                selectedColumns.Add(columnName);
            }
            if (_selectedColumns.Length > 0 && selectedColumns.Count > 0)
            {
                _selectedColumns += ", ";
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
                {
                    currentColumns = columnName;
                }
                else
                {
                    currentColumns += ", " + columnName;
                }
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
            _orderByClause = orderByClause;
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
            _offset = (pageNumber - 1) * pageSize;
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

        public IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subIQueryBuilder)
        {
            AddApply("CROSS APPLY", onCondition, subIQueryBuilder);
            return this;
        }

        public IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subIQueryBuilder)
        {
            AddApply("OUTER APPLY", onCondition, subIQueryBuilder);
            return this;
        }

        public IQueryBuilder<T> CustomJoin<TLeft, TRight>(string stringJoin, Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            AddJoin(stringJoin, onCondition);
            return this;
        }

        public IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber")
        {
            if (partitionBy == null)
            {
                throw new ArgumentNullException(nameof(partitionBy));
            }

            var partitionByColumns = new List<string>();
            var partitionExpression = partitionBy.Body;

            if (partitionExpression is NewExpression newPartitionExpression)
            {
                foreach (var arg in newPartitionExpression.Arguments)
                {
                    partitionByColumns.Add(ParseMember(arg));
                }
            }
            else if (partitionExpression is UnaryExpression unaryPartition)
            {
                partitionByColumns.Add(ParseMember(unaryPartition.Operand));
            }
            else
            {
                partitionByColumns.Add(ParseMember(partitionExpression));
            }

            if (orderBy == null)
            {
                throw new ArgumentNullException(nameof(orderBy));
            }

            var orderByColumns = new List<string>();
            var orderExpression = orderBy.Body;

            if (orderExpression is NewExpression newOrderExpression)
            {
                foreach (var arg in newOrderExpression.Arguments)
                {
                    orderByColumns.Add(ParseMember(arg));
                }
            }
            else if (orderExpression is UnaryExpression unaryOrder)
            {
                orderByColumns.Add(ParseMember(unaryOrder.Operand));
            }
            else
            {
                orderByColumns.Add(ParseMember(orderExpression));
            }

            _rowNumberClause = "ROW_NUMBER() OVER (PARTITION BY " + string.Join(", ", partitionByColumns) + " ORDER BY " + string.Join(", ", orderByColumns) + ") AS " + alias;
            return this;
        }

        public IQueryBuilder<T> Sum(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = "SUM(" + parsedColumn + ")";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += " AS " + alias;
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }

        public IQueryBuilder<T> Avg(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = "AVG(" + parsedColumn + ")";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += " AS " + alias;
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }

        public IQueryBuilder<T> Min(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = "MIN(" + parsedColumn + ")";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += " AS " + alias;
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }

        public IQueryBuilder<T> Max(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = "MAX(" + parsedColumn + ")";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += " AS " + alias;
            }
            _aggregateColumns.Add(aggregateColumn);
            return this;
        }

        public IQueryBuilder<T> Count(Expression<Func<T, object>> column, string alias = null)
        {
            var parsedColumn = ParseMember(column.Body);
            var aggregateColumn = "COUNT(" + parsedColumn + ")";
            if (!string.IsNullOrEmpty(alias))
            {
                aggregateColumn += " AS " + alias;
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
            _topClause = "TOP " + count;
            return this;
        }

        public IQueryBuilder<T> Union(IQueryBuilder<T> IQueryBuilder)
        {
            if (IQueryBuilder == null) throw new ArgumentNullException(nameof(IQueryBuilder));
            _unionClause = " UNION " + IQueryBuilder.BuildQuery();
            return this;
        }

        public IQueryBuilder<T> UnionAll(IQueryBuilder<T> IQueryBuilder)
        {
            if (IQueryBuilder == null) throw new ArgumentNullException(nameof(IQueryBuilder));
            _unionClause = " UNION ALL " + IQueryBuilder.BuildQuery();
            return this;
        }

        public IQueryBuilder<T> Intersect(IQueryBuilder<T> IQueryBuilder)
        {
            if (IQueryBuilder == null) throw new ArgumentNullException(nameof(IQueryBuilder));
            _intersectClause = " INTERSECT " + IQueryBuilder.BuildQuery();
            return this;
        }

        public IQueryBuilder<T> Except(IQueryBuilder<T> IQueryBuilder)
        {
            if (IQueryBuilder == null) throw new ArgumentNullException(nameof(IQueryBuilder));
            _exceptClause = " EXCEPT " + IQueryBuilder.BuildQuery();
            return this;
        }

        public IQueryBuilder<T> OrderByAscending(Expression<Func<T, object>> keySelector)
        {
            return OrderBy(keySelector, "ASC");
        }

        public IQueryBuilder<T> OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            return OrderBy(keySelector, "DESC");
        }

        public string GetRawSql()
        {
            return BuildQuery();
        }

        private IQueryBuilder<T> OrderBy(Expression<Func<T, object>> keySelector, string order)
        {
            if (keySelector == null)
            {
                throw new ArgumentNullException(nameof(keySelector));
            }

            var columns = new List<string>();
            var expression = keySelector.Body;

            if (expression is NewExpression newExpression)
            {
                foreach (var arg in newExpression.Arguments)
                {
                    columns.Add(ParseMember(arg) + " " + order);
                }
            }
            else if (expression is UnaryExpression unary)
            {
                columns.Add(ParseMember(unary.Operand) + " " + order);
            }
            else
            {
                columns.Add(ParseMember(expression) + " " + order);
            }

            if (string.IsNullOrEmpty(_orderByClause))
            {
                _orderByClause = string.Join(", ", columns);
            }
            else
            {
                _orderByClause = _orderByClause + ", " + string.Join(", ", columns);
            }

            return this;
        }

        private void AddJoin<TLeft, TRight>(string joinType, Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            var leftTableName = GetTableName(typeof(TLeft));
            var rightTableName = GetTableName(typeof(TRight));

            // اطمینان از وجود alias برای جدول سمت چپ
            if (!_tableAliasMappings.ContainsKey(leftTableName))
            {
                var leftAlias = GenerateAlias(leftTableName);
                _tableAliasMappings[leftTableName] = leftAlias;
                _typeAliasMappings[typeof(TLeft)] = leftAlias;
            }

            // اطمینان از وجود alias برای جدول سمت راست
            if (!_tableAliasMappings.ContainsKey(rightTableName))
            {
                var rightAlias = GenerateAlias(rightTableName);
                _tableAliasMappings[rightTableName] = rightAlias;
                _typeAliasMappings[typeof(TRight)] = rightAlias;
            }

            var parsedOnCondition = ParseExpression(onCondition.Body);
            parsedOnCondition = ReplaceTableNamesWithAliases(parsedOnCondition);

            _joins.Add(new JoinInfo
            {
                JoinType = joinType,
                TableName = rightTableName,
                Alias = _tableAliasMappings[rightTableName],
                OnCondition = parsedOnCondition
            });
        }

        private void AddApply<TSubQuery>(string applyType, Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subIQueryBuilder = null)
        {
            // ساخت زیرپرس‌وجو
            var subQueryInstance = new QueryBuilder<TSubQuery>(_lazyConnection.Value);
            string subQuerySql;
            string subMainAlias;

            if (subIQueryBuilder != null)
            {
                var query = subIQueryBuilder(subQueryInstance);
                if (query == null)
                {
                    throw new Exception("Failed to build sub-query");
                }
                subQuerySql = query.BuildQuery();

                // گرفتن alias جدول اصلی زیرپرس‌وجو
                subMainAlias = ((QueryBuilder<TSubQuery>)query)._tableAliasMappings[GetTableName(typeof(TSubQuery))];

                // ادغام پارامترها
                foreach (var kv in ((QueryBuilder<TSubQuery>)query)._parameters)
                {
                    _parameters[kv.Key] = kv.Value;
                }
            }
            else
            {
                // زیرپرس‌وجوی پیش‌فرض: انتخاب همه ستون‌ها از جدول
                var innerTable = GetTableName(typeof(TSubQuery));
                subMainAlias = GenerateAlias(innerTable);
                subQuerySql = $"SELECT * FROM {innerTable} AS {subMainAlias}";
            }

            // نگاشت نوع زیرپرس‌وجو به alias اصلی آن
            string previous;
            var hadPrev = _typeAliasMappings.TryGetValue(typeof(TSubQuery), out previous);
            _typeAliasMappings[typeof(TSubQuery)] = subMainAlias;

            var onConditionString = ParseExpression(onCondition.Body);

            if (hadPrev)
                _typeAliasMappings[typeof(TSubQuery)] = previous;
            else
                _typeAliasMappings.Remove(typeof(TSubQuery));

            // افزودن شرط همبستگی به زیرپرس‌وجو
            if (Regex.IsMatch(subQuerySql, @"\bWHERE\b", RegexOptions.IgnoreCase))
            {
                subQuerySql = Regex.Replace(subQuerySql, @"\bWHERE\b", match => $"WHERE ({onConditionString}) AND", RegexOptions.IgnoreCase);
            }
            else
            {
                subQuerySql += $" WHERE {onConditionString}";
            }

            // ایجاد alias برای زیرپرس‌وجو به‌عنوان جدول مشتق‌شده
            var applyAlias = GenerateAlias(GetTableName(typeof(TSubQuery)));
            _applies.Add(new ApplyInfo
            {
                ApplyType = applyType,
                SubQuery = $"({subQuerySql}) AS {applyAlias}"
            });

            // نگاشت نوع TSubQuery به alias جدول مشتق‌شده
            _typeAliasMappings[typeof(TSubQuery)] = applyAlias;
        }

        public string BuildQuery()
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
            return _groupByColumns.Any() ? "GROUP BY " + string.Join(", ", _groupByColumns) : string.Empty;
        }

        private string BuildHavingClause()
        {
            return !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : string.Empty;
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
                case ExpressionType.Not:
                    return HandleNot((UnaryExpression)expression);
                case ExpressionType.Constant:
                    return HandleConstant((ConstantExpression)expression);
                case ExpressionType.MemberAccess:
                    return HandleMemberAccess((MemberExpression)expression);
                default:
                    throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported.");
            }
        }

        private string HandleEqual(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
            {
                return ParseMember(expression.Left) + " IS NULL";
            }
            return HandleBinary(expression, "=");
        }

        private string HandleNotEqual(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
            {
                return ParseMember(expression.Left) + " IS NOT NULL";
            }
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

        private string HandleNot(UnaryExpression expression)
        {
            return "NOT (" + ParseExpression(expression.Operand) + ")";
        }

        private string HandleConstant(ConstantExpression expression)
        {
            var paramName = GetUniqueParameterName();
            _parameters[paramName] = expression.Value;
            return paramName;
        }

        private string HandleMemberAccess(MemberExpression expression)
        {
            return ParseMember(expression);
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
                    if (expression.Object != null)
                    {
                        return HandleLike(expression, "%{0}%");
                    }
                    else
                    {
                        return HandleIn(expression);
                    }
                case "IsNullOrEmpty":
                    return HandleIsNullOrEmpty(expression);
                case "Between":
                    return HandleBetween(expression);
                default:
                    throw new NotSupportedException("Method '" + expression.Method.Name + "' is not supported.");
            }
        }

        private string HandleIn(MethodCallExpression expression)
        {
            if (expression.Arguments.Count == 2)
            {
                var member = ParseMember(expression.Arguments[1]);
                var constExpr = expression.Arguments[0] as ConstantExpression;
                if (constExpr != null && constExpr.Value is System.Collections.IEnumerable enumerable)
                {
                    var items = new List<string>();
                    foreach (var obj in enumerable)
                    {
                        var p = GetUniqueParameterName();
                        _parameters[p] = obj;
                        items.Add(p);
                    }
                    return member + " IN (" + string.Join(",", items) + ")";
                }
            }
            throw new NotSupportedException("Unsupported Contains signature.");
        }

        private string HandleLike(MethodCallExpression expression, string format)
        {
            var property = ParseMember(expression.Object ?? expression.Arguments[0]);
            var valueExpr = expression.Object != null ? expression.Arguments[0] : (expression.Arguments.Count > 1 ? expression.Arguments[1] : null);
            if (valueExpr == null)
                throw new NotSupportedException("LIKE requires a value.");
            var value = ParseValue(valueExpr, format);
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

        private string ParseMember(Expression expression)
        {
            if (expression is UnaryExpression unary)
            {
                return ParseMember(unary.Operand);
            }

            if (expression is MemberExpression member)
            {
                var tableType = member.Expression != null ? member.Expression.Type : null;
                if (tableType != null)
                {
                    var alias = GetAliasForType(tableType);
                    return alias + "." + GetColumnName(member.Member as PropertyInfo);
                }
                return GetColumnName(member.Member as PropertyInfo);
            }

            if (expression is ParameterExpression parameter)
            {
                var alias = GetAliasForType(parameter.Type);
                return alias;
            }

            throw new NotSupportedException("Unsupported expression: " + expression);
        }

        private string GenerateAlias(string tableName)
        {
            lock (_lockObject)
            {
                // گرفتن نام جدول بدون Schema
                var shortTableName = tableName.Split('.').Last().Trim('[', ']');
                // محدود کردن طول نام جدول برای خوانایی
                shortTableName = shortTableName.Length > 10 ? shortTableName.Substring(0, 10) : shortTableName;
                // تولید alias با پسوند افزایشی
                return $"{shortTableName}_A{_aliasCounter++}";
            }
        }

        private string GetAliasForTable(string tableName)
        {
            if (_tableAliasMappings.TryGetValue(tableName, out var alias))
            {
                return alias;
            }
            var newAlias = GenerateAlias(tableName);
            _tableAliasMappings[tableName] = newAlias;
            return newAlias;
        }

        private string GetAliasForType(Type type)
        {
            if (_typeAliasMappings.TryGetValue(type, out var alias))
            {
                return alias;
            }

            var tableName = GetTableName(type);
            return GetAliasForTable(tableName);
        }

        private string ReplaceTableNamesWithAliases(string expression)
        {
            foreach (var mapping in _tableAliasMappings)
            {
                var pattern = Regex.Escape(mapping.Key);
                expression = Regex.Replace(expression, pattern, mapping.Value);
            }
            return expression;
        }

        private string ParseValue(Expression expression, string format = null)
        {
            if (expression is ConstantExpression constant)
            {
                var paramName = GetUniqueParameterName();
                var value = constant.Value;

                if (format != null && value is string)
                {
                    value = string.Format(format, value);
                }

                _parameters[paramName] = value;
                return paramName;
            }
            else if (expression is MemberExpression member)
            {
                if (member.Expression != null && member.Expression.NodeType == ExpressionType.Constant)
                {
                    var value = GetValue(member);
                    var paramName = GetUniqueParameterName();
                    _parameters[paramName] = value;
                    return paramName;
                }
                return ParseMember(member);
            }
            else if (expression is BinaryExpression binary)
            {
                var left = ParseValue(binary.Left);
                var right = ParseValue(binary.Right);
                return left + " " + GetOperator(binary.NodeType) + " " + right;
            }
            else if (expression is UnaryExpression unary)
            {
                return ParseValue(unary.Operand);
            }

            throw new NotSupportedException("Unsupported expression: " + expression);
        }

        private static object GetValue(MemberExpression member)
        {
            var obj = ((ConstantExpression)member.Expression).Value;
            if (member.Member is FieldInfo fi)
                return fi.GetValue(obj);
            if (member.Member is PropertyInfo pi)
                return pi.GetValue(obj, null);
            return null;
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
                case ExpressionType.AndAlso:
                    return "AND";
                case ExpressionType.OrElse:
                    return "OR";
                default:
                    throw new NotSupportedException("Unsupported operator: " + nodeType);
            }
        }

        private bool IsNullConstant(Expression expression)
        {
            var constant = expression as ConstantExpression;
            return constant != null && constant.Value == null;
        }

        private string GetTableName(Type type)
        {
            return TableNameCache.GetOrAdd(type, t =>
            {
                var tableAttr = t.GetCustomAttribute<TableAttribute>();
                return tableAttr == null
                    ? "[" + _defaultSchema + "].[" + t.Name + "]"
                    : "[" + (tableAttr.Schema ?? _defaultSchema) + "].[" + tableAttr.TableName + "]";
            });
        }

        private string GetColumnName(PropertyInfo property)
        {
            return ColumnNameCache.GetOrAdd(property, p =>
            {
                var column = p.GetCustomAttribute<ColumnAttribute>();
                return "[" + Escape(column != null ? column.ColumnName : p.Name) + "]";
            });
        }

        private static string Escape(string identifier)
        {
            return identifier != null ? identifier.Replace("]", "]]") : string.Empty;
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
            string columns;
            if (_isCountQuery)
            {
                columns = "COUNT(*) AS TotalCount";
            }
            else if (string.IsNullOrEmpty(_selectedColumns))
            {
                var alias = GetAliasForType(typeof(T));
                columns = string.Join(", ", typeof(T).GetProperties().Select(p => alias + "." + GetColumnName(p) + " AS " + p.Name));
            }
            else
            {
                columns = _selectedColumns;
            }

            if (!string.IsNullOrEmpty(_rowNumberClause))
            {
                columns = _rowNumberClause + ", " + columns;
            }

            if (_aggregateColumns.Any())
            {
                columns = string.Join(", ", _aggregateColumns) + (string.IsNullOrEmpty(columns) ? string.Empty : ", " + columns);
            }

            var result = new StringBuilder("SELECT");

            if (!string.IsNullOrEmpty(_distinctClause))
            {
                result.Append(" ").Append(_distinctClause);
            }

            if (!string.IsNullOrEmpty(_topClause))
            {
                result.Append(" ").Append(_topClause);
            }

            if (!string.IsNullOrEmpty(columns))
            {
                result.Append(" ").Append(columns);
            }

            return result.ToString();
        }

        private string BuildFromClause()
        {
            var tableName = GetTableName(typeof(T));
            var alias = GetAliasForTable(tableName);
            return " FROM " + tableName + " AS " + alias;
        }

        private string BuildJoinClauses()
        {
            var sb = new StringBuilder();
            foreach (var join in _joins)
            {
                sb.Append(" ")
                  .Append(join.JoinType)
                  .Append(" ")
                  .Append(join.TableName + " AS " + join.Alias)
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
            return _filters.Any() ? "WHERE " + string.Join(" AND ", _filters) : string.Empty;
        }

        private string BuildOrderByClause()
        {
            return !string.IsNullOrEmpty(_orderByClause) ? "ORDER BY " + _orderByClause : string.Empty;
        }

        private string BuildPaginationClause()
        {
            if (_limit.HasValue)
            {
                return "OFFSET " + _offset + " ROWS FETCH NEXT " + _limit + " ROWS ONLY";
            }
            return string.Empty;
        }

        private Expression CorrectCondition(Expression body)
        {
            var binaryExpression = body as BinaryExpression;
            if (binaryExpression != null)
            {
                var leftType = GetExpressionType(binaryExpression.Left);
                var rightType = GetExpressionType(binaryExpression.Right);

                if (leftType != null && rightType != null && leftType != rightType)
                {
                    return Expression.MakeBinary(
                        binaryExpression.NodeType,
                        binaryExpression.Right,
                        binaryExpression.Left,
                        binaryExpression.IsLiftedToNull,
                        binaryExpression.Method
                    );
                }
            }

            return body;
        }

        private Type GetExpressionType(Expression expression)
        {
            var memberExpression = expression as MemberExpression;
            if (memberExpression != null && memberExpression.Expression != null)
            {
                return memberExpression.Expression.Type;
            }

            return null;
        }

        private string GetUniqueParameterName()
        {
            lock (_lockObject)
            {
                var tableName = GetTableName(typeof(T));
                var shortTableName = tableName.Split('.').Last().Trim('[', ']');
                shortTableName = shortTableName.Length > 10 ? shortTableName.Substring(0, 10) : shortTableName;
                return $"@{shortTableName}_A{_aliasCounter}_p{_parameters.Count}";
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                if (_lazyConnection != null && _lazyConnection.IsValueCreated)
                {
                    _lazyConnection.Value.Dispose();
                }
                _disposed = true;
            }
        }

        private class JoinInfo
        {
            public string JoinType { get; set; }
            public string TableName { get; set; }
            public string Alias { get; set; }
            public string OnCondition { get; set; }
        }

        private class ApplyInfo
        {
            public string ApplyType { get; set; }
            public string SubQuery { get; set; }
        }
    }
}
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using EasyDapper.Attributes;

namespace EasyDapper
{
    internal sealed class QueryBuilder<T> : IQueryBuilder<T>, IDisposable
    {
        private readonly Lazy<IDbConnection> _lazyConnection;

        // Thread-safe collections for .NET 4.5
        private readonly ConcurrentQueue<string> _filters = new ConcurrentQueue<string>();
        private readonly ConcurrentDictionary<string, object> _parameters = new ConcurrentDictionary<string, object>();
        private readonly ConcurrentQueue<JoinInfo> _joins = new ConcurrentQueue<JoinInfo>();
        private readonly ConcurrentQueue<ApplyInfo> _applies = new ConcurrentQueue<ApplyInfo>();
        private readonly ConcurrentQueue<string> _aggregateColumns = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _groupByColumns = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _unionClauses = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _intersectClauses = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _exceptClauses = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _selectedColumnsQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _orderByQueue = new ConcurrentQueue<string>();

        // Thread-safe mappings
        private readonly ConcurrentDictionary<string, string> _tableAliasMappings = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<Type, string> _typeAliasMappings = new ConcurrentDictionary<Type, string>();

        // Instance fields (not volatile - each instance used by single thread)
        private string _rowNumberClause = string.Empty;
        private string _havingClause = string.Empty;
        private int _timeOut;
        private string _distinctClause = string.Empty;
        private string _topClause = string.Empty;
        private bool _isCountQuery = false;

        // Nullable fields with lock
        private int? _limit = null;
        private int? _offset = null;
        private readonly object _pagingLock = new object();

        // Constants and counters
        private readonly string _defaultSchema = "dbo";
        private int _aliasCounter = 1;
        private int _paramCounter = 0;
        private bool _disposed = false;

        // Thread-safe caches
        private static readonly ConcurrentDictionary<Type, string> TableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<MemberInfo, string> ColumnNameCache = new ConcurrentDictionary<MemberInfo, string>();

        // Concurrency limiter
        private static readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(Environment.ProcessorCount * 2, Environment.ProcessorCount * 2);

        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection), "Connection cannot be null.");

            _lazyConnection = new Lazy<IDbConnection>(() => connection);
            _timeOut = _lazyConnection.Value.ConnectionTimeout;

            // Register alias for main table
            var mainTableName = GetTableName(typeof(T));
            var mainAlias = GenerateAlias(mainTableName);
            _tableAliasMappings.TryAdd(mainTableName, mainAlias);
            _typeAliasMappings.TryAdd(typeof(T), mainAlias);
        }

        #region Public API - Selection/Filter
        public IQueryBuilder<T> WithTableAlias(string tableName, string customAlias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(customAlias))
                throw new ArgumentNullException("Table name and alias cannot be null or empty.");

            _tableAliasMappings.AddOrUpdate(tableName, customAlias, (k, v) => customAlias);
            return this;
        }

        public IQueryBuilder<T> Where(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            var expression = ParseExpressionWithBrackets(filter.Body);
            _filters.Enqueue(expression);
            return this;
        }

        public IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns)
        {
            if (columns == null) return this;

            foreach (var c in columns)
                _selectedColumnsQueue.Enqueue(ParseSelectMember(c.Body));

            return this;
        }

        public IQueryBuilder<T> Select<TSource>(params Expression<Func<TSource, object>>[] columns)
        {
            if (columns == null) return this;

            foreach (var c in columns)
                _selectedColumnsQueue.Enqueue(ParseSelectMember(c.Body));

            return this;
        }

        public IQueryBuilder<T> Distinct()
        {
            _distinctClause = "DISTINCT";
            return this;
        }

        public IQueryBuilder<T> Top(int count)
        {
            if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count));
            _topClause = $"TOP ({count})";
            return this;
        }

        public IQueryBuilder<T> Count()
        {
            _isCountQuery = true;
            return this;
        }

        public IQueryBuilder<T> OrderBy(string orderByClause)
        {
            if (string.IsNullOrEmpty(orderByClause)) return this;
            _orderByQueue.Enqueue(orderByClause);
            return this;
        }

        public IQueryBuilder<T> OrderByAscending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            var columns = ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " ASC");
            foreach (var col in columns) _orderByQueue.Enqueue(col);
            return this;
        }

        public IQueryBuilder<T> OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            var columns = ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " DESC");
            foreach (var col in columns) _orderByQueue.Enqueue(col);
            return this;
        }

        public IQueryBuilder<T> Paging(int pageSize, int pageNumber = 1)
        {
            if (pageSize <= 0) throw new ArgumentException("Page size must be greater than zero.");
            if (pageNumber <= 0) throw new ArgumentException("Page number must be greater than zero.");

            lock (_pagingLock)
            {
                _limit = pageSize;
                _offset = (pageNumber - 1) * pageSize;
            }
            return this;
        }
        #endregion

        #region Public API - Aggregates/Grouping/Having/RowNumber
        public IQueryBuilder<T> Sum(Expression<Func<T, object>> column, string alias = null)
        {
            return AddAggregate("SUM", column.Body, alias);
        }

        public IQueryBuilder<T> Avg(Expression<Func<T, object>> column, string alias = null)
        {
            return AddAggregate("AVG", column.Body, alias);
        }

        public IQueryBuilder<T> Min(Expression<Func<T, object>> column, string alias = null)
        {
            return AddAggregate("MIN", column.Body, alias);
        }

        public IQueryBuilder<T> Max(Expression<Func<T, object>> column, string alias = null)
        {
            return AddAggregate("MAX", column.Body, alias);
        }

        public IQueryBuilder<T> Count(Expression<Func<T, object>> column, string alias = null)
        {
            return AddAggregate("COUNT", column.Body, alias);
        }

        private IQueryBuilder<T> AddAggregate(string fn, Expression expr, string alias)
        {
            var parsed = ParseMemberWithBrackets(expr);
            var agg = string.IsNullOrEmpty(alias) ? $"{fn}({parsed})" : $"{fn}({parsed}) AS {alias}";
            _aggregateColumns.Enqueue(agg);
            return this;
        }

        public IQueryBuilder<T> GroupBy(params Expression<Func<T, object>>[] groupByColumns)
        {
            if (groupByColumns == null) return this;

            foreach (var column in groupByColumns)
                _groupByColumns.Enqueue(ParseMemberWithBrackets(column.Body));

            return this;
        }

        public IQueryBuilder<T> Having(Expression<Func<T, bool>> havingCondition)
        {
            if (havingCondition == null) throw new ArgumentNullException(nameof(havingCondition));
            _havingClause = ParseExpressionWithBrackets(havingCondition.Body);
            return this;
        }

        public IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber")
        {
            if (partitionBy == null) throw new ArgumentNullException(nameof(partitionBy));
            if (orderBy == null) throw new ArgumentNullException(nameof(orderBy));

            var parts = ExtractColumnListWithBrackets(partitionBy.Body);
            var orders = ExtractColumnListWithBrackets(orderBy.Body);
            _rowNumberClause = $"ROW_NUMBER() OVER (PARTITION BY {string.Join(", ", parts)} ORDER BY {string.Join(", ", orders)}) AS {alias}";
            return this;
        }
        #endregion

        #region Public API - Joins/Apply
        public IQueryBuilder<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        { AddJoin("INNER JOIN", onCondition); return this; }

        public IQueryBuilder<T> LeftJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        { AddJoin("LEFT JOIN", onCondition); return this; }

        public IQueryBuilder<T> RightJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        { AddJoin("RIGHT JOIN", onCondition); return this; }

        public IQueryBuilder<T> FullJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition)
        { AddJoin("FULL JOIN", onCondition); return this; }

        public IQueryBuilder<T> CustomJoin<TLeft, TRight>(string join, Expression<Func<TLeft, TRight, bool>> onCondition)
        { AddJoin(join, onCondition); return this; }

        private void AddJoin<TLeft, TRight>(string joinType, Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            var leftTableName = GetTableName(typeof(TLeft));
            var rightTableName = GetTableName(typeof(TRight));

            // Get or create alias for left table
            var leftAlias = _typeAliasMappings.GetOrAdd(typeof(TLeft), _ =>
            {
                var alias = GenerateAlias(leftTableName);
                _tableAliasMappings.TryAdd(leftTableName, alias);
                return alias;
            });

            // Always create a new alias for the right table
            var rightAlias = GenerateAlias(rightTableName);

            // Temporarily store the right alias
            string prevRightAlias;
            bool hadPrevRight = _typeAliasMappings.TryGetValue(typeof(TRight), out prevRightAlias);
            _typeAliasMappings[typeof(TRight)] = rightAlias;

            // Create parameter mapping
            var parameterAliases = new Dictionary<ParameterExpression, string>
            {
                { onCondition.Parameters[0], leftAlias },
                { onCondition.Parameters[1], rightAlias }
            };

            // Parse the condition with parameter mapping
            var parsed = ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);

            // Restore previous right alias if existed
            if (hadPrevRight)
                _typeAliasMappings[typeof(TRight)] = prevRightAlias;
            else
                _typeAliasMappings.TryRemove(typeof(TRight), out _);

            // Add join info
            _joins.Enqueue(new JoinInfo
            {
                JoinType = joinType,
                TableName = rightTableName,
                Alias = rightAlias,
                OnCondition = parsed
            });

            // Store the new alias for the right table
            _tableAliasMappings.TryAdd(rightTableName + "_" + rightAlias, rightAlias);
        }

        public IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
        { AddApply("CROSS APPLY", onCondition, subBuilder); return this; }

        public IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
        { AddApply("OUTER APPLY", onCondition, subBuilder); return this; }

        private void AddApply<TSubQuery>(string applyType, Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
        {
            if (subBuilder == null) throw new ArgumentNullException(nameof(subBuilder));

            var sub = new QueryBuilder<TSubQuery>(_lazyConnection.Value);
            var qb = subBuilder(sub);
            if (qb == null) throw new InvalidOperationException("Sub-query builder returned null");

            var subQB = (QueryBuilder<TSubQuery>)qb;
            string subSql = subQB.BuildQuery();

            // Get sub-query alias
            string subMainAlias = subQB._typeAliasMappings.GetOrAdd(typeof(TSubQuery), _ => subQB.GenerateAlias(GetTableName(typeof(TSubQuery))));

            // Temporarily register alias for condition parsing
            string prev;
            bool hadPrev = _typeAliasMappings.TryGetValue(typeof(TSubQuery), out prev);
            _typeAliasMappings.TryAdd(typeof(TSubQuery), subMainAlias);

            var onSql = ParseExpressionWithBrackets(onCondition.Body);

            // Restore previous alias if existed
            if (hadPrev) _typeAliasMappings.TryAdd(typeof(TSubQuery), prev);
            else _typeAliasMappings.TryRemove(typeof(TSubQuery), out _);

            // Append correlation condition to subSql
            if (Regex.IsMatch(subSql, @"\bWHERE\b", RegexOptions.IgnoreCase))
                subSql = Regex.Replace(subSql, @"\bWHERE\b", m => $"WHERE ({onSql}) AND", RegexOptions.IgnoreCase);
            else
                subSql += " WHERE " + onSql;

            // Merge parameters with unique names
            foreach (var kv in subQB._parameters)
            {
                var newName = kv.Key;
                if (_parameters.ContainsKey(newName))
                {
                    var fresh = GetUniqueParameterName();
                    subSql = Regex.Replace(subSql, $@"(?<![A-Za-z0-9_]){Regex.Escape(newName)}(?![A-Za-z0-9_])", fresh);
                    newName = fresh;
                }
                _parameters.TryAdd(newName, kv.Value);
            }

            var applyAlias = GenerateAlias(GetTableName(typeof(TSubQuery)));
            _applies.Enqueue(new ApplyInfo
            {
                ApplyType = applyType,
                SubQuery = $"({subSql}) AS {applyAlias}"
            });
            _typeAliasMappings.TryAdd(typeof(TSubQuery), applyAlias);
        }
        #endregion

        #region Public API - Set Operations
        public IQueryBuilder<T> Union(IQueryBuilder<T> other)
        { return AddSet("UNION", other); }

        public IQueryBuilder<T> UnionAll(IQueryBuilder<T> other)
        { return AddSet("UNION ALL", other); }

        public IQueryBuilder<T> Intersect(IQueryBuilder<T> other)
        { return AddSet("INTERSECT", other, isIntersect: true); }

        public IQueryBuilder<T> Except(IQueryBuilder<T> other)
        { return AddSet("EXCEPT", other, isExcept: true); }

        private IQueryBuilder<T> AddSet(string op, IQueryBuilder<T> other, bool isIntersect = false, bool isExcept = false)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));

            var sql = (other as QueryBuilder<T>)?.BuildQuery() ?? other.BuildQuery();
            var clause = $"{op} (" + sql + ")";

            if (isIntersect) _intersectClauses.Enqueue(clause);
            else if (isExcept) _exceptClauses.Enqueue(clause);
            else _unionClauses.Enqueue(clause);

            return this;
        }
        #endregion

        #region Public API - Execution/Utility
        public IEnumerable<T> Execute()
        {
            var query = BuildQuery();
            return GetOpenConnection().Query<T>(query, _parameters, commandTimeout: _timeOut);
        }

        public IEnumerable<TResult> Execute<TResult>()
        {
            var query = BuildQuery();
            return GetOpenConnection().Query<TResult>(query, _parameters, commandTimeout: _timeOut);
        }

        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = BuildQuery();
            await _connectionSemaphore.WaitAsync();
            try
            {
                return await GetOpenConnection().QueryAsync<T>(query, _parameters, commandTimeout: _timeOut);
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        public async Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            var query = BuildQuery();
            await _connectionSemaphore.WaitAsync();
            try
            {
                return await GetOpenConnection().QueryAsync<TResult>(query, _parameters, commandTimeout: _timeOut);
            }
            finally
            {
                _connectionSemaphore.Release();
            }
        }

        public string GetRawSql() => BuildQuery();
        #endregion

        #region Build Query
        public string BuildQuery()
        {
            ValidateAggregates();
            var selectClause = BuildSelectClause();
            var fromClause = BuildFromClause();
            var joinClauses = BuildJoinClauses();
            var applyClauses = BuildApplyClauses();
            var whereClause = BuildWhereClause();
            var groupByClause = BuildGroupByClause();
            var havingClause = BuildHavingClause();
            var orderByClause = BuildOrderByClause();
            var paginationClause = BuildPaginationClause(orderByClause);

            var sb = new StringBuilder();
            sb.Append(selectClause)
              .Append(fromClause)
              .Append(joinClauses)
              .Append(applyClauses);

            if (!string.IsNullOrEmpty(whereClause)) sb.Append(' ').Append(whereClause);
            if (!string.IsNullOrEmpty(groupByClause)) sb.Append(' ').Append(groupByClause);
            if (!string.IsNullOrEmpty(havingClause)) sb.Append(' ').Append(havingClause);
            if (!string.IsNullOrEmpty(orderByClause)) sb.Append(' ').Append(orderByClause);
            if (!string.IsNullOrEmpty(paginationClause)) sb.Append(' ').Append(paginationClause);

            foreach (var u in _unionClauses) sb.Append(' ').Append(u);
            foreach (var i in _intersectClauses) sb.Append(' ').Append(i);
            foreach (var e in _exceptClauses) sb.Append(' ').Append(e);

            return sb.ToString();
        }

        private void ValidateAggregates()
        {
            if (_aggregateColumns.Any() && !_groupByColumns.Any() && _selectedColumnsQueue.Any())
                throw new InvalidOperationException("When using aggregate functions, either use GROUP BY or avoid selecting non-aggregate columns.");
        }

        private string BuildSelectClause()
        {
            string columns;
            if (_isCountQuery)
            {
                columns = "COUNT(*) AS TotalCount";
            }
            else if (!_selectedColumnsQueue.Any())
            {
                var alias = GetAliasForType(typeof(T));
                columns = string.Join(", ", typeof(T).GetProperties().Select(p =>
                    $"{alias}.[{GetColumnName(p)}] AS {p.Name}"));
            }
            else
            {
                columns = string.Join(", ", _selectedColumnsQueue.ToArray());
            }

            if (!string.IsNullOrEmpty(_rowNumberClause))
                columns = _rowNumberClause + ", " + columns;

            if (_aggregateColumns.Any())
                columns = string.Join(", ", _aggregateColumns.ToArray()) + (string.IsNullOrEmpty(columns) ? string.Empty : ", " + columns);

            var result = new StringBuilder("SELECT");
            if (!string.IsNullOrEmpty(_distinctClause)) result.Append(' ').Append(_distinctClause);
            if (!string.IsNullOrEmpty(_topClause)) result.Append(' ').Append(_topClause);
            if (!string.IsNullOrEmpty(columns)) result.Append(' ').Append(columns);

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
                sb.Append(' ').Append(join.JoinType)
                  .Append(' ').Append(join.TableName + " AS " + join.Alias)
                  .Append(" ON ").Append(join.OnCondition);
            }
            return sb.ToString();
        }

        private string BuildApplyClauses()
        {
            var sb = new StringBuilder();
            foreach (var apply in _applies)
                sb.Append(' ').Append(apply.ApplyType).Append(' ').Append(apply.SubQuery);
            return sb.ToString();
        }

        private string BuildWhereClause() => _filters.Any() ? "WHERE " + string.Join(" AND ", _filters.ToArray()) : string.Empty;
        private string BuildGroupByClause() => _groupByColumns.Any() ? "GROUP BY " + string.Join(", ", _groupByColumns.ToArray()) : string.Empty;

        private string BuildHavingClause()
        {
            return !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : string.Empty;
        }

        private string BuildOrderByClause()
        {
            return _orderByQueue.Any() ? "ORDER BY " + string.Join(", ", _orderByQueue.ToArray()) : string.Empty;
        }

        private string BuildPaginationClause(string orderByClause)
        {
            int? limit, offset;
            lock (_pagingLock)
            {
                limit = _limit;
                offset = _offset;
            }

            if (limit.HasValue)
            {
                if (string.IsNullOrEmpty(orderByClause))
                {
                    _orderByQueue.Enqueue("(SELECT 1)");
                    orderByClause = BuildOrderByClause();
                }
                return $"OFFSET {offset} ROWS FETCH NEXT {limit} ROWS ONLY";
            }
            return string.Empty;
        }
        #endregion

        #region Parsing
        private List<string> ExtractColumnList(Expression expr)
        {
            var list = new List<string>();
            var unary = expr as UnaryExpression;
            if (unary != null)
                expr = unary.Operand;

            var newExpr = expr as NewExpression;
            if (newExpr != null)
            {
                foreach (var a in newExpr.Arguments)
                    list.Add(ParseMember(a));
            }
            else
            {
                list.Add(ParseMember(expr));
            }
            return list;
        }

        private List<string> ExtractColumnListWithBrackets(Expression expr)
        {
            var list = new List<string>();
            var unary = expr as UnaryExpression;
            if (unary != null)
                expr = unary.Operand;

            var newExpr = expr as NewExpression;
            if (newExpr != null)
            {
                foreach (var a in newExpr.Arguments)
                    list.Add(ParseMemberWithBrackets(a));
            }
            else
            {
                list.Add(ParseMemberWithBrackets(expr));
            }
            return list;
        }

        private string ParseExpression(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal: return HandleEqual((BinaryExpression)expression);
                case ExpressionType.NotEqual: return HandleNotEqual((BinaryExpression)expression);
                case ExpressionType.GreaterThan: return HandleBinary((BinaryExpression)expression, ">");
                case ExpressionType.LessThan: return HandleBinary((BinaryExpression)expression, "<");
                case ExpressionType.GreaterThanOrEqual: return HandleBinary((BinaryExpression)expression, ">=");
                case ExpressionType.LessThanOrEqual: return HandleBinary((BinaryExpression)expression, "<=");
                case ExpressionType.AndAlso: return "(" + ParseExpression(((BinaryExpression)expression).Left) + " AND " + ParseExpression(((BinaryExpression)expression).Right) + ")";
                case ExpressionType.OrElse: return "(" + ParseExpression(((BinaryExpression)expression).Left) + " OR " + ParseExpression(((BinaryExpression)expression).Right) + ")";
                case ExpressionType.Not: return "NOT (" + ParseExpression(((UnaryExpression)expression).Operand) + ")";
                case ExpressionType.Call: return HandleMethodCall((MethodCallExpression)expression);
                case ExpressionType.MemberAccess: return ParseMember((MemberExpression)expression);
                case ExpressionType.Constant: return HandleConstant((ConstantExpression)expression);
                case ExpressionType.Convert: return ParseExpression(((UnaryExpression)expression).Operand);
                default:
                    throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported.");
            }
        }

        private string ParseExpressionWithBrackets(Expression expression)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal: return HandleEqualWithBrackets((BinaryExpression)expression);
                case ExpressionType.NotEqual: return HandleNotEqualWithBrackets((BinaryExpression)expression);
                case ExpressionType.GreaterThan: return HandleBinaryWithBrackets((BinaryExpression)expression, ">");
                case ExpressionType.LessThan: return HandleBinaryWithBrackets((BinaryExpression)expression, "<");
                case ExpressionType.GreaterThanOrEqual: return HandleBinaryWithBrackets((BinaryExpression)expression, ">=");
                case ExpressionType.LessThanOrEqual: return HandleBinaryWithBrackets((BinaryExpression)expression, "<=");
                case ExpressionType.AndAlso: return "(" + ParseExpressionWithBrackets(((BinaryExpression)expression).Left) + " AND " + ParseExpressionWithBrackets(((BinaryExpression)expression).Right) + ")";
                case ExpressionType.OrElse: return "(" + ParseExpressionWithBrackets(((BinaryExpression)expression).Left) + " OR " + ParseExpressionWithBrackets(((BinaryExpression)expression).Right) + ")";
                case ExpressionType.Not: return "NOT (" + ParseExpressionWithBrackets(((UnaryExpression)expression).Operand) + ")";
                case ExpressionType.Call: return HandleMethodCallWithBrackets((MethodCallExpression)expression);
                case ExpressionType.MemberAccess: return ParseMemberWithBrackets((MemberExpression)expression);
                case ExpressionType.Constant: return HandleConstant((ConstantExpression)expression);
                case ExpressionType.Convert: return ParseExpressionWithBrackets(((UnaryExpression)expression).Operand);
                default:
                    throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported.");
            }
        }

        private string ParseExpressionWithParameterMapping(Expression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Parameter:
                    var paramExpr = (ParameterExpression)expression;
                    if (parameterAliases.TryGetValue(paramExpr, out var alias))
                        return alias;
                    return GetAliasForType(paramExpr.Type);

                case ExpressionType.Equal: return HandleEqual((BinaryExpression)expression, parameterAliases);
                case ExpressionType.NotEqual: return HandleNotEqual((BinaryExpression)expression, parameterAliases);
                case ExpressionType.GreaterThan: return HandleBinary((BinaryExpression)expression, ">", parameterAliases);
                case ExpressionType.LessThan: return HandleBinary((BinaryExpression)expression, "<", parameterAliases);
                case ExpressionType.GreaterThanOrEqual: return HandleBinary((BinaryExpression)expression, ">=", parameterAliases);
                case ExpressionType.LessThanOrEqual: return HandleBinary((BinaryExpression)expression, "<=", parameterAliases);
                case ExpressionType.AndAlso: return "(" + ParseExpressionWithParameterMapping(((BinaryExpression)expression).Left, parameterAliases) + " AND " + ParseExpressionWithParameterMapping(((BinaryExpression)expression).Right, parameterAliases) + ")";
                case ExpressionType.OrElse: return "(" + ParseExpressionWithParameterMapping(((BinaryExpression)expression).Left, parameterAliases) + " OR " + ParseExpressionWithParameterMapping(((BinaryExpression)expression).Right, parameterAliases) + ")";
                case ExpressionType.Not: return "NOT (" + ParseExpressionWithParameterMapping(((UnaryExpression)expression).Operand, parameterAliases) + ")";
                case ExpressionType.Call: return HandleMethodCall((MethodCallExpression)expression, parameterAliases);
                case ExpressionType.MemberAccess: return ParseMember((MemberExpression)expression, parameterAliases);
                case ExpressionType.Constant: return HandleConstant((ConstantExpression)expression);
                case ExpressionType.Convert: return ParseExpressionWithParameterMapping(((UnaryExpression)expression).Operand, parameterAliases);
                default:
                    throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported.");
            }
        }

        private string ParseExpressionWithParameterMappingAndBrackets(Expression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Parameter:
                    var paramExpr = (ParameterExpression)expression;
                    if (parameterAliases.TryGetValue(paramExpr, out var alias))
                        return alias;
                    return GetAliasForType(paramExpr.Type);

                case ExpressionType.Equal: return HandleEqualWithBrackets((BinaryExpression)expression, parameterAliases);
                case ExpressionType.NotEqual: return HandleNotEqualWithBrackets((BinaryExpression)expression, parameterAliases);
                case ExpressionType.GreaterThan: return HandleBinaryWithBrackets((BinaryExpression)expression, ">", parameterAliases);
                case ExpressionType.LessThan: return HandleBinaryWithBrackets((BinaryExpression)expression, "<", parameterAliases);
                case ExpressionType.GreaterThanOrEqual: return HandleBinaryWithBrackets((BinaryExpression)expression, ">=", parameterAliases);
                case ExpressionType.LessThanOrEqual: return HandleBinaryWithBrackets((BinaryExpression)expression, "<=", parameterAliases);
                case ExpressionType.AndAlso: return "(" + ParseExpressionWithParameterMappingAndBrackets(((BinaryExpression)expression).Left, parameterAliases) + " AND " + ParseExpressionWithParameterMappingAndBrackets(((BinaryExpression)expression).Right, parameterAliases) + ")";
                case ExpressionType.OrElse: return "(" + ParseExpressionWithParameterMappingAndBrackets(((BinaryExpression)expression).Left, parameterAliases) + " OR " + ParseExpressionWithParameterMappingAndBrackets(((BinaryExpression)expression).Right, parameterAliases) + ")";
                case ExpressionType.Not: return "NOT (" + ParseExpressionWithParameterMappingAndBrackets(((UnaryExpression)expression).Operand, parameterAliases) + ")";
                case ExpressionType.Call: return HandleMethodCallWithBrackets((MethodCallExpression)expression, parameterAliases);
                case ExpressionType.MemberAccess: return ParseMemberWithBrackets((MemberExpression)expression, parameterAliases);
                case ExpressionType.Constant: return HandleConstant((ConstantExpression)expression);
                case ExpressionType.Convert: return ParseExpressionWithParameterMappingAndBrackets(((UnaryExpression)expression).Operand, parameterAliases);
                default:
                    throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported.");
            }
        }

        private string HandleEqual(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
                return ParseMember(expression.Left) + " IS NULL";
            return HandleBinary(expression, "=");
        }

        private string HandleEqualWithBrackets(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
                return ParseMemberWithBrackets(expression.Left) + " IS NULL";
            return HandleBinaryWithBrackets(expression, "=");
        }

        private string HandleEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            if (IsNullConstant(expression.Right))
                return ParseMember(expression.Left, parameterAliases) + " IS NULL";
            return HandleBinary(expression, "=", parameterAliases);
        }

        private string HandleEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            if (IsNullConstant(expression.Right))
                return ParseMemberWithBrackets(expression.Left, parameterAliases) + " IS NULL";
            return HandleBinaryWithBrackets(expression, "=", parameterAliases);
        }

        private string HandleNotEqual(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
                return ParseMember(expression.Left) + " IS NOT NULL";
            return HandleBinary(expression, "<>");
        }

        private string HandleNotEqualWithBrackets(BinaryExpression expression)
        {
            if (IsNullConstant(expression.Right))
                return ParseMemberWithBrackets(expression.Left) + " IS NOT NULL";
            return HandleBinaryWithBrackets(expression, "<>");
        }

        private string HandleNotEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            if (IsNullConstant(expression.Right))
                return ParseMember(expression.Left, parameterAliases) + " IS NOT NULL";
            return HandleBinary(expression, "<>", parameterAliases);
        }

        private string HandleNotEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            if (IsNullConstant(expression.Right))
                return ParseMemberWithBrackets(expression.Left, parameterAliases) + " IS NOT NULL";
            return HandleBinaryWithBrackets(expression, "<>", parameterAliases);
        }

        private string HandleBinary(BinaryExpression expression, string op)
        {
            var left = ParseMember(expression.Left);
            var right = ParseValue(expression.Right);
            return left + " " + op + " " + right;
        }

        private string HandleBinaryWithBrackets(BinaryExpression expression, string op)
        {
            var left = ParseMemberWithBrackets(expression.Left);
            var right = ParseValue(expression.Right);
            return left + " " + op + " " + right;
        }

        private string HandleBinary(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases)
        {
            var left = ParseMember(expression.Left, parameterAliases);
            var right = ParseValue(expression.Right, parameterAliases: parameterAliases);
            return left + " " + op + " " + right;
        }

        private string HandleBinaryWithBrackets(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases)
        {
            var left = ParseMemberWithBrackets(expression.Left, parameterAliases);
            var right = ParseValue(expression.Right, parameterAliases: parameterAliases);
            return left + " " + op + " " + right;
        }

        private string HandleConstant(ConstantExpression expression)
        {
            var p = GetUniqueParameterName();
            _parameters.TryAdd(p, expression.Value);
            return p;
        }

        private string HandleMethodCall(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(string))
            {
                if (m.Method.Name == "StartsWith") return HandleLike(m, "{0}%");
                if (m.Method.Name == "EndsWith") return HandleLike(m, "%{0}");
                if (m.Method.Name == "Contains") return HandleLike(m, "%{0}%");
            }

            if (m.Method.Name == "Contains" && m.Arguments.Count == 1)
            {
                var memberExpr = m.Arguments[0];
                var listObject = Evaluate(m.Object) as System.Collections.IEnumerable;
                if (listObject == null)
                    throw new NotSupportedException("Unsupported Contains signature or non-constant collection.");

                var memberSql = ParseMember(memberExpr);
                var items = new List<string>();

                foreach (var item in listObject)
                {
                    var p = GetUniqueParameterName();
                    _parameters.TryAdd(p, item);
                    items.Add(p);
                }
                return memberSql + " IN (" + string.Join(",", items) + ")";
            }

            if (m.Method.Name == "Between" && m.Arguments.Count == 3)
            {
                var property = ParseMember(m.Arguments[0]);
                var lower = ParseValue(m.Arguments[1]);
                var upper = ParseValue(m.Arguments[2]);
                return property + " BETWEEN " + lower + " AND " + upper;
            }

            if (m.Method.Name == "IsNullOrEmpty" && m.Arguments.Count == 1 && m.Method.DeclaringType == typeof(string))
            {
                var prop = ParseMember(m.Arguments[0]);
                return "(" + prop + " IS NULL OR " + prop + " = '')";
            }

            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported.");
        }

        private string HandleMethodCallWithBrackets(MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(string))
            {
                if (m.Method.Name == "StartsWith") return HandleLikeWithBrackets(m, "{0}%");
                if (m.Method.Name == "EndsWith") return HandleLikeWithBrackets(m, "%{0}");
                if (m.Method.Name == "Contains") return HandleLikeWithBrackets(m, "%{0}%");
            }

            if (m.Method.Name == "Contains" && m.Arguments.Count == 1)
            {
                var memberExpr = m.Arguments[0];
                var listObject = Evaluate(m.Object) as System.Collections.IEnumerable;
                if (listObject == null)
                    throw new NotSupportedException("Unsupported Contains signature or non-constant collection.");

                var memberSql = ParseMemberWithBrackets(memberExpr);
                var items = new List<string>();

                foreach (var item in listObject)
                {
                    var p = GetUniqueParameterName();
                    _parameters.TryAdd(p, item);
                    items.Add(p);
                }
                return memberSql + " IN (" + string.Join(",", items) + ")";
            }

            if (m.Method.Name == "Between" && m.Arguments.Count == 3)
            {
                var property = ParseMemberWithBrackets(m.Arguments[0]);
                var lower = ParseValue(m.Arguments[1]);
                var upper = ParseValue(m.Arguments[2]);
                return property + " BETWEEN " + lower + " AND " + upper;
            }

            if (m.Method.Name == "IsNullOrEmpty" && m.Arguments.Count == 1 && m.Method.DeclaringType == typeof(string))
            {
                var prop = ParseMemberWithBrackets(m.Arguments[0]);
                return "(" + prop + " IS NULL OR " + prop + " = '')";
            }

            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported.");
        }

        private string HandleMethodCall(MethodCallExpression m, Dictionary<ParameterExpression, string> parameterAliases)
        {
            if (m.Method.DeclaringType == typeof(string))
            {
                if (m.Method.Name == "StartsWith") return HandleLike(m, "{0}%", parameterAliases);
                if (m.Method.Name == "EndsWith") return HandleLike(m, "%{0}", parameterAliases);
                if (m.Method.Name == "Contains") return HandleLike(m, "%{0}%", parameterAliases);
            }

            if (m.Method.Name == "Contains" && m.Arguments.Count == 1)
            {
                var memberExpr = m.Arguments[0];
                var listObject = Evaluate(m.Object) as System.Collections.IEnumerable;
                if (listObject == null)
                    throw new NotSupportedException("Unsupported Contains signature or non-constant collection.");

                var memberSql = ParseMember(memberExpr, parameterAliases);
                var items = new List<string>();

                foreach (var item in listObject)
                {
                    var p = GetUniqueParameterName();
                    _parameters.TryAdd(p, item);
                    items.Add(p);
                }
                return memberSql + " IN (" + string.Join(",", items) + ")";
            }

            if (m.Method.Name == "Between" && m.Arguments.Count == 3)
            {
                var property = ParseMember(m.Arguments[0], parameterAliases);
                var lower = ParseValue(m.Arguments[1], parameterAliases: parameterAliases);
                var upper = ParseValue(m.Arguments[2], parameterAliases: parameterAliases);
                return property + " BETWEEN " + lower + " AND " + upper;
            }

            if (m.Method.Name == "IsNullOrEmpty" && m.Arguments.Count == 1 && m.Method.DeclaringType == typeof(string))
            {
                var prop = ParseMember(m.Arguments[0], parameterAliases);
                return "(" + prop + " IS NULL OR " + prop + " = '')";
            }

            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported.");
        }

        private string HandleMethodCallWithBrackets(MethodCallExpression m, Dictionary<ParameterExpression, string> parameterAliases)
        {
            if (m.Method.DeclaringType == typeof(string))
            {
                if (m.Method.Name == "StartsWith") return HandleLikeWithBrackets(m, "{0}%", parameterAliases);
                if (m.Method.Name == "EndsWith") return HandleLikeWithBrackets(m, "%{0}", parameterAliases);
                if (m.Method.Name == "Contains") return HandleLikeWithBrackets(m, "%{0}%", parameterAliases);
            }

            if (m.Method.Name == "Contains" && m.Arguments.Count == 1)
            {
                var memberExpr = m.Arguments[0];
                var listObject = Evaluate(m.Object) as System.Collections.IEnumerable;
                if (listObject == null)
                    throw new NotSupportedException("Unsupported Contains signature or non-constant collection.");

                var memberSql = ParseMemberWithBrackets(memberExpr, parameterAliases);
                var items = new List<string>();

                foreach (var item in listObject)
                {
                    var p = GetUniqueParameterName();
                    _parameters.TryAdd(p, item);
                    items.Add(p);
                }
                return memberSql + " IN (" + string.Join(",", items) + ")";
            }

            if (m.Method.Name == "Between" && m.Arguments.Count == 3)
            {
                var property = ParseMemberWithBrackets(m.Arguments[0], parameterAliases);
                var lower = ParseValue(m.Arguments[1], parameterAliases: parameterAliases);
                var upper = ParseValue(m.Arguments[2], parameterAliases: parameterAliases);
                return property + " BETWEEN " + lower + " AND " + upper;
            }

            if (m.Method.Name == "IsNullOrEmpty" && m.Arguments.Count == 1 && m.Method.DeclaringType == typeof(string))
            {
                var prop = ParseMemberWithBrackets(m.Arguments[0], parameterAliases);
                return "(" + prop + " IS NULL OR " + prop + " = '')";
            }

            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported.");
        }

        private string HandleLike(MethodCallExpression m, string format)
        {
            Expression propExpr;
            Expression valueExpr;

            if (m.Object != null)
            {
                propExpr = m.Object;
                valueExpr = m.Arguments[0];
            }
            else
            {
                throw new NotSupportedException("LIKE requires instance method call on string.");
            }

            var prop = ParseMember(propExpr);
            var val = ParseValue(valueExpr, format);
            return prop + " LIKE " + val;
        }

        private string HandleLikeWithBrackets(MethodCallExpression m, string format)
        {
            Expression propExpr;
            Expression valueExpr;

            if (m.Object != null)
            {
                propExpr = m.Object;
                valueExpr = m.Arguments[0];
            }
            else
            {
                throw new NotSupportedException("LIKE requires instance method call on string.");
            }

            var prop = ParseMemberWithBrackets(propExpr);
            var val = ParseValue(valueExpr, format);
            return prop + " LIKE " + val;
        }

        private string HandleLike(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases)
        {
            Expression propExpr;
            Expression valueExpr;

            if (m.Object != null)
            {
                propExpr = m.Object;
                valueExpr = m.Arguments[0];
            }
            else
            {
                throw new NotSupportedException("LIKE requires instance method call on string.");
            }

            var prop = ParseMember(propExpr, parameterAliases);
            var val = ParseValue(valueExpr, format, parameterAliases);
            return prop + " LIKE " + val;
        }

        private string HandleLikeWithBrackets(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases)
        {
            Expression propExpr;
            Expression valueExpr;

            if (m.Object != null)
            {
                propExpr = m.Object;
                valueExpr = m.Arguments[0];
            }
            else
            {
                throw new NotSupportedException("LIKE requires instance method call on string.");
            }

            var prop = ParseMemberWithBrackets(propExpr, parameterAliases);
            var val = ParseValue(valueExpr, format, parameterAliases);
            return prop + " LIKE " + val;
        }

        private string ParseSelectMember(Expression expression)
        {
            var unary = expression as UnaryExpression;
            if (unary != null)
                return ParseSelectMember(unary.Operand);

            var member = expression as MemberExpression;
            if (member != null)
            {
                var columnName = ParseMemberWithBrackets(member);
                var propertyName = member.Member.Name;
                return $"{columnName} AS {propertyName}";
            }

            // For other expression types, just parse without alias
            return ParseMemberWithBrackets(expression);
        }

        private string ParseMember(Expression expression)
        {
            var unary = expression as UnaryExpression;
            if (unary != null)
                return ParseMember(unary.Operand);

            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Expression != null)
                {
                    var alias = ParseMember(member.Expression);
                    return alias + "." + GetColumnName(member.Member as PropertyInfo);
                }
                return GetColumnName(member.Member as PropertyInfo);
            }

            var parameter = expression as ParameterExpression;
            if (parameter != null)
            {
                return GetAliasForType(parameter.Type);
            }

            throw new NotSupportedException("Unsupported expression: " + expression);
        }

        private string ParseMemberWithBrackets(Expression expression)
        {
            var member = ParseMember(expression);
            if (member.Contains("."))
            {
                var parts = member.Split('.');
                return parts[0] + ".[" + parts[1] + "]";
            }
            return "[" + member + "]";
        }

        private string ParseMember(Expression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            var unary = expression as UnaryExpression;
            if (unary != null)
                return ParseMember(unary.Operand, parameterAliases);

            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Expression != null)
                {
                    var alias = ParseMember(member.Expression, parameterAliases);
                    return alias + "." + GetColumnName(member.Member as PropertyInfo);
                }
                return GetColumnName(member.Member as PropertyInfo);
            }

            var parameter = expression as ParameterExpression;
            if (parameter != null)
            {
                if (parameterAliases.TryGetValue(parameter, out var alias))
                    return alias;
                return GetAliasForType(parameter.Type);
            }

            throw new NotSupportedException("Unsupported expression: " + expression);
        }

        private string ParseMemberWithBrackets(Expression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            var member = ParseMember(expression, parameterAliases);
            if (member.Contains("."))
            {
                var parts = member.Split('.');
                return parts[0] + ".[" + parts[1] + "]";
            }
            return "[" + member + "]";
        }

        private string ParseValue(Expression expression, string format = null, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            var constant = expression as ConstantExpression;
            if (constant != null)
            {
                var p = GetUniqueParameterName();
                var v = constant.Value;
                if (format != null && v is string)
                    v = string.Format(format, v);
                _parameters.TryAdd(p, v);
                return p;
            }

            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Expression != null && member.Expression.NodeType == ExpressionType.Constant)
                {
                    var value = GetValue(member);
                    var p = GetUniqueParameterName();
                    _parameters.TryAdd(p, value);
                    return p;
                }
                return ParseMember(member, parameterAliases);
            }

            var unary = expression as UnaryExpression;
            if (unary != null)
                return ParseValue(unary.Operand, format, parameterAliases);

            var binary = expression as BinaryExpression;
            if (binary != null)
            {
                var left = ParseValue(binary.Left, format, parameterAliases);
                var right = ParseValue(binary.Right, format, parameterAliases);
                return left + " " + GetOperator(binary.NodeType) + " " + right;
            }

            var eval = Evaluate(expression);
            var p2 = GetUniqueParameterName();
            _parameters.TryAdd(p2, eval);
            return p2;
        }

        private static object Evaluate(Expression expr)
        {
            if (expr is ConstantExpression ce) return ce.Value;
            try
            {
                var lambda = Expression.Lambda(expr);
                var compiled = lambda.Compile();
                return compiled.DynamicInvoke();
            }
            catch
            {
                return null;
            }
        }

        private static object GetValue(MemberExpression member)
        {
            var obj = ((ConstantExpression)member.Expression).Value;
            var fi = member.Member as FieldInfo;
            if (fi != null) return fi.GetValue(obj);
            var pi = member.Member as PropertyInfo;
            if (pi != null) return pi.GetValue(obj, null);
            return null;
        }

        private static string GetOperator(ExpressionType nodeType)
        {
            switch (nodeType)
            {
                case ExpressionType.Equal: return "=";
                case ExpressionType.NotEqual: return "<>";
                case ExpressionType.GreaterThan: return ">";
                case ExpressionType.LessThan: return "<";
                case ExpressionType.GreaterThanOrEqual: return ">=";
                case ExpressionType.LessThanOrEqual: return "<=";
                case ExpressionType.AndAlso: return "AND";
                case ExpressionType.OrElse: return "OR";
                default: throw new NotSupportedException("Unsupported operator: " + nodeType);
            }
        }

        private bool IsNullConstant(Expression expression)
        {
            var c = expression as ConstantExpression;
            return c != null && c.Value == null;
        }
        #endregion

        #region Metadata/Alias/Connection
        private string GetTableName(Type type)
        {
            return TableNameCache.GetOrAdd(type, t =>
            {
                var tableAttr = t.GetCustomAttribute<TableAttribute>();
                return tableAttr == null
                    ? $"[{_defaultSchema}].[{t.Name}]"
                    : $"[{(tableAttr.Schema ?? _defaultSchema)}].[{tableAttr.TableName}]";
            });
        }

        private string GetColumnName(PropertyInfo property)
        {
            return ColumnNameCache.GetOrAdd(property, p =>
            {
                var column = p.GetCustomAttribute<ColumnAttribute>();
                var name = column != null ? column.ColumnName : p?.Name ?? string.Empty;
                return name; // حذف براکت‌ها
            });
        }

        private string GenerateAlias(string tableName)
        {
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 10) shortName = shortName.Substring(0, 10);
            var counter = Interlocked.Increment(ref _aliasCounter);
            return $"{shortName}_A{counter}";
        }

        private string GetAliasForTable(string tableName)
        {
            return _tableAliasMappings.GetOrAdd(tableName, _ => GenerateAlias(tableName));
        }

        private string GetAliasForType(Type type)
        {
            return _typeAliasMappings.GetOrAdd(type, _ =>
            {
                var table = GetTableName(type);
                return GetAliasForTable(table);
            });
        }

        private string GetUniqueParameterName()
        {
            var id = Interlocked.Increment(ref _paramCounter);
            return "@p" + id.ToString();
        }

        private IDbConnection GetOpenConnection()
        {
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open)
                connection.Open();
            return connection;
        }
        #endregion

        #region IDisposable
        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
        }
        #endregion

        #region Nested Types
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
        #endregion
    }
}
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;

namespace EasyDapper
{
    internal sealed class QueryBuilderCore<T> : IDisposable
    {
        private readonly Lazy<IDbConnection> _lazyConnection;
        private readonly AliasManager _aliasManager;
        private readonly ParameterBuilder _parameterBuilder;
        private readonly ExpressionParser _expressionParser;
        private readonly ConcurrentQueue<string> _filters = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<JoinInfo> _joins = new ConcurrentQueue<JoinInfo>();
        private readonly ConcurrentQueue<ApplyInfo> _applies = new ConcurrentQueue<ApplyInfo>();
        private readonly ConcurrentQueue<string> _aggregateColumns = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _groupByColumns = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _unionClauses = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _intersectClauses = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _exceptClauses = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _selectedColumnsQueue = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<string> _orderByQueue = new ConcurrentQueue<string>();
        private string _rowNumberClause = string.Empty;
        private string _havingClause = string.Empty;
        private int _timeOut;
        private string _distinctClause = string.Empty;
        private string _topClause = string.Empty;
        private bool _isCountQuery = false;
        private int? _limit = null;
        private int? _offset = null;
        private readonly object _pagingLock = new object();
        private bool _disposed = false;

        private readonly ConnectionManager _connectionManager;

        private static readonly char[] InvalidAliasChars =
            new[] { ']', '[', ';', '-', '/', '*', '\'', '"', '(', ')', '&', '|', '^', '%', '~',
                    '`', '$', '{', '}', '<', '>', '?', '!', '=', '+', ',', ':', '\\', ' ', '\t', '\n', '\r' };

        private static void ValidateCustomAlias(string alias)
        {
            if (!string.IsNullOrEmpty(alias) && alias.IndexOfAny(InvalidAliasChars) >= 0)
            {
                throw new ArgumentException("Custom alias contains invalid characters.", "alias");
            }
        }

        private static string QuoteIdentifier(string identifier)
        {
            if (string.IsNullOrEmpty(identifier)) return identifier;
            return "[" + identifier + "]";
        }

        public QueryBuilderCore(IDbConnection connection, AliasManager aliasManager,
            ParameterBuilder parameterBuilder, ExpressionParser expressionParser,
            bool ownsConnection = false, ConnectionManager connectionManager = null,
            int? commandTimeout = null)
            : this(aliasManager, parameterBuilder, expressionParser,
                  connectionManager,
                  commandTimeout ?? (connection != null ? connection.ConnectionTimeout : 30),
                  connection)
        {
        }

        public QueryBuilderCore(ConnectionManager connectionManager, AliasManager aliasManager,
            ParameterBuilder parameterBuilder, ExpressionParser expressionParser)
            : this(aliasManager, parameterBuilder, expressionParser,
                  connectionManager, connectionManager.CommandTimeout, null)
        {
        }

        private QueryBuilderCore(AliasManager aliasManager, ParameterBuilder parameterBuilder,
            ExpressionParser expressionParser, ConnectionManager connectionManager,
            int commandTimeout, IDbConnection directConnection)
        {
            if (aliasManager == null) throw new ArgumentNullException("aliasManager");
            if (parameterBuilder == null) throw new ArgumentNullException("parameterBuilder");
            if (expressionParser == null) throw new ArgumentNullException("expressionParser");
            _aliasManager = aliasManager;
            _parameterBuilder = parameterBuilder;
            _expressionParser = expressionParser;
            _connectionManager = connectionManager;
            _timeOut = commandTimeout;
            if (connectionManager != null)
            {
                _lazyConnection = new Lazy<IDbConnection>(() => connectionManager.GetOpenConnection(), LazyThreadSafetyMode.ExecutionAndPublication);
            }
            else
            {
                if (directConnection == null) throw new ArgumentNullException("connection");
                _lazyConnection = new Lazy<IDbConnection>(() => directConnection, LazyThreadSafetyMode.ExecutionAndPublication);
            }
            var mainTableName = QueryBuilderCache.GetTableName(typeof(T));
            var mainAlias = _aliasManager.GenerateAlias(mainTableName);
            _aliasManager.SetTableAlias(mainTableName, mainAlias);
            _aliasManager.SetTypeAlias(typeof(T), mainAlias);
        }

        public void Where(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException("filter");
            _filters.Enqueue(_expressionParser.ParseExpressionWithBrackets(filter.Body));
        }

        public void Select(params Expression<Func<T, object>>[] columns)
        {
            if (columns == null) return;
            foreach (var c in columns) if (c != null) _selectedColumnsQueue.Enqueue(_expressionParser.ParseSelectMember(c.Body));
        }

        public void Select<TSource>(params Expression<Func<TSource, object>>[] columns)
        {
            if (columns == null) return;
            foreach (var c in columns) if (c != null) _selectedColumnsQueue.Enqueue(_expressionParser.ParseSelectMember(c.Body));
        }

        public void Distinct() => SetClause(ref _distinctClause, "DISTINCT");

        public void Top(int count)
        {
            if (count <= 0) throw new ArgumentOutOfRangeException("count");
            SetClause(ref _topClause, "TOP (" + count + ")");
        }

        public void Count() => SetFlag(ref _isCountQuery, true);

        public void OrderBy(string orderByClause)
        {
            if (string.IsNullOrEmpty(orderByClause)) return;
            _orderByQueue.Enqueue(orderByClause);
        }

        public void OrderByAscending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            foreach (var col in _expressionParser.ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " ASC"))
                _orderByQueue.Enqueue(col);
        }

        public void OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            foreach (var col in _expressionParser.ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " DESC"))
                _orderByQueue.Enqueue(col);
        }

        public void Paging(int pageSize, int pageNumber = 1)
        {
            if (pageSize <= 0) throw new ArgumentException("Page size must be greater than zero");
            if (pageNumber <= 0) throw new ArgumentException("Page number must be greater than zero");
            lock (_pagingLock)
            {
                _limit = pageSize;
                _offset = (pageNumber - 1) * pageSize;
            }
        }

        public void Sum(Expression<Func<T, object>> column, string alias = null) => AddAggregate("SUM", column.Body, alias);
        public void Avg(Expression<Func<T, object>> column, string alias = null) => AddAggregate("AVG", column.Body, alias);
        public void Min(Expression<Func<T, object>> column, string alias = null) => AddAggregate("MIN", column.Body, alias);
        public void Max(Expression<Func<T, object>> column, string alias = null) => AddAggregate("MAX", column.Body, alias);
        public void Count(Expression<Func<T, object>> column, string alias = null) => AddAggregate("COUNT", column.Body, alias);

        private void AddAggregate(string fn, Expression expr, string alias)
        {
            if (expr == null) throw new ArgumentNullException("expr");
            ValidateCustomAlias(alias);
            var parsed = _expressionParser.ParseMemberWithBrackets(expr);
            var agg = string.IsNullOrEmpty(alias)
                ? fn + "(" + parsed + ")"
                : fn + "(" + parsed + ") AS " + QuoteIdentifier(alias);
            _aggregateColumns.Enqueue(agg);
        }

        public void GroupBy(params Expression<Func<T, object>>[] groupByColumns)
        {
            if (groupByColumns == null) return;
            foreach (var column in groupByColumns)
                if (column != null) _groupByColumns.Enqueue(_expressionParser.ParseMemberWithBrackets(column.Body));
        }

        public void Having(Expression<Func<T, bool>> havingCondition)
        {
            if (havingCondition == null) throw new ArgumentNullException("havingCondition");
            _havingClause = _expressionParser.ParseExpressionWithBrackets(havingCondition.Body);
        }

        public void Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber")
        {
            if (partitionBy == null) throw new ArgumentNullException("partitionBy");
            if (orderBy == null) throw new ArgumentNullException("orderBy");
            ValidateCustomAlias(alias);
            var parts = _expressionParser.ExtractColumnListWithBrackets(partitionBy.Body);
            var orders = _expressionParser.ExtractColumnListWithBrackets(orderBy.Body);
            _rowNumberClause = "ROW_NUMBER() OVER (PARTITION BY " + string.Join(", ", parts)
                + " ORDER BY " + string.Join(", ", orders) + ") AS " + QuoteIdentifier(alias);
        }

        public void InnerJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) => AddJoin("INNER JOIN", onCondition);
        public void LeftJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) => AddJoin("LEFT JOIN", onCondition);
        public void RightJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) => AddJoin("RIGHT JOIN", onCondition);
        public void FullJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) => AddJoin("FULL JOIN", onCondition);
        public void CustomJoin<TLeft, TRight>(string join, Expression<Func<TLeft, TRight, bool>> onCondition) => AddJoin(join, onCondition);

        private void AddJoin<TLeft, TRight>(string joinType, Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            if (string.IsNullOrWhiteSpace(joinType)) throw new ArgumentNullException("joinType");
            if (onCondition == null) throw new ArgumentNullException("onCondition");
            var leftTableName = QueryBuilderCache.GetTableName(typeof(TLeft));
            var rightTableName = QueryBuilderCache.GetTableName(typeof(TRight));
            var leftAlias = _aliasManager.GetAliasForType(typeof(TLeft));
            string rightAlias;
            bool isSelfJoin = typeof(TLeft) == typeof(TRight);
            int joinCount = _joins.Count(j => j.TableName == rightTableName);
            bool isRepeatedJoin = joinCount > 0;
            if (isSelfJoin || isRepeatedJoin) rightAlias = _aliasManager.GetUniqueAliasForType(typeof(TRight));
            else rightAlias = _aliasManager.GetAliasForType(typeof(TRight));
            var parameterAliases = new Dictionary<ParameterExpression, string>
            {
                { onCondition.Parameters[0], leftAlias },
                { onCondition.Parameters[1], rightAlias }
            };
            var parsed = _expressionParser.ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);
            _joins.Enqueue(new JoinInfo { JoinType = joinType, TableName = rightTableName, Alias = rightAlias, OnCondition = parsed });
        }

        public void CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
            => AddApply("CROSS APPLY", onCondition, subBuilder);
        public void OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
            => AddApply("OUTER APPLY", onCondition, subBuilder);

        private void AddApply<TSubQuery>(string applyType, Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
        {
            if (string.IsNullOrWhiteSpace(applyType)) throw new ArgumentNullException("applyType");
            if (onCondition == null) throw new ArgumentNullException("onCondition");
            if (subBuilder == null) throw new ArgumentNullException("subBuilder");
            QueryBuilder<TSubQuery> sub;
            if (_connectionManager != null)
            {
                sub = new QueryBuilder<TSubQuery>(_connectionManager);
            }
            else
            {
                sub = new QueryBuilder<TSubQuery>(_lazyConnection.Value);
            }
            var qb = subBuilder(sub);
            if (qb == null) throw new InvalidOperationException("Sub-query builder returned null");
            var subQb = (QueryBuilder<TSubQuery>)qb;
            var subAliasManager = subQb.GetAliasManager();
            var subParameterBuilder = subQb.GetParameterBuilder();
            string subSql = subQb.BuildQuery();
            string actualSubAlias = ExtractSubQueryTableAlias(subSql);
            if (string.IsNullOrEmpty(actualSubAlias))
            {
                var subTableName = QueryBuilderCache.GetTableName(typeof(TSubQuery));
                actualSubAlias = subAliasManager.GetAliasForTable(subTableName);
            }

            var parameterAliases = new Dictionary<ParameterExpression, string>
            {
                { onCondition.Parameters[0], _aliasManager.GetAliasForType(typeof(T)) },
                { onCondition.Parameters[1], actualSubAlias }
            };

            var onSql = _expressionParser.ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);

            if (subSql.Contains("WHERE "))
            {
                int whereIndex = subSql.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
                int insertIndex = whereIndex + 6;
                subSql = subSql.Insert(insertIndex, "(" + onSql + ") AND ");
            }
            else
            {
                int insertIndex = FindWhereInsertPosition(subSql);
                subSql = subSql.Insert(insertIndex, " WHERE " + onSql);
            }

            var renamings = _parameterBuilder.MergeParameters(subParameterBuilder.GetParameters());
            foreach (var renaming in renamings) subSql = SafeReplaceParameter(subSql, renaming.Key, renaming.Value);

            var applyAlias = _aliasManager.GenerateSubQueryAlias(QueryBuilderCache.GetTableName(typeof(TSubQuery)));
            _aliasManager.SetSubQueryAlias(typeof(TSubQuery), applyAlias);
            _applies.Enqueue(new ApplyInfo { ApplyType = applyType, SubQuery = "(" + subSql + ") AS " + QuoteIdentifier(applyAlias), SubQueryAlias = applyAlias });
        }

        private string ExtractSubQueryTableAlias(string sql)
        {
            var match = Regex.Match(sql, @"FROM\s+(?:\[[^\]]+\]\.)?\[[^\]]+\]\s+AS\s+\[([A-Za-z0-9_]+)\]", RegexOptions.IgnoreCase | RegexOptions.Multiline);
            return match.Success ? match.Groups[1].Value : null;
        }

        public void Union(IQueryBuilder<T> other) => AddSet("UNION", other);
        public void UnionAll(IQueryBuilder<T> other) => AddSet("UNION ALL", other);
        public void Intersect(IQueryBuilder<T> other) => AddSet("INTERSECT", other);
        public void Except(IQueryBuilder<T> other) => AddSet("EXCEPT", other);

        private void AddSet(string op, IQueryBuilder<T> other)
        {
            if (string.IsNullOrWhiteSpace(op)) throw new ArgumentNullException("op");
            if (other == null) throw new ArgumentNullException("other");
            var otherQb = (QueryBuilder<T>)other;
            string sql = otherQb.BuildQuery();
            var sourceParameters = otherQb.GetParameterBuilder().GetParameters();
            var renamings = _parameterBuilder.MergeParameters(sourceParameters);
            foreach (var renaming in renamings) sql = SafeReplaceParameter(sql, renaming.Key, renaming.Value);

            var clause = op + " (" + sql + ")";
            if (op.StartsWith("UNION")) _unionClauses.Enqueue(clause);
            else if (op == "INTERSECT") _intersectClauses.Enqueue(clause);
            else if (op == "EXCEPT") _exceptClauses.Enqueue(clause);
        }

        public IEnumerable<T> Execute()
            => GetOpenConnection().Query<T>(BuildQuery(), _parameterBuilder.GetParameters(),
                GetTransaction(), commandTimeout: _timeOut);

        public IEnumerable<TResult> Execute<TResult>()
            => GetOpenConnection().Query<TResult>(BuildQuery(), _parameterBuilder.GetParameters(),
                GetTransaction(), commandTimeout: _timeOut);

        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = BuildQuery();
            return await GetOpenConnection().QueryAsync<T>(query, _parameterBuilder.GetParameters(),
                GetTransaction(), commandTimeout: _timeOut);
        }

        public async Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            var query = BuildQuery();
            return await GetOpenConnection().QueryAsync<TResult>(query, _parameterBuilder.GetParameters(),
                GetTransaction(), commandTimeout: _timeOut);
        }

        public string GetRawSql() => BuildQuery();

        internal string BuildQuery()
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
            if (string.IsNullOrEmpty(orderByClause) && !string.IsNullOrEmpty(paginationClause))
                orderByClause = BuildOrderByClause();
            var sb = new StringBuilder();
            sb.Append(selectClause).Append(fromClause).Append(joinClauses).Append(applyClauses);
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
            if (_isCountQuery) columns = "COUNT(*) AS TotalCount";
            else if (_selectedColumnsQueue.Any())
            {
                columns = string.Join(", ", _selectedColumnsQueue.ToArray());
                if (_aggregateColumns.Any()) columns = string.Join(", ", _aggregateColumns.ToArray()) + ", " + columns;
            }
            else if (_aggregateColumns.Any()) columns = string.Join(", ", _aggregateColumns.ToArray());
            else columns = string.Join(", ", typeof(T).GetProperties().Select(p =>
                _aliasManager.GetAliasForType(typeof(T)) + ".[" + QueryBuilderCache.GetColumnName(p) + "] AS " + QuoteIdentifier(p.Name)));
            if (!string.IsNullOrEmpty(_rowNumberClause)) columns = _rowNumberClause + ", " + columns;
            var result = new StringBuilder("SELECT");
            if (!string.IsNullOrEmpty(_distinctClause)) result.Append(' ').Append(_distinctClause);
            if (!string.IsNullOrEmpty(_topClause)) result.Append(' ').Append(_topClause);
            result.Append(' ').Append(columns);
            return result.ToString();
        }

        private string BuildFromClause()
        {
            var tableName = QueryBuilderCache.GetTableName(typeof(T));
            var alias = _aliasManager.GetAliasForTable(tableName);
            return " FROM " + tableName + " AS " + QuoteIdentifier(alias);
        }

        private string BuildJoinClauses()
        {
            var sb = new StringBuilder();
            foreach (var join in _joins)
                sb.Append(' ').Append(join.JoinType).Append(' ')
                  .Append(join.TableName + " AS " + QuoteIdentifier(join.Alias))
                  .Append(" ON ").Append(join.OnCondition);
            return sb.ToString();
        }

        private string BuildApplyClauses()
        {
            var sb = new StringBuilder();
            foreach (var apply in _applies)
                sb.Append(' ').Append(apply.ApplyType).Append(' ').Append(apply.SubQuery);
            return sb.ToString();
        }

        private string BuildWhereClause()
            => _filters.Any() ? "WHERE " + string.Join(" AND ", _filters.ToArray()) : string.Empty;

        private string BuildGroupByClause()
            => _groupByColumns.Any() ? "GROUP BY " + string.Join(", ", _groupByColumns.ToArray()) : string.Empty;

        private string BuildHavingClause()
            => !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : string.Empty;

        private string BuildOrderByClause()
            => _orderByQueue.Any() ? "ORDER BY " + string.Join(", ", _orderByQueue.ToArray()) : string.Empty;

        private string BuildPaginationClause(string orderByClause)
        {
            int? limit, offset;
            lock (_pagingLock) { limit = _limit; offset = _offset; }
            if (limit.HasValue)
            {
                if (string.IsNullOrEmpty(orderByClause))
                {
                    _orderByQueue.Enqueue("(SELECT 1)");
                    orderByClause = BuildOrderByClause();
                }
                return "OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
            }
            return string.Empty;
        }

        private int FindWhereInsertPosition(string sql)
        {
            const string FROM = "FROM ";
            const string GROUP_BY = " GROUP BY ";
            const string ORDER_BY = " ORDER BY ";
            const string HAVING = " HAVING ";

            int fromIndex = sql.IndexOf(FROM, StringComparison.OrdinalIgnoreCase);
            if (fromIndex == -1) return 0;

            int searchStart = fromIndex + FROM.Length;

            int idxGroupBy = sql.IndexOf(GROUP_BY, searchStart, StringComparison.OrdinalIgnoreCase);
            int idxOrderBy = sql.IndexOf(ORDER_BY, searchStart, StringComparison.OrdinalIgnoreCase);
            int idxHaving = sql.IndexOf(HAVING, searchStart, StringComparison.OrdinalIgnoreCase);

            int minTerminator = -1;
            if (idxGroupBy != -1) minTerminator = idxGroupBy;
            if (idxOrderBy != -1) minTerminator = (minTerminator == -1) ? idxOrderBy : Math.Min(minTerminator, idxOrderBy);
            if (idxHaving != -1) minTerminator = (minTerminator == -1) ? idxHaving : Math.Min(minTerminator, idxHaving);

            return minTerminator != -1 ? minTerminator : sql.Length;
        }

        private void SetClause(ref string clauseField, string value) { clauseField = value; }
        private void SetFlag(ref bool flagField, bool value) { flagField = value; }

        private string SafeReplaceParameter(string sql, string oldName, string newName)
        {
            return Regex.Replace(sql, @"\b" + Regex.Escape(oldName) + @"\b", newName, RegexOptions.IgnoreCase);
        }

        private IDbConnection GetOpenConnection()
        {
            if (_connectionManager != null) return _connectionManager.GetOpenConnection();
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open) connection.Open();
            return connection;
        }

        private IDbTransaction GetTransaction()
            => _connectionManager?.CurrentTransaction;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            _disposed = true;
        }

        private sealed class JoinInfo
        {
            public string JoinType { get; set; }
            public string TableName { get; set; }
            public string Alias { get; set; }
            public string OnCondition { get; set; }
        }

        private sealed class ApplyInfo
        {
            public string ApplyType { get; set; }
            public string SubQuery { get; set; }
            public string SubQueryAlias { get; set; }
        }

        internal AliasManager GetAliasManager() => _aliasManager;
        internal ParameterBuilder GetParameterBuilder() => _parameterBuilder;
    }
}

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
        private readonly List<string> _filters = new List<string>();
        private readonly List<JoinInfo> _joins = new List<JoinInfo>();
        private readonly List<ApplyInfo> _applies = new List<ApplyInfo>();
        private readonly List<string> _aggregateColumns = new List<string>();
        private readonly List<string> _groupByColumns = new List<string>();
        private readonly List<string> _unionClauses = new List<string>();
        private readonly List<string> _intersectClauses = new List<string>();
        private readonly List<string> _exceptClauses = new List<string>();
        private readonly List<string> _selectedColumnsQueue = new List<string>();
        private readonly List<string> _orderByQueue = new List<string>();
        private string _rowNumberClause = string.Empty;
        private string _havingClause = string.Empty;
        private int _timeOut;
        private string _distinctClause = string.Empty;
        private string _topClause = string.Empty;
        private bool _isCountQuery = false;
        private int? _limit = null;
        private int? _offset = null;
        private bool _disposed = false;

        private readonly ConnectionManager _connectionManager;
        private readonly object _stateLock = new object();
        private readonly object _executeLock = new object();

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
            var parsed = _expressionParser.ParseExpressionWithBrackets(filter.Body);
            lock (_stateLock) { _filters.Add(parsed); }
        }

        public void Select(params Expression<Func<T, object>>[] columns)
        {
            if (columns == null) return;
            var parsed = new List<string>();
            foreach (var c in columns) if (c != null) parsed.Add(_expressionParser.ParseSelectMember(c.Body));
            lock (_stateLock) { _selectedColumnsQueue.AddRange(parsed); }
        }

        public void Select<TSource>(params Expression<Func<TSource, object>>[] columns)
        {
            if (columns == null) return;
            var parsed = new List<string>();
            foreach (var c in columns) if (c != null) parsed.Add(_expressionParser.ParseSelectMember(c.Body));
            lock (_stateLock) { _selectedColumnsQueue.AddRange(parsed); }
        }

        public void Distinct()
        {
            lock (_stateLock) { _distinctClause = "DISTINCT"; }
        }

        public void Top(int count)
        {
            if (count <= 0) throw new ArgumentOutOfRangeException("count");
            lock (_stateLock) { _topClause = "TOP (" + count + ")"; }
        }

        public void Count()
        {
            lock (_stateLock) { _isCountQuery = true; }
        }

        public void OrderBy(string orderByClause)
        {
            if (string.IsNullOrEmpty(orderByClause)) return;
            lock (_stateLock) { _orderByQueue.Add(orderByClause); }
        }

        public void OrderByAscending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            var cols = _expressionParser.ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " ASC").ToList();
            lock (_stateLock) { _orderByQueue.AddRange(cols); }
        }

        public void OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            var cols = _expressionParser.ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " DESC").ToList();
            lock (_stateLock) { _orderByQueue.AddRange(cols); }
        }

        public void Paging(int pageSize, int pageNumber = 1)
        {
            if (pageSize <= 0) throw new ArgumentException("Page size must be greater than zero");
            if (pageNumber <= 0) throw new ArgumentException("Page number must be greater than zero");
            lock (_stateLock)
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
            lock (_stateLock) { _aggregateColumns.Add(agg); }
        }

        public void GroupBy(params Expression<Func<T, object>>[] groupByColumns)
        {
            if (groupByColumns == null) return;
            var parsed = new List<string>();
            foreach (var column in groupByColumns)
                if (column != null) parsed.Add(_expressionParser.ParseMemberWithBrackets(column.Body));
            lock (_stateLock) { _groupByColumns.AddRange(parsed); }
        }

        public void Having(Expression<Func<T, bool>> havingCondition)
        {
            if (havingCondition == null) throw new ArgumentNullException("havingCondition");
            var parsed = _expressionParser.ParseExpressionWithBrackets(havingCondition.Body);
            lock (_stateLock) { _havingClause = parsed; }
        }

        public void Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber")
        {
            if (partitionBy == null) throw new ArgumentNullException("partitionBy");
            if (orderBy == null) throw new ArgumentNullException("orderBy");
            ValidateCustomAlias(alias);
            var parts = _expressionParser.ExtractColumnListWithBrackets(partitionBy.Body);
            var orders = _expressionParser.ExtractColumnListWithBrackets(orderBy.Body);
            var clause = "ROW_NUMBER() OVER (PARTITION BY " + string.Join(", ", parts)
                + " ORDER BY " + string.Join(", ", orders) + ") AS " + QuoteIdentifier(alias);
            lock (_stateLock) { _rowNumberClause = clause; }
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
            int joinCount;
            lock (_stateLock) { joinCount = _joins.Count(j => j.TableName == rightTableName); }
            bool isRepeatedJoin = joinCount > 0;
            if (isSelfJoin || isRepeatedJoin) rightAlias = _aliasManager.GetUniqueAliasForType(typeof(TRight));
            else rightAlias = _aliasManager.GetAliasForType(typeof(TRight));
            var parameterAliases = new Dictionary<ParameterExpression, string>
            {
                { onCondition.Parameters[0], leftAlias },
                { onCondition.Parameters[1], rightAlias }
            };
            var parsed = _expressionParser.ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);
            var info = new JoinInfo { JoinType = joinType, TableName = rightTableName, Alias = rightAlias, OnCondition = parsed };
            lock (_stateLock) { _joins.Add(info); }
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
            var info = new ApplyInfo { ApplyType = applyType, SubQuery = "(" + subSql + ") AS " + QuoteIdentifier(applyAlias), SubQueryAlias = applyAlias };
            lock (_stateLock) { _applies.Add(info); }
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
            lock (_stateLock)
            {
                if (op.StartsWith("UNION")) _unionClauses.Add(clause);
                else if (op == "INTERSECT") _intersectClauses.Add(clause);
                else if (op == "EXCEPT") _exceptClauses.Add(clause);
            }
        }

        public IEnumerable<T> Execute()
        {
            lock (_executeLock)
            {
                var query = BuildQuery();
                return GetOpenConnection().Query<T>(query, _parameterBuilder.GetParameters(),
                    GetTransaction(), commandTimeout: _timeOut);
            }
        }

        public IEnumerable<TResult> Execute<TResult>()
        {
            lock (_executeLock)
            {
                var query = BuildQuery();
                return GetOpenConnection().Query<TResult>(query, _parameterBuilder.GetParameters(),
                    GetTransaction(), commandTimeout: _timeOut);
            }
        }

        public Task<IEnumerable<T>> ExecuteAsync()
        {
            string query;
            IDictionary<string, object> parameters;
            IDbTransaction transaction;
            int timeout;
            lock (_executeLock)
            {
                query = BuildQuery();
                parameters = _parameterBuilder.GetParameters();
                transaction = GetTransaction();
                timeout = _timeOut;
            }
            return ExecuteAsyncCore<T>(query, parameters, transaction, timeout);
        }

        public Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            string query;
            IDictionary<string, object> parameters;
            IDbTransaction transaction;
            int timeout;
            lock (_executeLock)
            {
                query = BuildQuery();
                parameters = _parameterBuilder.GetParameters();
                transaction = GetTransaction();
                timeout = _timeOut;
            }
            return ExecuteAsyncCore<TResult>(query, parameters, transaction, timeout);
        }

        private async Task<IEnumerable<TResult>> ExecuteAsyncCore<TResult>(string query, IDictionary<string, object> parameters, IDbTransaction transaction, int timeout)
        {
            var connection = GetOpenConnection();
            return await connection.QueryAsync<TResult>(query, parameters, transaction, commandTimeout: timeout).ConfigureAwait(false);
        }

        public string GetRawSql()
        {
            lock (_stateLock) { return BuildQueryLocked(); }
        }

        internal string BuildQuery()
        {
            lock (_stateLock) { return BuildQueryLocked(); }
        }

        private string BuildQueryLocked()
        {
            ValidateAggregatesLocked();
            var selectClause = BuildSelectClauseLocked();
            var fromClause = BuildFromClauseLocked();
            var joinClauses = BuildJoinClausesLocked();
            var applyClauses = BuildApplyClausesLocked();
            var whereClause = BuildWhereClauseLocked();
            var groupByClause = BuildGroupByClauseLocked();
            var havingClause = BuildHavingClauseLocked();
            var orderByClause = BuildOrderByClauseLocked();
            var paginationClause = BuildPaginationClauseLocked(ref orderByClause);
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

        private void ValidateAggregatesLocked()
        {
            if (_aggregateColumns.Count > 0 && _groupByColumns.Count == 0 && _selectedColumnsQueue.Count > 0)
                throw new InvalidOperationException("When using aggregate functions, either use GROUP BY or avoid selecting non-aggregate columns.");
        }

        private string BuildSelectClauseLocked()
        {
            string columns;
            if (_isCountQuery) columns = "COUNT(*) AS TotalCount";
            else if (_selectedColumnsQueue.Count > 0)
            {
                columns = string.Join(", ", _selectedColumnsQueue);
                if (_aggregateColumns.Count > 0) columns = string.Join(", ", _aggregateColumns) + ", " + columns;
            }
            else if (_aggregateColumns.Count > 0) columns = string.Join(", ", _aggregateColumns);
            else columns = string.Join(", ", typeof(T).GetProperties().Select(p =>
                _aliasManager.GetAliasForType(typeof(T)) + ".[" + QueryBuilderCache.GetColumnName(p) + "] AS " + QuoteIdentifier(p.Name)));
            if (!string.IsNullOrEmpty(_rowNumberClause)) columns = _rowNumberClause + ", " + columns;
            var result = new StringBuilder("SELECT");
            if (!string.IsNullOrEmpty(_distinctClause)) result.Append(' ').Append(_distinctClause);
            if (!string.IsNullOrEmpty(_topClause)) result.Append(' ').Append(_topClause);
            result.Append(' ').Append(columns);
            return result.ToString();
        }

        private string BuildFromClauseLocked()
        {
            var tableName = QueryBuilderCache.GetTableName(typeof(T));
            var alias = _aliasManager.GetAliasForTable(tableName);
            return " FROM " + tableName + " AS " + QuoteIdentifier(alias);
        }

        private string BuildJoinClausesLocked()
        {
            var sb = new StringBuilder();
            foreach (var join in _joins)
                sb.Append(' ').Append(join.JoinType).Append(' ')
                  .Append(join.TableName + " AS " + QuoteIdentifier(join.Alias))
                  .Append(" ON ").Append(join.OnCondition);
            return sb.ToString();
        }

        private string BuildApplyClausesLocked()
        {
            var sb = new StringBuilder();
            foreach (var apply in _applies)
                sb.Append(' ').Append(apply.ApplyType).Append(' ').Append(apply.SubQuery);
            return sb.ToString();
        }

        private string BuildWhereClauseLocked()
            => _filters.Count > 0 ? "WHERE " + string.Join(" AND ", _filters) : string.Empty;

        private string BuildGroupByClauseLocked()
            => _groupByColumns.Count > 0 ? "GROUP BY " + string.Join(", ", _groupByColumns) : string.Empty;

        private string BuildHavingClauseLocked()
            => !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : string.Empty;

        private string BuildOrderByClauseLocked()
            => _orderByQueue.Count > 0 ? "ORDER BY " + string.Join(", ", _orderByQueue) : string.Empty;

        private string BuildPaginationClauseLocked(ref string orderByClause)
        {
            if (!_limit.HasValue) return string.Empty;
            if (string.IsNullOrEmpty(orderByClause))
            {
                _orderByQueue.Add("(SELECT 1)");
                orderByClause = BuildOrderByClauseLocked();
            }
            return "OFFSET " + _offset + " ROWS FETCH NEXT " + _limit + " ROWS ONLY";
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

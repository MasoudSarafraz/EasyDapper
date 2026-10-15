using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        private HashSet<string> _pendingJoinAliases;
        private readonly List<ApplyInfo> _applies = new List<ApplyInfo>();
        private readonly List<string> _aggregateColumns = new List<string>();
        private readonly List<string> _groupByColumns = new List<string>();
        private readonly List<string> _selectedColumnsQueue = new List<string>();
        private readonly List<Type> _selectedColumnSources = new List<Type>();
        private readonly List<string> _selectedColumnAliases = new List<string>();
        private readonly List<string> _orderByQueue = new List<string>();
        private readonly List<string> _typedOrderBy = new List<string>();
        private readonly Dictionary<string, int> _selectedOutputBindings = new Dictionary<string, int>(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _aggregateOutputBindings = new Dictionary<string, int>(StringComparer.Ordinal);
        private readonly List<string> _selectedOutputNames = new List<string>();
        private readonly List<string> _aggregateOutputNames = new List<string>();
        private IDictionary<string, string> _inheritedOutputBindings;
        private IDictionary<string, int> _setOutputOrdinals;
        private IList<string> _plainSetOutputNames;
        private IList<string> _setOutputNames;
        private string _rowNumberClause = string.Empty;
        private string _rowNumberOutputName;
        private string _havingClause = string.Empty;
        private int _timeOut;
        private string _distinctClause = string.Empty;
        private string _topClause = string.Empty;
        private bool _isCountQuery = false;
        private int? _limit = null;
        private int? _offset = null;
        private bool _disposed = false;

        private readonly ConnectionManager _connectionManager;
        private readonly ConnectionManager _executionManager;
        private readonly StoredProcedureExecutor _commandExecutor;
        private readonly object _stateLock = new object();
        private readonly object _scopeLock = new object();
        private readonly ThreadLocal<int> _translationDepth = new ThreadLocal<int>();
        private int _activeTranslations;
        private bool _scopeChanging;
        private int _correlationAnchorCounter;
        private string _setSql;
        private bool _setRequiresDerivedSource;
        private string _setSourceAlias;
        private static readonly Regex SqlParameterTokens = new Regex(
            @"'(?:''|[^'])*'|\[(?:\]\]|[^\]])*\]|""(?:""""|[^""])*""|(?<![\w@])@[A-Za-z_][A-Za-z0-9_]*",
            RegexOptions.CultureInvariant);
        private static readonly Regex SqlIdentifierTokens = new Regex(
            @"'(?:''|[^'])*'|\[(?:\]\]|[^\]])*\]|""(?:""""|[^""])*""|[\p{L}_#][\p{L}\p{N}_#$]*|[0-9]+", RegexOptions.CultureInvariant);

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
            return "[" + identifier.Replace("]", "]]") + "]";
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
            _expressionParser.EvaluateConsumerCode = EvaluateConsumerCode;
            _connectionManager = connectionManager;
            _executionManager = connectionManager ?? new ConnectionManager(directConnection);
            _commandExecutor = new StoredProcedureExecutor(_executionManager, new SqlBuilder(new QueryCache()));
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
            var mainAlias = _aliasManager.GetAliasForTable(mainTableName);
            _aliasManager.SetTypeAlias(typeof(T), mainAlias);
        }

        public void Where(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException("filter");
            using (AcquireScope(false))
                lock (_stateLock) { _filters.Add(ParseAtomically(() => _expressionParser.ParseExpressionWithParameterMappingAndBrackets(filter.Body, ParameterAliases(filter)))); }
        }

        public void Select(params Expression<Func<T, object>>[] columns)
        {
            Select<T>(columns);
        }

        public void Select<TSource>(params Expression<Func<TSource, object>>[] columns)
        {
            if (columns == null) return;
            using (AcquireScope(false))
            lock (_stateLock)
            {
                var parsedColumns = ParseAtomically(() => columns.Where(column => column != null).Select(column =>
                {
                    var aliases = ParameterAliases(column);
                    var selection = new SelectionInfo
                    {
                        Sql = _expressionParser.ParseSelectMember(column.Body, aliases),
                        Alias = aliases[column.Parameters[0]]
                    };
                    RecordProjection(selection, column.Body, typeof(TSource), aliases);
                    return selection;
                }).ToList());
                foreach (var column in parsedColumns)
                {
                    _selectedColumnsQueue.Add(column.Sql);
                    _selectedColumnSources.Add(typeof(TSource));
                    _selectedColumnAliases.Add(column.Alias);
                    int outputOffset = _selectedOutputNames.Count;
                    _selectedOutputNames.AddRange(column.OutputNames);
                    foreach (var binding in column.OutputBindings) _selectedOutputBindings[binding.Key] = outputOffset + binding.Value;
                }
            }
        }

        internal void SetTableAlias(string tableName, string alias)
        {
            using (AcquireScope(false))
            lock (_stateLock)
            {
                string current;
                if (_aliasManager.TryGetTableAlias(tableName, out current) && current == alias) return;
            }
            using (AcquireScope(true))
            lock (_stateLock)
            {
                string oldAlias;
                if (_aliasManager.TryGetTableAlias(tableName, out oldAlias) && oldAlias != alias)
                {
                    var fragments = _filters.Concat(_selectedColumnsQueue).Concat(_aggregateColumns).Concat(_groupByColumns)
                        .Concat(_typedOrderBy).Concat(new[] { _rowNumberClause, _havingClause })
                        .Concat(_joins.Select(join => join.OnCondition)).Concat(_applies.Select(apply => apply.ParentReferences));
                    if (_joins.Any(join => join.Alias == oldAlias) || fragments.Any(sql => ReferencesAlias(sql, oldAlias)))
                        throw new InvalidOperationException("Set a table alias before adding clauses that reference that table.");
                }
                _aliasManager.SetTableAlias(tableName, alias);
            }
        }

        private static bool ReferencesAlias(string sql, string alias)
        {
            if (string.IsNullOrEmpty(sql)) return false;
            foreach (Match token in SqlIdentifierTokens.Matches(sql))
            {
                if (token.Value[0] == '\'') continue;
                var identifier = token.Value;
                if (identifier[0] == '[') identifier = identifier.Substring(1, identifier.Length - 2).Replace("]]", "]");
                else if (identifier[0] == '"') identifier = identifier.Substring(1, identifier.Length - 2).Replace("\"\"", "\"");
                if (identifier != alias) continue;
                int next = token.Index + token.Length;
                while (next < sql.Length && char.IsWhiteSpace(sql[next])) next++;
                if (next < sql.Length && sql[next] == '.') return true;
            }
            return false;
        }

        private TResult ParseAtomically<TResult>(Func<TResult> parse)
        {
            var checkpoint = _parameterBuilder.CreateCheckpoint();
            try
            {
                var result = parse();
                _parameterBuilder.CommitCheckpoint(checkpoint);
                return result;
            }
            catch
            {
                _parameterBuilder.RestoreCheckpoint(checkpoint);
                throw;
            }
        }

        private object EvaluateConsumerCode(Func<object> evaluate)
        {
            // Captured getters may wait for another thread to add a clause to
            // this builder, as they could before translation moved under the lock.
            bool entered = Monitor.IsEntered(_stateLock);
            if (entered) Monitor.Exit(_stateLock);
            try { return evaluate(); }
            finally { if (entered) Monitor.Enter(_stateLock); }
        }

        private IDisposable AcquireScope(bool changeScope)
        {
            lock (_scopeLock)
            {
                if (changeScope && _translationDepth.Value != 0)
                    throw new InvalidOperationException("A query scope cannot change from inside its predicate translation.");
                // A pending scope change must still admit nested predicates that
                // a captured getter is waiting for on another thread.
                while (_scopeChanging || changeScope && _activeTranslations != 0) Monitor.Wait(_scopeLock);
                if (changeScope) _scopeChanging = true;
                else { _activeTranslations++; _translationDepth.Value++; }
            }
            return new ScopeLease(this, changeScope);
        }

        private sealed class ScopeLease : IDisposable
        {
            private QueryBuilderCore<T> _owner;
            private readonly bool _changesScope;
            internal ScopeLease(QueryBuilderCore<T> owner, bool changesScope) { _owner = owner; _changesScope = changesScope; }
            public void Dispose()
            {
                var owner = Interlocked.Exchange(ref _owner, null);
                if (owner == null) return;
                lock (owner._scopeLock)
                {
                    if (_changesScope) owner._scopeChanging = false;
                    else { owner._activeTranslations--; owner._translationDepth.Value--; }
                    Monitor.PulseAll(owner._scopeLock);
                }
            }
        }

        private sealed class SelectionInfo
        {
            internal string Sql;
            internal string Alias;
            internal readonly List<string> OutputNames = new List<string>();
            internal readonly Dictionary<string, int> OutputBindings = new Dictionary<string, int>(StringComparer.Ordinal);
        }

        private static MemberExpression SelectedMember(Expression expression)
        {
            while (expression is UnaryExpression) expression = ((UnaryExpression)expression).Operand;
            var member = expression as MemberExpression;
            if (member != null && member.Member.Name == "Value" && member.Expression != null
                && Nullable.GetUnderlyingType(member.Expression.Type) != null) return SelectedMember(member.Expression);
            return member;
        }

        private void RecordProjection(SelectionInfo selection, Expression expression, Type sourceType,
            Dictionary<ParameterExpression, string> aliases)
        {
            while (expression is UnaryExpression) expression = ((UnaryExpression)expression).Operand;
            var created = expression as NewExpression;
            if (created != null)
            {
                for (int i = 0; i < created.Arguments.Count; i++)
                {
                    var member = SelectedMember(created.Arguments[i]);
                    var argumentMember = created.Arguments[i] as MemberExpression;
                    // A boxed constructor argument is emitted without AS by the
                    // historical parser, so its output is the mapped column name.
                    bool nullableValue = argumentMember != null && argumentMember.Member.Name == "Value" && argumentMember.Expression != null
                        && Nullable.GetUnderlyingType(argumentMember.Expression.Type) != null && member != null;
                    var output = nullableValue ? member.Member.Name : argumentMember != null ? created.Members[i].Name
                        : member != null ? _expressionParser.GetUnaliasedOutputName(member, aliases) : null;
                    if (output == null) continue;
                    selection.OutputNames.Add(output);
                    if (member != null && sourceType == typeof(T)) selection.OutputBindings[member.Member.Name] = selection.OutputNames.Count - 1;
                }
            }
            else
            {
                var member = SelectedMember(expression);
                if (member == null) return;
                selection.OutputNames.Add(member.Member.Name);
                if (sourceType == typeof(T)) selection.OutputBindings[member.Member.Name] = 0;
            }
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
            lock (_stateLock)
            {
                var columns = _expressionParser.ExtractColumnListWithBrackets(keySelector.Body, ParameterAliases(keySelector)).Select(c => c + " ASC").ToList();
                _orderByQueue.AddRange(columns);
                _typedOrderBy.AddRange(columns);
            }
        }

        public void OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            lock (_stateLock)
            {
                var columns = _expressionParser.ExtractColumnListWithBrackets(keySelector.Body, ParameterAliases(keySelector)).Select(c => c + " DESC").ToList();
                _orderByQueue.AddRange(columns);
                _typedOrderBy.AddRange(columns);
            }
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

        public void Sum(Expression<Func<T, object>> column, string alias = null) => AddAggregate("SUM", column, alias);
        public void Avg(Expression<Func<T, object>> column, string alias = null) => AddAggregate("AVG", column, alias);
        public void Min(Expression<Func<T, object>> column, string alias = null) => AddAggregate("MIN", column, alias);
        public void Max(Expression<Func<T, object>> column, string alias = null) => AddAggregate("MAX", column, alias);
        public void Count(Expression<Func<T, object>> column, string alias = null) => AddAggregate("COUNT", column, alias);

        private void AddAggregate(string fn, Expression<Func<T, object>> column, string alias)
        {
            if (column == null) throw new ArgumentNullException("column");
            ValidateCustomAlias(alias);
            lock (_stateLock)
            {
                var member = SelectedMember(column.Body);
                var parsed = _expressionParser.ParseMemberWithBrackets(column.Body, ParameterAliases(column));
                bool implicitAlias = string.IsNullOrEmpty(alias);
                if (string.IsNullOrEmpty(alias))
                {
                    var baseName = fn + "_" + (member != null ? member.Member.Name : "Value");
                    alias = baseName;
                    int suffix = 1;
                    while (_aggregateOutputNames.Contains(alias)) alias = baseName + "_" + (++suffix);
                }
                _aggregateColumns.Add(fn + "(" + parsed + ")" + (implicitAlias ? string.Empty : " AS " + QuoteIdentifier(alias)));
                _aggregateOutputNames.Add(alias);
                if (member != null) _aggregateOutputBindings[member.Member.Name] = _aggregateOutputNames.Count - 1;
            }
        }

        public void GroupBy(params Expression<Func<T, object>>[] groupByColumns)
        {
            if (groupByColumns == null) return;
            using (AcquireScope(false))
            lock (_stateLock)
                _groupByColumns.AddRange(ParseAtomically(() => groupByColumns.Where(column => column != null)
                    .Select(column => _expressionParser.ParseMemberWithBrackets(column.Body, ParameterAliases(column))).ToList()));
        }

        public void Having(Expression<Func<T, bool>> havingCondition)
        {
            if (havingCondition == null) throw new ArgumentNullException("havingCondition");
            using (AcquireScope(false))
            lock (_stateLock)
                _havingClause = ParseAtomically(() => _expressionParser.ParseExpressionWithParameterMappingAndBrackets(havingCondition.Body, ParameterAliases(havingCondition)));
        }

        public void Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber")
        {
            if (partitionBy == null) throw new ArgumentNullException("partitionBy");
            if (orderBy == null) throw new ArgumentNullException("orderBy");
            ValidateCustomAlias(alias);
            lock (_stateLock)
            {
                var parts = _expressionParser.ExtractColumnListWithBrackets(partitionBy.Body, ParameterAliases(partitionBy));
                var orders = _expressionParser.ExtractColumnListWithBrackets(orderBy.Body, ParameterAliases(orderBy));
                _rowNumberClause = "ROW_NUMBER() OVER (PARTITION BY " + string.Join(", ", parts)
                    + " ORDER BY " + string.Join(", ", orders) + ") AS " + QuoteIdentifier(alias);
                _rowNumberOutputName = alias;
            }
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
            using (AcquireScope(false))
            lock (_stateLock)
            {
                bool crossJoin = string.Equals(Regex.Replace(joinType.Trim(), @"\s+", " "), "CROSS JOIN", StringComparison.OrdinalIgnoreCase);
                var leftTableName = QueryBuilderCache.GetTableName(typeof(TLeft));
                var rightTableName = QueryBuilderCache.GetTableName(typeof(TRight));
                var leftAlias = GetSourceAlias(typeof(TLeft));
                string rightAlias;
                bool isSelfJoin = typeof(TLeft) == typeof(TRight);
                int joinCount = _joins.Count(j => j.TableName == rightTableName);
                bool isRepeatedJoin = joinCount > 0;
                var candidate = _aliasManager.GetAliasForType(typeof(TRight));
                bool candidateInUse = candidate == GetSourceAlias(typeof(T)) || _joins.Any(j => j.Alias == candidate)
                    || _pendingJoinAliases != null && _pendingJoinAliases.Contains(candidate);
                if (isSelfJoin || isRepeatedJoin || candidateInUse)
                    rightAlias = _aliasManager.GetUniqueAliasForType(typeof(TRight));
                else rightAlias = candidate;
                var parameterAliases = new Dictionary<ParameterExpression, string>
                {
                    { onCondition.Parameters[0], leftAlias },
                    { onCondition.Parameters[1], rightAlias }
                };
                // Captured getters can release the state lock and add another
                // JOIN. Keep this role occupied until its translation completes.
                if (_pendingJoinAliases == null) _pendingJoinAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                _pendingJoinAliases.Add(rightAlias);
                try
                {
                    var parsed = ParseAtomically(() => _expressionParser.ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases));
                    if (rightAlias != candidate && !isSelfJoin && typeof(TRight) != typeof(T) && !_joins.Any(j => j.SourceType == typeof(TRight)))
                    {
                        // Commit a new distinct CLR role only after parsing succeeds.
                        // Same-CLR self joins keep their established primary binding.
                        _aliasManager.SetTypeAlias(typeof(TRight), rightAlias);
                        RebindSelectedSource(typeof(TRight), candidate, rightAlias);
                    }
                    var info = new JoinInfo { JoinType = joinType, TableName = rightTableName, Alias = rightAlias, OnCondition = parsed, IsCrossJoin = crossJoin, SourceType = typeof(TRight) };
                    _joins.Add(info);
                    if (crossJoin) _filters.Add("(" + parsed + ")");
                }
                finally { _pendingJoinAliases.Remove(rightAlias); }
            }
        }

        private void RebindSelectedSource(Type source, string oldAlias, string newAlias)
        {
            var tokens = new Regex(@"'(?:''|[^'])*'|\[(?:\]\]|[^\]])*\]|""(?:""""|[^""])*""|(?<![\w@$#])"
                + Regex.Escape(oldAlias) + @"(?=\s*\.)", RegexOptions.CultureInvariant);
            for (int i = 0; i < _selectedColumnsQueue.Count; i++)
            {
                if (_selectedColumnSources[i] != source || _selectedColumnAliases[i] != oldAlias) continue;
                var sql = _selectedColumnsQueue[i];
                _selectedColumnsQueue[i] = tokens.Replace(sql, match =>
                {
                    int next = match.Index + match.Length;
                    while (next < sql.Length && char.IsWhiteSpace(sql[next])) next++;
                    if (next == sql.Length || sql[next] != '.') return match.Value;
                    if (match.Value == oldAlias) return newAlias;
                    if (match.Value == QuoteIdentifier(oldAlias)) return QuoteIdentifier(newAlias);
                    return match.Value;
                });
                _selectedColumnAliases[i] = newAlias;
            }
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
                sub = new QueryBuilder<TSubQuery>(_connectionManager, null, _aliasManager);
            }
            else
            {
                sub = new QueryBuilder<TSubQuery>(_lazyConnection.Value, _aliasManager);
            }
            var qb = subBuilder(sub);
            if (qb == null) throw new InvalidOperationException("Sub-query builder returned null");
            var subQb = (QueryBuilder<TSubQuery>)qb;
            using (AcquireScope(false))
            {
                // Capture the returned builder after the callback, which may have
                // changed the parent scope or returned an independent query.
                // Do not hold two builders' state locks at once.
                var snapshot = subQb.CaptureApplyQuery();
                string subSql = PrepareDerivedOperand(snapshot.Sql);
                string actualSubAlias = snapshot.SourceAlias;
                var sourceColumns = snapshot.SourceColumns;
                var outputColumns = snapshot.OutputColumns;
                var outputNames = UniqueOutputNames(snapshot.OutputNames);
                lock (_stateLock)
                {
                    var parentAlias = GetSourceAlias(typeof(T));
                    var leftSymbol = CorrelationSymbol("__EasyDapperCorrelationLeft", subQb.GetAliasManager(), actualSubAlias);
                    var rightSymbol = CorrelationSymbol("__EasyDapperCorrelationRight", subQb.GetAliasManager(), actualSubAlias);
                    string anchor = null;
                    if (snapshot.ActiveAliases.Contains(parentAlias, StringComparer.OrdinalIgnoreCase))
                    {
                        do { anchor = "__EasyDapperCorrelation_C" + (++_correlationAnchorCounter); }
                        while (subQb.GetAliasManager().IsReservedName(anchor)
                            || string.Equals(anchor, actualSubAlias, StringComparison.OrdinalIgnoreCase)
                            || !_aliasManager.ReserveAliasName(anchor));
                    }
                    var previousLeft = _expressionParser.GetProjectedColumns(leftSymbol);
                    var previousRight = _expressionParser.GetProjectedColumns(rightSymbol);
                    var onSql = ParseAtomically(() =>
                    {
                        var renamings = _parameterBuilder.MergeParameters(snapshot.Parameters);
                        subSql = ReplaceParameters(subSql, renamings);
                        try
                        {
                            if (_setSql != null) _expressionParser.SetProjectedColumns(leftSymbol, _inheritedOutputBindings);
                            if (sourceColumns != null) _expressionParser.SetProjectedColumns(rightSymbol, sourceColumns);
                            var parameterAliases = new Dictionary<ParameterExpression, string>
                            {
                                { onCondition.Parameters[0], leftSymbol },
                                { onCondition.Parameters[1], rightSymbol }
                            };
                            return _expressionParser.ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);
                        }
                        finally
                        {
                            _expressionParser.RestoreProjectedColumns(leftSymbol, previousLeft);
                            _expressionParser.RestoreProjectedColumns(rightSymbol, previousRight);
                        }
                    });

                    // Only generated ON qualifiers are replaced. The returned
                    // query's SQL, including nested raw scopes, stays untouched.
                    var columnPattern = Regex.Escape(AliasManager.SqlReference(leftSymbol)) + @"\.(\[(?:\]\]|[^\]])*\])";
                    var parentColumns = Regex.Matches(onSql, columnPattern).Cast<Match>()
                        .Select(match => match.Groups[1].Value).Distinct(StringComparer.Ordinal).ToArray();
                    var parentReferences = string.Join(", ", parentColumns.Select(column => AliasManager.SqlReference(parentAlias) + "." + column));
                    string parentProjection = null;
                    if (anchor != null && parentColumns.Length != 0)
                        parentProjection = "CROSS APPLY (SELECT " + string.Join(", ", parentColumns.Select(column =>
                            AliasManager.SqlReference(parentAlias) + "." + column + " AS " + column)) + ") AS " + QuoteIdentifier(anchor);
                    else anchor = null;
                    onSql = ReplaceCorrelationQualifier(onSql, leftSymbol, anchor ?? parentAlias);
                    onSql = ReplaceCorrelationQualifier(onSql, rightSymbol, actualSubAlias);

                    int whereIndex = FindTopLevelClause(subSql, "WHERE");
                    if (whereIndex >= 0) subSql = subSql.Insert(whereIndex + 6, "(" + onSql + ") AND ");
                    else subSql = subSql.Insert(FindWhereInsertPosition(subSql), " WHERE " + onSql);

                    var applyAlias = _aliasManager.GenerateSubQueryAlias(QueryBuilderCache.GetTableName(typeof(TSubQuery)));
                    _aliasManager.SetSubQueryAlias(typeof(TSubQuery), applyAlias);
                    _expressionParser.SetProjectedColumns(applyAlias, outputColumns);
                    _applies.Add(new ApplyInfo
                    {
                        ApplyType = applyType,
                        SubQuery = "(" + subSql + ") AS " + QuoteIdentifier(applyAlias)
                            + " (" + string.Join(", ", outputNames.Select(QuoteIdentifier)) + ")",
                        SubQueryAlias = applyAlias, ParentProjection = parentProjection,
                        ParentProjectionAlias = anchor, ParentReferences = parentReferences
                    });
                }
            }
        }

        private string CorrelationSymbol(string prefix, AliasManager subAliases, string sourceAlias)
        {
            int suffix = 0;
            string symbol;
            do { symbol = prefix + "_C" + (++suffix); }
            while (subAliases.IsReservedName(symbol)
                || string.Equals(symbol, sourceAlias, StringComparison.OrdinalIgnoreCase)
                || !_aliasManager.ReserveAliasName(symbol));
            return symbol;
        }

        private static string ReplaceCorrelationQualifier(string sql, string symbol, string alias)
        {
            return SqlIdentifierTokens.Replace(sql, token =>
            {
                if (token.Value != QuoteIdentifier(symbol)) return token.Value;
                int previous = token.Index - 1;
                while (previous >= 0 && char.IsWhiteSpace(sql[previous])) previous--;
                if (previous >= 0 && sql[previous] == '.') return token.Value;
                int next = token.Index + token.Length;
                while (next < sql.Length && char.IsWhiteSpace(sql[next])) next++;
                return next < sql.Length && sql[next] == '.' ? AliasManager.SqlReference(alias) : token.Value;
            });
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

            string otherSql = otherQb.BuildQuery();
            var otherParams = otherQb.GetParameterBuilder().GetParameters();
            using (AcquireScope(true))
            lock (_stateLock)
            {
                var ownSql = BuildQueryLocked();
                var requiresDerivedSource = FindTopLevelClause(ownSql, "ORDER BY") >= 0
                    || FindTopLevelClause(otherSql, "ORDER BY") >= 0;
                var outputOrdinals = CurrentOutputOrdinals();
                var outputNames = CurrentOutputNames();
                var renamings = ParseAtomically(() => _parameterBuilder.MergeParameters(otherParams));
                otherSql = ReplaceParameters(otherSql, renamings);
                _setSql = "(" + PrepareDerivedOperand(ownSql) + ") " + op + " (" + PrepareDerivedOperand(otherSql) + ")";
                _setRequiresDerivedSource = requiresDerivedSource;
                _filters.Clear(); _joins.Clear(); _applies.Clear(); _aggregateColumns.Clear();
                _groupByColumns.Clear(); _selectedColumnsQueue.Clear(); _orderByQueue.Clear();
                _typedOrderBy.Clear();
                _selectedColumnSources.Clear(); _selectedColumnAliases.Clear();
                _selectedOutputNames.Clear(); _aggregateOutputNames.Clear();
                _selectedOutputBindings.Clear(); _aggregateOutputBindings.Clear();
                _rowNumberClause = _havingClause = _distinctClause = _topClause = string.Empty;
                _rowNumberOutputName = null;
                _isCountQuery = false; _limit = _offset = null;
                _aliasManager.ClearSubQueryTypeBindings();
                _setSourceAlias = _aliasManager.GenerateSubQueryAlias(QueryBuilderCache.GetTableName(typeof(T)));
                _aliasManager.SetSubQueryAlias(typeof(T), _setSourceAlias);
                _setOutputOrdinals = outputOrdinals;
                _plainSetOutputNames = outputNames;
                _setOutputNames = UniqueOutputNames(outputNames);
                var sourceBindings = BindOutputColumns(_setOutputNames, outputOrdinals);
                _inheritedOutputBindings = sourceBindings;
                _expressionParser.SetProjectedColumns(_setSourceAlias, sourceBindings);
            }
        }

        private IDictionary<string, string> CurrentOutputBindings()
            => BindOutputColumns(CurrentOutputNames(), CurrentOutputOrdinals());

        private static IDictionary<string, string> BindOutputColumns(IList<string> names, IDictionary<string, int> ordinals)
            => ordinals.ToDictionary(binding => binding.Key, binding => names[binding.Value], StringComparer.Ordinal);

        // Derived sources must have unique names. Preserve member positions so
        // renaming duplicate headers cannot redirect a member to another column.
        private IDictionary<string, int> CurrentOutputOrdinals()
        {
            var bindings = new Dictionary<string, int>(StringComparer.Ordinal);
            if (_isCountQuery) return bindings;
            if (_selectedColumnsQueue.Count == 0 && _aggregateColumns.Count == 0)
            {
                if (_setSql != null) bindings = new Dictionary<string, int>(_setOutputOrdinals, StringComparer.Ordinal);
                else
                {
                    var properties = typeof(T).GetProperties();
                    for (int i = 0; i < properties.Length; i++) bindings[properties[i].Name] = i;
                }
            }
            else
            {
                foreach (var binding in _aggregateOutputBindings) bindings[binding.Key] = binding.Value;
                foreach (var binding in _selectedOutputBindings) bindings[binding.Key] = _aggregateOutputNames.Count + binding.Value;
            }
            if (!string.IsNullOrEmpty(_rowNumberClause))
                foreach (var member in bindings.Keys.ToArray()) bindings[member]++;
            foreach (var property in typeof(T).GetProperties())
            {
                if (bindings.ContainsKey(property.Name)) continue;
                int position = _aggregateOutputNames.IndexOf(property.Name);
                if (position < 0)
                {
                    position = _selectedOutputNames.IndexOf(property.Name);
                    if (position >= 0) position += _aggregateOutputNames.Count;
                }
                if (position >= 0) bindings[property.Name] = position + (string.IsNullOrEmpty(_rowNumberClause) ? 0 : 1);
            }
            return bindings;
        }

        private IList<string> CurrentOutputNames()
        {
            var names = new List<string>();
            if (_isCountQuery) names.Add("TotalCount");
            else if (_selectedColumnsQueue.Count > 0) { names.AddRange(_aggregateOutputNames); names.AddRange(_selectedOutputNames); }
            else if (_aggregateColumns.Count > 0) names.AddRange(_aggregateOutputNames);
            else if (_setSql != null) names.AddRange(HasOuterOperations() ? _setOutputNames : _plainSetOutputNames);
            else names.AddRange(typeof(T).GetProperties().Select(property => property.Name));
            if (!string.IsNullOrEmpty(_rowNumberClause)) names.Insert(0, _rowNumberOutputName);
            return names;
        }

        private static IList<string> UniqueOutputNames(IList<string> names)
        {
            var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var result = new List<string>();
            foreach (var name in names)
            {
                string candidate = name;
                int suffix = 1;
                while (!used.Add(candidate)) candidate = name + "_" + (++suffix);
                result.Add(candidate);
            }
            return result;
        }

        private bool HasOuterOperations()
            // SQL Server accepts ordered SET operands inside a derived source,
            // including when the consumer adds no subsequent fluent operation.
            => _setRequiresDerivedSource || _filters.Count != 0 || _joins.Count != 0 || _applies.Count != 0 || _aggregateColumns.Count != 0
                || _groupByColumns.Count != 0 || _selectedColumnsQueue.Count != 0 || _orderByQueue.Count != 0
                || !string.IsNullOrEmpty(_rowNumberClause) || !string.IsNullOrEmpty(_havingClause)
                || !string.IsNullOrEmpty(_distinctClause) || !string.IsNullOrEmpty(_topClause)
                || _isCountQuery || _limit.HasValue;

        private static string PrepareDerivedOperand(string sql)
        {
            sql = PreserveTrailingCommentBoundary(sql);
            // A branch ORDER BY is valid in a derived table with TOP or OFFSET.
            // OFFSET 0 keeps ordered branches valid when embedded in SET/APPLY.
            if (FindTopLevelClause(sql, "ORDER BY") < 0) return sql;
            if (FindTopLevelClause(sql, "OFFSET") < 0 && !Regex.IsMatch(sql, @"\ASELECT\s+(?:DISTINCT\s+)?TOP\s*\(", RegexOptions.IgnoreCase))
                sql += " OFFSET 0 ROWS";
            return sql;
        }

        private static string PreserveTrailingCommentBoundary(string sql)
        {
            int marker = sql.LastIndexOf("--", StringComparison.Ordinal);
            // A separator after a quoted marker is harmless; a real final line
            // comment must end before appended OFFSET, parentheses or aliases.
            return marker >= 0 && sql.IndexOf('\r', marker) < 0 && sql.IndexOf('\n', marker) < 0
                ? sql + "\n" : sql;
        }

        public IEnumerable<T> Execute() => Execute<T>();

        public IEnumerable<TResult> Execute<TResult>()
        {
            string query;
            IDictionary<string, object> parameters;
            IDbConnection connection;
            IDbTransaction transaction;
            int timeout;
            using (AcquireOperation())
            {
                lock (_stateLock)
                {
                    if (_disposed) throw new ObjectDisposedException("QueryBuilder");
                    query = BuildQueryLocked();
                    parameters = _parameterBuilder.GetParameters();
                    transaction = GetTransaction();
                    timeout = _timeOut;
                }
                connection = GetOpenConnection();
            }
            using (var command = _commandExecutor.PrepareText(connection, transaction, query, parameters, typeof(TResult), timeout))
            {
                try
                {
                    using (_executionManager.AcquirePreparedOperation()) return _commandExecutor.ExecutePreparedQuery<TResult>(command);
                }
                finally { command.CompleteCallbacks(); }
            }
        }

        public Task<IEnumerable<T>> ExecuteAsync() => ExecuteAsyncCore<T>();

        public Task<IEnumerable<TResult>> ExecuteAsync<TResult>() => ExecuteAsyncCore<TResult>();

        private async Task<IEnumerable<TResult>> ExecuteAsyncCore<TResult>()
        {
            string query;
            IDictionary<string, object> parameters;
            IDbConnection connection;
            IDbTransaction transaction;
            int timeout;
            lock (_stateLock)
            {
                if (_disposed) throw new ObjectDisposedException("QueryBuilder");
                query = BuildQueryLocked();
                parameters = _parameterBuilder.GetParameters();
                timeout = _timeOut;
            }
            connection = await GetOpenConnectionAsync().ConfigureAwait(false);
            using (await _executionManager.AcquirePreparedOperationAsync(CancellationToken.None).ConfigureAwait(false))
                transaction = GetTransaction();
            using (var command = _commandExecutor.PrepareText(connection, transaction, query, parameters, typeof(TResult), timeout))
            {
                try
                {
                    using (await _executionManager.AcquirePreparedOperationAsync(CancellationToken.None).ConfigureAwait(false))
                        return await _commandExecutor.ExecutePreparedQueryAsync<TResult>(command, CancellationToken.None).ConfigureAwait(false);
                }
                finally { command.CompleteCallbacks(); }
            }
        }

        public string GetRawSql()
        {
            return BuildQuery();
        }

        internal string BuildQuery()
        {
            lock (_stateLock)
            {
                return BuildQueryLocked();
            }
        }

        private string BuildQueryLocked(bool forceSetWrapper = false)
        {
            if (_setSql != null && !HasOuterOperations() && !forceSetWrapper) return _setSql;
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
            else if (_setSql != null) columns = AliasManager.SqlReference(_setSourceAlias) + ".*";
            else columns = string.Join(", ", typeof(T).GetProperties().Select(p =>
                AliasManager.SqlReference(_aliasManager.GetAliasForType(typeof(T))) + ".[" + QueryBuilderCache.GetColumnName(p) + "] AS " + QuoteIdentifier(p.Name)));
            if (!string.IsNullOrEmpty(_rowNumberClause)) columns = _rowNumberClause + ", " + columns;
            var result = new StringBuilder("SELECT");
            if (!string.IsNullOrEmpty(_distinctClause)) result.Append(' ').Append(_distinctClause);
            if (!string.IsNullOrEmpty(_topClause)) result.Append(' ').Append(_topClause);
            result.Append(' ').Append(columns);
            return result.ToString();
        }

        private string BuildFromClauseLocked()
        {
            if (_setSql != null) return " FROM (" + _setSql + ") AS " + QuoteIdentifier(_setSourceAlias)
                + " (" + string.Join(", ", _setOutputNames.Select(QuoteIdentifier)) + ")";
            var tableName = QueryBuilderCache.GetTableName(typeof(T));
            var alias = _aliasManager.GetAliasForTable(tableName);
            return " FROM " + tableName + " AS " + QuoteIdentifier(alias);
        }

        private string BuildJoinClausesLocked()
        {
            var sb = new StringBuilder();
            foreach (var join in _joins)
            {
                sb.Append(' ').Append(join.JoinType).Append(' ')
                  .Append(join.TableName + " AS " + QuoteIdentifier(join.Alias));
                if (!join.IsCrossJoin) sb.Append(" ON ").Append(join.OnCondition);
            }
            return sb.ToString();
        }

        private string BuildApplyClausesLocked()
        {
            var sb = new StringBuilder();
            foreach (var apply in _applies)
            {
                if (!string.IsNullOrEmpty(apply.ParentProjection))
                    sb.Append(' ').Append(apply.ParentProjection);
                sb.Append(' ').Append(apply.ApplyType).Append(' ').Append(apply.SubQuery);
            }
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
            int position = sql.Length;
            foreach (var clause in new[] { "GROUP BY", "HAVING", "ORDER BY", "OFFSET" })
            {
                var index = FindTopLevelClause(sql, clause);
                if (index >= 0) position = Math.Min(position, index);
            }
            // Keep a separator before any following clause when inserting WHERE.
            return position > 0 && position < sql.Length && sql[position - 1] == ' ' ? position - 1 : position;
        }

        internal static string ReplaceParameters(string sql, IDictionary<string, string> renamings)
        {
            if (renamings == null || renamings.Count == 0) return sql;
            if (sql == null || sql.IndexOf("--", StringComparison.Ordinal) < 0 && sql.IndexOf("/*", StringComparison.Ordinal) < 0)
                return ReplaceParameterTokens(sql, renamings);

            // Quotes inside SQL comments are not string delimiters. Rename each
            // code segment independently so a comment cannot hide later parameters.
            StringBuilder rewritten = null;
            int codeStart = 0;
            for (int i = 0; i < sql.Length; i++)
            {
                char c = sql[i];
                if (c == '[' || c == '\'' || c == '"')
                {
                    char closing = c == '[' ? ']' : c;
                    while (++i < sql.Length)
                    {
                        if (sql[i] != closing) continue;
                        if (i + 1 < sql.Length && sql[i + 1] == closing) i++;
                        else break;
                    }
                    continue;
                }
                bool lineComment = c == '-' && i + 1 < sql.Length && sql[i + 1] == '-';
                bool blockComment = c == '/' && i + 1 < sql.Length && sql[i + 1] == '*';
                if (!lineComment && !blockComment) continue;
                int end = i + 2;
                if (lineComment)
                {
                    while (end < sql.Length && sql[end] != '\r' && sql[end] != '\n') end++;
                }
                else
                {
                    int depth = 1;
                    while (end < sql.Length && depth != 0)
                    {
                        if (sql[end] == '/' && end + 1 < sql.Length && sql[end + 1] == '*') { depth++; end += 2; }
                        else if (sql[end] == '*' && end + 1 < sql.Length && sql[end + 1] == '/') { depth--; end += 2; }
                        else end++;
                    }
                }
                if (rewritten == null) rewritten = new StringBuilder(sql.Length);
                rewritten.Append(ReplaceParameterTokens(sql.Substring(codeStart, i - codeStart), renamings));
                rewritten.Append(sql, i, end - i);
                codeStart = end;
                i = end - 1;
            }
            if (rewritten == null) return ReplaceParameterTokens(sql, renamings);
            rewritten.Append(ReplaceParameterTokens(sql.Substring(codeStart), renamings));
            return rewritten.ToString();
        }

        private static string ReplaceParameterTokens(string sql, IDictionary<string, string> renamings)
        {
            return SqlParameterTokens.Replace(sql, match =>
            {
                string replacement;
                return match.Value[0] == '@' && renamings.TryGetValue(match.Value, out replacement) ? replacement : match.Value;
            });
        }

        private static int FindTopLevelClause(string sql, string clause)
        {
            int depth = 0;
            char quote = '\0';
            for (int i = 0; i < sql.Length; i++)
            {
                char c = sql[i];
                if (quote != '\0')
                {
                    if (c == quote)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == quote) i++;
                        else quote = '\0';
                    }
                    continue;
                }
                if (c == '[') { quote = ']'; continue; }
                if (c == '\'' || c == '"') { quote = c; continue; }
                if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < sql.Length && sql[i] != '\r' && sql[i] != '\n') i++;
                    continue;
                }
                if (c == '/' && i + 1 < sql.Length && sql[i + 1] == '*')
                {
                    int commentDepth = 1;
                    i += 2;
                    while (i < sql.Length && commentDepth > 0)
                    {
                        if (sql[i] == '/' && i + 1 < sql.Length && sql[i + 1] == '*') { commentDepth++; i += 2; }
                        else if (sql[i] == '*' && i + 1 < sql.Length && sql[i + 1] == '/') { commentDepth--; i += 2; }
                        else i++;
                    }
                    i--;
                    continue;
                }
                if (c == '(') { depth++; continue; }
                if (c == ')') { depth--; continue; }
                if (depth == 0 && (i == 0 || char.IsWhiteSpace(sql[i - 1]))
                    && i + clause.Length <= sql.Length
                    && string.Compare(sql, i, clause, 0, clause.Length, StringComparison.OrdinalIgnoreCase) == 0
                    && (i + clause.Length == sql.Length || char.IsWhiteSpace(sql[i + clause.Length]))) return i;
            }
            return -1;
        }

        internal string GetSourceAlias(Type sourceType)
        {
            if (sourceType == typeof(T)) return _setSql != null ? _setSourceAlias : _aliasManager.GetAliasForType(sourceType);
            string alias;
            return _aliasManager.TryGetSubQueryAlias(sourceType, out alias) ? alias : _aliasManager.GetAliasForType(sourceType);
        }

        private Dictionary<ParameterExpression, string> ParameterAliases(LambdaExpression expression)
            => expression.Parameters.ToDictionary(parameter => parameter, parameter => GetSourceAlias(parameter.Type));

        internal IDictionary<string, string> GetOutputColumnBindings(bool forceSetWrapper = false)
        {
            lock (_stateLock)
                return forceSetWrapper && _setSql != null && !HasOuterOperations()
                    ? new Dictionary<string, string>(_inheritedOutputBindings, StringComparer.Ordinal) : CurrentOutputBindings();
        }

        internal IList<string> GetOutputNames(bool forceSetWrapper = false)
        {
            lock (_stateLock)
                return forceSetWrapper && _setSql != null && !HasOuterOperations() ? new List<string>(_setOutputNames) : CurrentOutputNames();
        }

        internal string BuildApplyQuery()
        {
            lock (_stateLock) { return BuildQueryLocked(true); }
        }

        internal sealed class ApplyQuerySnapshot
        {
            internal string Sql;
            internal string SourceAlias;
            internal IList<string> ActiveAliases;
            internal IDictionary<string, object> Parameters;
            internal IDictionary<string, string> SourceColumns;
            internal IDictionary<string, string> OutputColumns;
            internal IList<string> OutputNames;
        }

        internal ApplyQuerySnapshot CaptureApplyQuery()
        {
            lock (_stateLock)
            {
                var sourceAlias = GetSourceAlias(typeof(T));
                var sql = BuildQueryLocked(true);
                return new ApplyQuerySnapshot
                {
                    Sql = sql,
                    SourceAlias = sourceAlias,
                    ActiveAliases = new[] { sourceAlias }.Concat(_joins.Select(join => join.Alias))
                        .Concat(_applies.Select(apply => apply.SubQueryAlias))
                        .Concat(_applies.Where(apply => apply.ParentProjectionAlias != null)
                            .Select(apply => apply.ParentProjectionAlias)).ToList(),
                    Parameters = _parameterBuilder.GetParameters(),
                    SourceColumns = _setSql != null ? new Dictionary<string, string>(_inheritedOutputBindings, StringComparer.Ordinal) : null,
                    OutputColumns = BindOutputColumns(UniqueOutputNames(GetOutputNames(true)), CurrentOutputOrdinals()),
                    OutputNames = GetOutputNames(true)
                };
            }
        }

        internal IDictionary<string, string> GetSourceColumnBindings()
        {
            lock (_stateLock) { return _setSql != null ? new Dictionary<string, string>(_inheritedOutputBindings, StringComparer.Ordinal) : null; }
        }

        private IDbConnection GetOpenConnection()
        {
            if (_connectionManager != null) return _connectionManager.GetOpenConnection();
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open) connection.Open();
            return connection;
        }

        private async Task<IDbConnection> GetOpenConnectionAsync()
        {
            if (_connectionManager != null) return await _connectionManager.GetOpenConnectionAsync().ConfigureAwait(false);
            var connection = _lazyConnection.Value;
            if (connection.State == ConnectionState.Broken) connection.Close();
            if (connection.State != ConnectionState.Open)
            {
                var dbConnection = connection as DbConnection;
                if (dbConnection != null) await dbConnection.OpenAsync().ConfigureAwait(false);
                else connection.Open();
            }
            return connection;
        }

        private IDisposable AcquireOperation()
            => _executionManager.AcquireOperation();

        private Task<IDisposable> AcquireOperationAsync()
            => _executionManager.AcquireOperationAsync();

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
            public bool IsCrossJoin { get; set; }
            public string JoinType { get; set; }
            public string TableName { get; set; }
            public string Alias { get; set; }
            public string OnCondition { get; set; }
            public Type SourceType { get; set; }
        }

        private sealed class ApplyInfo
        {
            public string ApplyType { get; set; }
            public string SubQuery { get; set; }
            public string SubQueryAlias { get; set; }
            public string ParentProjection { get; set; }
            public string ParentProjectionAlias { get; set; }
            public string ParentReferences { get; set; }
        }

        internal AliasManager GetAliasManager() => _aliasManager;
        internal ParameterBuilder GetParameterBuilder() => _parameterBuilder;
    }
}

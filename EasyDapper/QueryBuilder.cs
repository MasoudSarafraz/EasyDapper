using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
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
        private readonly Lazy<IDbConnection> _lazyConnection;
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
        private readonly ConcurrentDictionary<string, string> _tableAliasMappings = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<Type, string> _typeAliasMappings = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<Type, string> _subQueryTypeAliases = new ConcurrentDictionary<Type, string>();
        private string _rowNumberClause = string.Empty;
        private string _havingClause = string.Empty;
        private int _timeOut;
        private string _distinctClause = string.Empty;
        private string _topClause = string.Empty;
        private bool _isCountQuery = false;
        private int? _limit = null;
        private int? _offset = null;
        private readonly object _pagingLock = new object();
        private readonly string _defaultSchema = "dbo";
        private int _aliasCounter = 1;
        private int _paramCounter = 0;
        private int _subQueryCounter = 1;
        private bool _disposed = false;
        private static readonly ConcurrentDictionary<Type, string> TableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<MemberInfo, string> ColumnNameCache = new ConcurrentDictionary<MemberInfo, string>();

        private static readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(Environment.ProcessorCount * 2, Environment.ProcessorCount * 2);

        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null)
            {
                throw new ArgumentNullException(nameof(connection), "Connection cannot be null.");

            }
            _lazyConnection = new Lazy<IDbConnection>(() => connection);
            _timeOut = _lazyConnection.Value.ConnectionTimeout;
            var mainTableName = GetTableName(typeof(T));
            var mainAlias = GenerateAlias(mainTableName);
            _tableAliasMappings.TryAdd(mainTableName, mainAlias);
            _typeAliasMappings.TryAdd(typeof(T), mainAlias);
        }


        public IQueryBuilder<T> WithTableAlias(string tableName, string customAlias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(customAlias))
            {
                throw new ArgumentNullException("Table name and alias cannot be null or empty.");

            }
            _tableAliasMappings.AddOrUpdate(tableName, customAlias, (k, v) => customAlias);
            return this;
        }
        //public IQueryBuilder<T> WithTableAlias(Type tableType)
        //{
        //    if (tableType == null) throw new ArgumentNullException(nameof(tableType));
        //    if (string.IsNullOrEmpty(customAlias)) throw new ArgumentNullException(nameof(customAlias));
        //    var tableName = GetTableName(tableType);
        //    return WithTableAlias(tableName, customAlias);
        //}
        public IQueryBuilder<T> WithTableAlias(Type tableType, string customAlias)
        {
            if (tableType == null) throw new ArgumentNullException(nameof(tableType));
            if (string.IsNullOrEmpty(customAlias)) throw new ArgumentNullException(nameof(customAlias));
            var tableName = GetTableName(tableType);
            return WithTableAlias(tableName, customAlias);
        }

        public IQueryBuilder<T> WithTableAlias<TTable>(string customAlias)
        {
            return WithTableAlias(typeof(TTable), customAlias);
        }

        public IQueryBuilder<T> Where(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            _filters.Enqueue(ParseExpressionWithBrackets(filter.Body));
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
            {
                _selectedColumnsQueue.Enqueue(ParseSelectMember(c.Body));
            }                
            return this;
        }

        public IQueryBuilder<T> Select(Expression<Func<T, object>> columns)
        {
            if (columns == null) return this;
            _selectedColumnsQueue.Enqueue(ParseSelectMember(columns.Body));
            return this;
        }

        public IQueryBuilder<T> Select<TSource>(Expression<Func<TSource, object>> columns)
        {
            if (columns == null) return this;
            _selectedColumnsQueue.Enqueue(ParseSelectMember(columns.Body));
            return this;
        }

        public IQueryBuilder<T> Distinct() => SetClause(ref _distinctClause, "DISTINCT");

        public IQueryBuilder<T> Top(int count)
        {
            if (count <= 0) throw new ArgumentOutOfRangeException(nameof(count));
            return SetClause(ref _topClause, $"TOP ({count})");
        }

        public IQueryBuilder<T> Count() => SetFlag(ref _isCountQuery, true);

        public IQueryBuilder<T> OrderBy(string orderByClause)
        {
            if (string.IsNullOrEmpty(orderByClause)) return this;
            _orderByQueue.Enqueue(orderByClause);
            return this;
        }

        public IQueryBuilder<T> OrderByAscending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            foreach (var col in ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " ASC"))
                _orderByQueue.Enqueue(col);
            return this;
        }

        public IQueryBuilder<T> OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            foreach (var col in ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " DESC"))
                _orderByQueue.Enqueue(col);
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

        public IQueryBuilder<T> Sum(Expression<Func<T, object>> column, string alias = null) =>
            AddAggregate("SUM", column.Body, alias);

        public IQueryBuilder<T> Avg(Expression<Func<T, object>> column, string alias = null) =>
            AddAggregate("AVG", column.Body, alias);

        public IQueryBuilder<T> Min(Expression<Func<T, object>> column, string alias = null) =>
            AddAggregate("MIN", column.Body, alias);

        public IQueryBuilder<T> Max(Expression<Func<T, object>> column, string alias = null) =>
            AddAggregate("MAX", column.Body, alias);

        public IQueryBuilder<T> Count(Expression<Func<T, object>> column, string alias = null) =>
            AddAggregate("COUNT", column.Body, alias);

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

        public IQueryBuilder<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) =>
            AddJoin("INNER JOIN", onCondition);

        public IQueryBuilder<T> LeftJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) =>
            AddJoin("LEFT JOIN", onCondition);

        public IQueryBuilder<T> RightJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) =>
            AddJoin("RIGHT JOIN", onCondition);

        public IQueryBuilder<T> FullJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) =>
            AddJoin("FULL JOIN", onCondition);

        public IQueryBuilder<T> CustomJoin<TLeft, TRight>(string join, Expression<Func<TLeft, TRight, bool>> onCondition) =>
            AddJoin(join, onCondition);

        private IQueryBuilder<T> AddJoin<TLeft, TRight>(string joinType, Expression<Func<TLeft, TRight, bool>> onCondition)
        {
            var leftTableName = GetTableName(typeof(TLeft));
            var rightTableName = GetTableName(typeof(TRight));
            var leftAlias = _typeAliasMappings.GetOrAdd(typeof(TLeft), _ =>
            {
                var alias = GenerateAlias(leftTableName);
                _tableAliasMappings.TryAdd(leftTableName, alias);
                return alias;
            });
            string rightAlias;
            bool isSelfJoin = typeof(TLeft) == typeof(TRight);

            int joinCount = _joins.Count(j => j.TableName == rightTableName);
            bool isRepeatedJoin = joinCount > 0;
            if (isSelfJoin || isRepeatedJoin)
            {
                rightAlias = GenerateAlias(rightTableName);
                string uniqueKey = $"{rightTableName}_{joinCount + 1}";
                _tableAliasMappings.TryAdd(uniqueKey, rightAlias);

                // برای joinهای تکراری، یک نگاشت موقت ایجاد می‌کنیم
                // که بعداً در GetTableAliasForMember استفاده شود
                // ما نمی‌توانیم مستقیماً به _typeAliasMappings اضافه کنیم
                // زیرا کلید آن Type است و نمی‌تواند برای نمونه‌های تکراری استفاده شود
            }
            else
            {                if (_tableAliasMappings.TryGetValue(rightTableName, out rightAlias))
                {
                    _typeAliasMappings.AddOrUpdate(typeof(TRight), rightAlias, (k, v) => rightAlias);
                }
                else if (!_typeAliasMappings.TryGetValue(typeof(TRight), out rightAlias))
                {
                    rightAlias = GenerateAlias(rightTableName);
                    _typeAliasMappings.TryAdd(typeof(TRight), rightAlias);
                    _tableAliasMappings.TryAdd(rightTableName, rightAlias);
                }
            }
            var parameterAliases = new Dictionary<ParameterExpression, string>
    {
        { onCondition.Parameters[0], leftAlias },
        { onCondition.Parameters[1], rightAlias }
    };

            var parsed = ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);
            _joins.Enqueue(new JoinInfo
            {
                JoinType = joinType,
                TableName = rightTableName,
                Alias = rightAlias,
                OnCondition = parsed
            });

            return this;
        }
        public IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder) =>
            AddApply("CROSS APPLY", onCondition, subBuilder);

        public IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder) =>
            AddApply("OUTER APPLY", onCondition, subBuilder);

        private IQueryBuilder<T> AddApply<TSubQuery>(string applyType, Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
        {
            if (subBuilder == null) throw new ArgumentNullException(nameof(subBuilder));
            var sub = new QueryBuilder<TSubQuery>(_lazyConnection.Value);
            var qb = subBuilder(sub);
            if (qb == null) throw new InvalidOperationException("Sub-query builder returned null");
            var subQB = (QueryBuilder<TSubQuery>)qb;
            string subSql = subQB.BuildQuery();
            string subMainAlias = subQB._typeAliasMappings.GetOrAdd(typeof(TSubQuery), _ => subQB.GenerateAlias(subQB.GetTableName(typeof(TSubQuery))));

            var parameterAliases = new Dictionary<ParameterExpression, string>
            {
                { onCondition.Parameters[0], GetAliasForType(typeof(T)) },
                { onCondition.Parameters[1], subMainAlias }
            };

            var onSql = ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);

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
            MergeParameters(subQB._parameters);

            var applyAlias = GenerateSubQueryAlias(GetTableName(typeof(TSubQuery)));
            _subQueryTypeAliases.AddOrUpdate(typeof(TSubQuery), applyAlias, (k, v) => applyAlias);

            _applies.Enqueue(new ApplyInfo
            {
                ApplyType = applyType,
                SubQuery = $"({subSql}) AS {applyAlias}",
                SubQueryAlias = applyAlias
            });
            return this;
        }

        public IQueryBuilder<T> Union(IQueryBuilder<T> other) => AddSet("UNION", other);
        public IQueryBuilder<T> UnionAll(IQueryBuilder<T> other) => AddSet("UNION ALL", other);
        public IQueryBuilder<T> Intersect(IQueryBuilder<T> other) => AddSet("INTERSECT", other);
        public IQueryBuilder<T> Except(IQueryBuilder<T> other) => AddSet("EXCEPT", other);

        private IQueryBuilder<T> AddSet(string op, IQueryBuilder<T> other)
        {
            if (other == null) throw new ArgumentNullException(nameof(other));
            var sql = (other as QueryBuilder<T>)?.BuildQuery() ?? other.BuildQuery();
            var clause = $"{op} ({sql})";

            if (op.StartsWith("UNION")) _unionClauses.Enqueue(clause);
            else if (op == "INTERSECT") _intersectClauses.Enqueue(clause);
            else if (op == "EXCEPT") _exceptClauses.Enqueue(clause);
            if (other is QueryBuilder<T> otherQB)
                MergeParameters(otherQB._parameters);

            return this;
        }


        public IEnumerable<T> Execute() =>
            GetOpenConnection().Query<T>(BuildQuery(), _parameters, commandTimeout: _timeOut);

        public IEnumerable<TResult> Execute<TResult>() =>
            GetOpenConnection().Query<TResult>(BuildQuery(), _parameters, commandTimeout: _timeOut);

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
            string columns = _isCountQuery
                ? "COUNT(*) AS TotalCount"
                : !_selectedColumnsQueue.Any()
                    ? string.Join(", ", typeof(T).GetProperties().Select(p =>
                        $"{GetAliasForType(typeof(T))}.[{GetColumnName(p)}] AS {p.Name}"))
                    : string.Join(", ", _selectedColumnsQueue.ToArray());

            if (!string.IsNullOrEmpty(_rowNumberClause))
                columns = _rowNumberClause + ", " + columns;

            if (_aggregateColumns.Any())
                columns = string.Join(", ", _aggregateColumns.ToArray()) +
                         (string.IsNullOrEmpty(columns) ? string.Empty : ", " + columns);

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

        private string BuildHavingClause() => !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : string.Empty;

        private string BuildOrderByClause() => _orderByQueue.Any() ? "ORDER BY " + string.Join(", ", _orderByQueue.ToArray()) : string.Empty;

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

        #region Parsing

        private List<string> ExtractColumnList(Expression expr)
        {
            var list = new List<string>();
            var unary = expr as UnaryExpression;
            if (unary != null)
                expr = unary.Operand;

            var newExpr = expr as NewExpression;
            if (newExpr != null)
                foreach (var a in newExpr.Arguments)
                    list.Add(ParseMember(a));
            else
                list.Add(ParseMember(expr));

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
                foreach (var a in newExpr.Arguments)
                    list.Add(ParseMemberWithBrackets(a));
            else
                list.Add(ParseMemberWithBrackets(expr));

            return list;
        }

        private string ParseExpression(Expression expression) =>
            ParseExpressionInternal(expression, useBrackets: false, parameterAliases: null);

        private string ParseExpressionWithBrackets(Expression expression) =>
            ParseExpressionInternal(expression, useBrackets: true, parameterAliases: null);

        private string ParseExpressionWithParameterMapping(Expression expression, Dictionary<ParameterExpression, string> parameterAliases) =>
            ParseExpressionInternal(expression, useBrackets: false, parameterAliases: parameterAliases);

        private string ParseExpressionWithParameterMappingAndBrackets(Expression expression, Dictionary<ParameterExpression, string> parameterAliases) =>
            ParseExpressionInternal(expression, useBrackets: true, parameterAliases: parameterAliases);

        private string ParseExpressionInternal(Expression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    return useBrackets
                        ? HandleEqualWithBrackets((BinaryExpression)expression, parameterAliases)
                        : HandleEqual((BinaryExpression)expression, parameterAliases);
                case ExpressionType.NotEqual:
                    return useBrackets
                        ? HandleNotEqualWithBrackets((BinaryExpression)expression, parameterAliases)
                        : HandleNotEqual((BinaryExpression)expression, parameterAliases);
                case ExpressionType.GreaterThan:
                case ExpressionType.LessThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThanOrEqual:
                    var op = GetOperator(expression.NodeType);
                    return useBrackets
                        ? HandleBinaryWithBrackets((BinaryExpression)expression, op, parameterAliases)
                        : HandleBinary((BinaryExpression)expression, op, parameterAliases);
                case ExpressionType.AndAlso:
                    return "(" + ParseExpressionInternal(((BinaryExpression)expression).Left, useBrackets, parameterAliases) +
                           " AND " + ParseExpressionInternal(((BinaryExpression)expression).Right, useBrackets, parameterAliases) + ")";
                case ExpressionType.OrElse:
                    return "(" + ParseExpressionInternal(((BinaryExpression)expression).Left, useBrackets, parameterAliases) +
                           " OR " + ParseExpressionInternal(((BinaryExpression)expression).Right, useBrackets, parameterAliases) + ")";
                case ExpressionType.Not:
                    return "NOT (" + ParseExpressionInternal(((UnaryExpression)expression).Operand, useBrackets, parameterAliases) + ")";
                case ExpressionType.Call:
                    return useBrackets
                        ? HandleMethodCallWithBrackets((MethodCallExpression)expression, parameterAliases)
                        : HandleMethodCall((MethodCallExpression)expression, parameterAliases);
                case ExpressionType.MemberAccess:
                    return useBrackets
                        ? ParseMemberWithBrackets((MemberExpression)expression, parameterAliases)
                        : ParseMember((MemberExpression)expression, parameterAliases);
                case ExpressionType.Constant:
                    return HandleConstant((ConstantExpression)expression);
                case ExpressionType.Convert:
                    return ParseExpressionInternal(((UnaryExpression)expression).Operand, useBrackets, parameterAliases);
                case ExpressionType.Parameter:
                    var paramExpr = (ParameterExpression)expression;
                    if (parameterAliases != null && parameterAliases.TryGetValue(paramExpr, out var alias))
                        return alias;
                    return GetAliasForType(paramExpr.Type);
                default:
                    throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported.");
            }
        }

        private string HandleEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (IsNullConstant(expression.Right))
                return ParseMember(expression.Left, parameterAliases) + " IS NULL";
            return HandleBinary(expression, "=", parameterAliases);
        }

        private string HandleEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (IsNullConstant(expression.Right))
                return ParseMemberWithBrackets(expression.Left, parameterAliases) + " IS NULL";
            return HandleBinaryWithBrackets(expression, "=", parameterAliases);
        }

        private string HandleNotEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (IsNullConstant(expression.Right))
                return ParseMember(expression.Left, parameterAliases) + " IS NOT NULL";
            return HandleBinary(expression, "<>", parameterAliases);
        }

        private string HandleNotEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (IsNullConstant(expression.Right))
                return ParseMemberWithBrackets(expression.Left, parameterAliases) + " IS NOT NULL";
            return HandleBinaryWithBrackets(expression, "<>", parameterAliases);
        }

        private string HandleBinary(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            var left = ParseMember(expression.Left, parameterAliases);
            var right = ParseValue(expression.Right, parameterAliases: parameterAliases);
            return left + " " + op + " " + right;
        }

        private string HandleBinaryWithBrackets(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases = null)
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

        private string HandleMethodCall(MethodCallExpression m, Dictionary<ParameterExpression, string> parameterAliases = null)
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

        private string HandleMethodCallWithBrackets(MethodCallExpression m, Dictionary<ParameterExpression, string> parameterAliases = null)
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

        private string HandleLike(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (m.Object == null)
                throw new NotSupportedException("LIKE requires instance method call on string.");
            var prop = ParseMember(m.Object, parameterAliases);
            var val = ParseValue(m.Arguments[0], format, parameterAliases);
            return prop + " LIKE " + val;
        }

        private string HandleLikeWithBrackets(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (m.Object == null)
                throw new NotSupportedException("LIKE requires instance method call on string.");
            var prop = ParseMemberWithBrackets(m.Object, parameterAliases);
            var val = ParseValue(m.Arguments[0], format, parameterAliases);
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
                var tableAlias = GetTableAliasForMember(member);
                var columnName = member.Member.Name;
                if (IsSubqueryAlias(tableAlias))
                    return $"{tableAlias}.{columnName} AS {columnName}";
                return $"{tableAlias}.[{GetColumnName(member.Member as PropertyInfo)}] AS {columnName}";
            }
            var newExpr = expression as NewExpression;
            if (newExpr != null)
            {
                var columns = new List<string>();
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var memberArg = arg as MemberExpression;
                    if (memberArg != null)
                    {
                        var tableAlias = GetTableAliasForMember(memberArg);
                        var columnName = memberArg.Member.Name;
                        if (IsSubqueryAlias(tableAlias))
                            columns.Add($"{tableAlias}.{columnName} AS {newExpr.Members[i].Name}");
                        else
                            columns.Add($"{tableAlias}.[{GetColumnName(memberArg.Member as PropertyInfo)}] AS {newExpr.Members[i].Name}");
                    }
                    else
                    {
                        columns.Add(ParseMemberWithBrackets(arg));
                    }
                }
                return string.Join(", ", columns);
            }

            return ParseMemberWithBrackets(expression);
        }
        private string ParseMember(Expression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            var unary = expression as UnaryExpression;
            if (unary != null)
            {
                return ParseMember(unary.Operand, parameterAliases);
            }

            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Expression != null)
                {
                    var tableAlias = GetTableAliasForMember(member, parameterAliases);
                    var columnName = member.Member.Name;

                    if (IsSubqueryAlias(tableAlias))
                        return $"{tableAlias}.{columnName}";

                    return $"{tableAlias}.[{GetColumnName(member.Member as PropertyInfo)}]";
                }
                return $"[{GetColumnName(member.Member as PropertyInfo)}]";
            }

            var parameter = expression as ParameterExpression;
            if (parameter != null)
            {
                if (parameterAliases != null && parameterAliases.TryGetValue(parameter, out var alias))
                    return alias;
                return GetAliasForType(parameter.Type);
            }

            throw new NotSupportedException("Unsupported expression: " + expression);
        }

        private string ParseMemberWithBrackets(Expression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            var unary = expression as UnaryExpression;
            if (unary != null)
            {
                return ParseMemberWithBrackets(unary.Operand, parameterAliases);
            }

            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Expression != null)
                {
                    var tableAlias = GetTableAliasForMember(member, parameterAliases);
                    var columnName = member.Member.Name;

                    if (IsSubqueryAlias(tableAlias))
                        return $"{tableAlias}.{columnName}";
                    return $"{tableAlias}.[{GetColumnName(member.Member as PropertyInfo)}]";
                }
                return $"[{GetColumnName(member.Member as PropertyInfo)}]";
            }

            var newExpr = expression as NewExpression;
            if (newExpr != null)
            {
                var columns = new List<string>();
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var memberArg = arg as MemberExpression;
                    if (memberArg != null)
                    {
                        var tableAlias = GetTableAliasForMember(memberArg, parameterAliases);
                        var columnName = memberArg.Member.Name;
                        if (IsSubqueryAlias(tableAlias))
                            columns.Add($"{tableAlias}.{columnName} AS {newExpr.Members[i].Name}");
                        else
                            columns.Add($"{tableAlias}.[{GetColumnName(memberArg.Member as PropertyInfo)}] AS {newExpr.Members[i].Name}");
                    }
                    else
                    {
                        columns.Add(ParseMemberWithBrackets(arg, parameterAliases));
                    }
                }
                return string.Join(", ", columns);
            }

            var parameter = expression as ParameterExpression;
            if (parameter != null)
            {
                if (parameterAliases != null && parameterAliases.TryGetValue(parameter, out var alias))
                    return alias;
                if (_subQueryTypeAliases.TryGetValue(parameter.Type, out var subQueryAlias))
                    return subQueryAlias;
                return GetAliasForType(parameter.Type);
            }

            throw new NotSupportedException("Unsupported expression: " + expression);
        }

        private string GetTableAliasForMember(MemberExpression member, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (member.Expression is ParameterExpression paramExpr)
            {
                if (parameterAliases != null && parameterAliases.TryGetValue(paramExpr, out var alias))
                    return alias;

                // ابتدا بررسی می‌کنیم که آیا این نوع یک subquery است (مثلاً از APPLY)
                if (_subQueryTypeAliases.TryGetValue(paramExpr.Type, out var subQueryAlias))
                    return subQueryAlias;

                // برای joinهای تکراری، باید alias مناسب را پیدا کنیم
                var memberType = member.Member.DeclaringType;

                // ابتدا سعی می‌کنیم alias اصلی را پیدا کنیم
                if (_typeAliasMappings.TryGetValue(memberType, out var mainAlias))
                    return mainAlias;

                // اگر alias اصلی وجود نداشت، به دنبال aliasهای منحصر به فرد می‌گردیم
                var tableName = GetTableName(memberType);
                foreach (var mapping in _tableAliasMappings)
                {
                    if (mapping.Key.StartsWith(tableName + "_"))
                        return mapping.Value;
                }

                // اگر چیزی پیدا نکردیم، از alias پیش‌فرض استفاده می‌کنیم
                return GetAliasForType(memberType);
            }

            if (member.Expression is MemberExpression nestedMember)
            {
                return GetTableAliasForMember(nestedMember, parameterAliases);
            }

            if (member.Expression != null)
            {
                return ParseMemberWithBrackets(member.Expression, parameterAliases);
            }
            return null;
        }
        private bool IsSubqueryAlias(string alias)
        {
            if (string.IsNullOrEmpty(alias)) return false;
            return _subQueryTypeAliases.Values.Contains(alias);
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

        #region Helper Methods

        private string GenerateAlias(string tableName)
        {
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 10) shortName = shortName.Substring(0, 10);
            var counter = Interlocked.Increment(ref _aliasCounter);
            return $"{shortName}_A{counter}";
        }

        private string GenerateSubQueryAlias(string tableName)
        {
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 8) shortName = shortName.Substring(0, 8);
            var counter = Interlocked.Increment(ref _subQueryCounter);
            return $"{shortName}_SQ{counter}";
        }

        private void MergeParameters(ConcurrentDictionary<string, object> sourceParameters)
        {
            if (sourceParameters == null) return;
            foreach (var kv in sourceParameters)
            {
                var newName = kv.Key;
                if (_parameters.ContainsKey(newName))
                {
                    var fresh = GetUniqueParameterName();
                    newName = fresh;
                }
                _parameters.TryAdd(newName, kv.Value);
            }
        }

        private int FindWhereInsertPosition(string sql)
        {
            int fromIndex = sql.IndexOf("FROM ", StringComparison.OrdinalIgnoreCase);
            if (fromIndex == -1) return 0;
            int currentPos = fromIndex;
            while (true)
            {
                int nextWhere = sql.IndexOf(" WHERE ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextGroupBy = sql.IndexOf(" GROUP BY ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextOrderBy = sql.IndexOf(" ORDER BY ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextHaving = sql.IndexOf(" HAVING ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextJoin = sql.IndexOf(" JOIN ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextInnerJoin = sql.IndexOf(" INNER JOIN ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextLeftJoin = sql.IndexOf(" LEFT JOIN ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextRightJoin = sql.IndexOf(" RIGHT JOIN ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextFullJoin = sql.IndexOf(" FULL JOIN ", currentPos, StringComparison.OrdinalIgnoreCase);
                int nextCrossJoin = sql.IndexOf(" CROSS JOIN ", currentPos, StringComparison.OrdinalIgnoreCase);
                int[] positions = { nextWhere, nextGroupBy, nextOrderBy, nextHaving, nextJoin, nextInnerJoin,
                                   nextLeftJoin, nextRightJoin, nextFullJoin, nextCrossJoin };
                int minPos = positions.Where(p => p != -1).DefaultIfEmpty(sql.Length).Min();

                if (minPos == sql.Length)
                    return sql.Length;

                if (minPos == nextJoin || minPos == nextInnerJoin || minPos == nextLeftJoin ||
                    minPos == nextRightJoin || minPos == nextFullJoin || minPos == nextCrossJoin)
                {
                    int onIndex = sql.IndexOf("ON ", minPos, StringComparison.OrdinalIgnoreCase);
                    if (onIndex != -1)
                    {
                        int onEnd = FindNextClausePosition(sql, onIndex + 3);
                        if (onEnd == -1)
                            currentPos = sql.Length;
                        else
                            currentPos = onEnd;
                        continue;
                    }
                    else
                    {
                        currentPos = minPos + 4; 
                        continue;
                    }
                }
                else
                {
                    return minPos;
                }
            }
        }

        private int FindNextClausePosition(string sql, int startIndex)
        {
            int[] positions = {
                sql.IndexOf("GROUP BY ", startIndex, StringComparison.OrdinalIgnoreCase),
                sql.IndexOf("ORDER BY ", startIndex, StringComparison.OrdinalIgnoreCase),
                sql.IndexOf("HAVING ", startIndex, StringComparison.OrdinalIgnoreCase)
            };
            return positions.Where(p => p >= startIndex).DefaultIfEmpty(-1).Min();
        }

        private IQueryBuilder<T> SetClause(ref string clauseField, string value)
        {
            clauseField = value;
            return this;
        }

        private IQueryBuilder<T> SetFlag(ref bool flagField, bool value)
        {
            flagField = value;
            return this;
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
            if (property == null)
                throw new ArgumentNullException(nameof(property), "PropertyInfo cannot be null.");

            return ColumnNameCache.GetOrAdd(property, p =>
            {
                var column = p.GetCustomAttribute<ColumnAttribute>();
                return column != null ? column.ColumnName : p.Name;
            });
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
            if (_lazyConnection.IsValueCreated && _lazyConnection.Value.State != ConnectionState.Closed)
                _lazyConnection.Value.Close();
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
            public string SubQueryAlias { get; set; }
        }

        #endregion
    }
}
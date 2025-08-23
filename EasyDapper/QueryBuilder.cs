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
    // بازنویسی کامل با اعمال نکات: عدم Dispose کانکشن، بهبود Thread-Safety، نام‌گذاری پارامتر مستقل،
    // پشتیبانی کامل از Join/Apply/GroupBy/Aggregates/Row_Number/OrderBy/Union... و اصلاح IN/LIKE
    internal sealed class QueryBuilder<T> : IQueryBuilder<T>, IDisposable
    {
        private readonly Lazy<IDbConnection> _lazyConnection;

        // وضعیت داخلی
        private readonly List<string> _filters = new List<string>();
        private readonly Dictionary<string, object> _parameters = new Dictionary<string, object>();
        private readonly List<JoinInfo> _joins = new List<JoinInfo>();
        private readonly List<ApplyInfo> _applies = new List<ApplyInfo>();
        private readonly List<string> _aggregateColumns = new List<string>();
        private readonly List<string> _groupByColumns = new List<string>();
        private readonly List<string> _unionClauses = new List<string>();
        private readonly List<string> _intersectClauses = new List<string>();
        private readonly List<string> _exceptClauses = new List<string>();

        // نقشه‌های Alias
        private readonly Dictionary<string, string> _tableAliasMappings = new Dictionary<string, string>();
        private readonly Dictionary<Type, string> _typeAliasMappings = new Dictionary<Type, string>();

        // سایر حالت‌ها
        private string _orderByClause = string.Empty;
        private int? _limit = null;
        private int? _offset = null;
        private bool _isCountQuery = false;
        private string _selectedColumns = string.Empty;
        private string _rowNumberClause = string.Empty;
        private string _havingClause = string.Empty;
        private int _timeOut;
        private string _distinctClause = string.Empty;
        private string _topClause = string.Empty;
        private readonly string _defaultSchema = "dbo";

        // شمارنده‌ها/قفل
        private int _aliasCounter = 1;
        private int _paramCounter = 0;
        private readonly object _lockObject = new object();
        private bool _disposed = false;

        // کش‌ها
        private static readonly ConcurrentDictionary<Type, string> TableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<MemberInfo, string> ColumnNameCache = new ConcurrentDictionary<MemberInfo, string>();

        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection), "Connection cannot be null.");

            _lazyConnection = new Lazy<IDbConnection>(() => connection);
            _timeOut = _lazyConnection.Value.ConnectionTimeout;

            // ثبت alias برای جدول اصلی
            var mainTableName = GetTableName(typeof(T));
            var mainAlias = GenerateAlias(mainTableName);
            _tableAliasMappings[mainTableName] = mainAlias;
            _typeAliasMappings[typeof(T)] = mainAlias;
        }

        #region Public API - Selection/Filter
        public IQueryBuilder<T> WithTableAlias(string tableName, string customAlias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(customAlias))
                throw new ArgumentNullException("Table name and alias cannot be null or empty.");

            lock (_lockObject)
            {
                _tableAliasMappings[tableName] = customAlias;
            }
            return this;
        }

        public IQueryBuilder<T> Where(Expression<Func<T, bool>> filter)
        {
            if (filter == null) throw new ArgumentNullException(nameof(filter));
            var expression = ParseExpression(filter.Body);
            lock (_lockObject) { _filters.Add(expression); }
            return this;
        }

        public IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns)
        {
            var selected = new List<string>();
            foreach (var c in columns)
                selected.Add(ParseMember(c.Body));

            lock (_lockObject)
            {
                if (!string.IsNullOrEmpty(_selectedColumns) && selected.Count > 0)
                    _selectedColumns += ", ";
                _selectedColumns += string.Join(", ", selected);
            }
            return this;
        }

        public IQueryBuilder<T> Select<TSource>(params Expression<Func<TSource, object>>[] columns)
        {
            var selected = new List<string>();
            foreach (var c in columns)
                selected.Add(ParseMember(c.Body));

            lock (_lockObject)
            {
                if (!string.IsNullOrEmpty(_selectedColumns) && selected.Count > 0)
                    _selectedColumns += ", ";
                _selectedColumns += string.Join(", ", selected);
            }
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
            _orderByClause = orderByClause;
            return this;
        }

        public IQueryBuilder<T> OrderByAscending(Expression<Func<T, object>> keySelector)
            => OrderBy(keySelector, "ASC");

        public IQueryBuilder<T> OrderByDescending(Expression<Func<T, object>> keySelector)
            => OrderBy(keySelector, "DESC");

        public IQueryBuilder<T> Paging(int pageSize, int pageNumber = 1)
        {
            if (pageSize <= 0) throw new ArgumentException("Page size must be greater than zero.");
            if (pageNumber <= 0) throw new ArgumentException("Page number must be greater than zero.");

            _limit = pageSize;
            _offset = (pageNumber - 1) * pageSize;
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
            var parsed = ParseMember(expr);
            var agg = string.IsNullOrEmpty(alias) ? $"{fn}({parsed})" : $"{fn}({parsed}) AS {alias}";
            lock (_lockObject) { _aggregateColumns.Add(agg); }
            return this;
        }

        public IQueryBuilder<T> GroupBy(params Expression<Func<T, object>>[] groupByColumns)
        {
            foreach (var column in groupByColumns)
            {
                var parsed = ParseMember(column.Body);
                lock (_lockObject) { _groupByColumns.Add(parsed); }
            }
            return this;
        }

        public IQueryBuilder<T> Having(Expression<Func<T, bool>> havingCondition)
        {
            _havingClause = ParseExpression(havingCondition.Body);
            return this;
        }

        public IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber")
        {
            if (partitionBy == null) throw new ArgumentNullException(nameof(partitionBy));
            if (orderBy == null) throw new ArgumentNullException(nameof(orderBy));

            var parts = ExtractColumnList(partitionBy.Body);
            var orders = ExtractColumnList(orderBy.Body);

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

            // alias دهی امن
            lock (_lockObject)
            {
                if (!_tableAliasMappings.ContainsKey(leftTableName))
                {
                    var leftAlias = GenerateAlias(leftTableName);
                    _tableAliasMappings[leftTableName] = leftAlias;
                    _typeAliasMappings[typeof(TLeft)] = leftAlias;
                }
                if (!_tableAliasMappings.ContainsKey(rightTableName))
                {
                    var rightAlias = GenerateAlias(rightTableName);
                    _tableAliasMappings[rightTableName] = rightAlias;
                    _typeAliasMappings[typeof(TRight)] = rightAlias;
                }
            }

            var parsed = ParseExpression(onCondition.Body);

            lock (_lockObject)
            {
                _joins.Add(new JoinInfo
                {
                    JoinType = joinType,
                    TableName = rightTableName,
                    Alias = _tableAliasMappings[rightTableName],
                    OnCondition = parsed
                });
            }
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

            // alias اصلی زیرپرس‌وجو
            string subMainAlias = subQB._typeAliasMappings.ContainsKey(typeof(TSubQuery))
                ? subQB._typeAliasMappings[typeof(TSubQuery)]
                : subQB.GenerateAlias(GetTableName(typeof(TSubQuery)));

            // نگاشت موقت برای پارس شرط همبستگی
            string prev;
            bool hadPrev = _typeAliasMappings.TryGetValue(typeof(TSubQuery), out prev);
            _typeAliasMappings[typeof(TSubQuery)] = subMainAlias;

            var onSql = ParseExpression(onCondition.Body);

            if (hadPrev) _typeAliasMappings[typeof(TSubQuery)] = prev; else _typeAliasMappings.Remove(typeof(TSubQuery));

            // الحاق شرط همبستگی به subSql
            if (Regex.IsMatch(subSql, @"WHERE", RegexOptions.IgnoreCase))
                subSql = Regex.Replace(subSql, @"WHERE", m => $"WHERE ({onSql}) AND", RegexOptions.IgnoreCase);
            else
                subSql += " WHERE " + onSql;

            // ادغام پارامترها با جلوگیری از برخورد نام
            foreach (var kv in subQB._parameters)
            {
                var newName = kv.Key;
                if (_parameters.ContainsKey(newName))
                {
                    var fresh = GetUniqueParameterName();
                    // جایگزینی امن نام پارامتر در subSql
                    subSql = Regex.Replace(subSql, $@"(?<![A-Za-z0-9_]){Regex.Escape(newName)}(?![A-Za-z0-9_])", fresh);
                    newName = fresh;
                }
                _parameters[newName] = kv.Value;
            }

            var applyAlias = GenerateAlias(GetTableName(typeof(TSubQuery)));
            lock (_lockObject)
            {
                _applies.Add(new ApplyInfo
                {
                    ApplyType = applyType,
                    SubQuery = $"({subSql}) AS {applyAlias}"
                });
                _typeAliasMappings[typeof(TSubQuery)] = applyAlias;
            }
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
            if (isIntersect) _intersectClauses.Add(clause);
            else if (isExcept) _exceptClauses.Add(clause);
            else _unionClauses.Add(clause);
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
            return await GetOpenConnection().QueryAsync<T>(query, _parameters, commandTimeout: _timeOut);
        }
        public async Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            var query = BuildQuery();
            return await GetOpenConnection().QueryAsync<TResult>(query, _parameters, commandTimeout: _timeOut);
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
            if (_aggregateColumns.Any() && _groupByColumns.Count == 0 && !string.IsNullOrEmpty(_selectedColumns))
                throw new InvalidOperationException("When using aggregate functions, either use GROUP BY or avoid selecting non-aggregate columns.");
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
                columns = _rowNumberClause + ", " + columns;

            if (_aggregateColumns.Any())
                columns = string.Join(", ", _aggregateColumns) + (string.IsNullOrEmpty(columns) ? string.Empty : ", " + columns);

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

        private string BuildWhereClause() => _filters.Count > 0 ? "WHERE " + string.Join(" AND ", _filters) : string.Empty;
        private string BuildGroupByClause() => _groupByColumns.Count > 0 ? "GROUP BY " + string.Join(", ", _groupByColumns) : string.Empty;
        private string BuildHavingClause() => !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : string.Empty;

        private string BuildOrderByClause()
        {
            return !string.IsNullOrEmpty(_orderByClause) ? "ORDER BY " + _orderByClause : string.Empty;
        }

        private string BuildPaginationClause(string orderByClause)
        {
            if (_limit.HasValue)
            {
                // SQL Server به ORDER BY نیاز دارد؛ اگر نبود، یک ORDER BY ثابت اضافه کن
                if (string.IsNullOrEmpty(orderByClause))
                {
                    _orderByClause = "(SELECT 1)"; // امن برای OFFSET
                    orderByClause = BuildOrderByClause();
                }
                return $"OFFSET {_offset} ROWS FETCH NEXT {_limit} ROWS ONLY";
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

        private IQueryBuilder<T> OrderBy(Expression<Func<T, object>> keySelector, string order)
        {
            if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));
            var columns = ExtractColumnList(keySelector.Body).Select(c => c + " " + order);
            _orderByClause = string.IsNullOrEmpty(_orderByClause)
                ? string.Join(", ", columns)
                : _orderByClause + ", " + string.Join(", ", columns);
            return this;
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
        private string HandleConstant(ConstantExpression expression)
        {
            var p = GetUniqueParameterName();
            _parameters[p] = expression.Value;
            return p;
        }

        private string HandleMethodCall(MethodCallExpression m)
        {
            // string.StartsWith/EndsWith/Contains
            if (m.Method.DeclaringType == typeof(string))
            {
                if (m.Method.Name == "StartsWith") return HandleLike(m, "{0}%");
                if (m.Method.Name == "EndsWith") return HandleLike(m, "%{0}");
                if (m.Method.Name == "Contains") return HandleLike(m, "%{0}%");
            }

            // IEnumerable.Contains(prop)  -->  prop IN (...)
            if (m.Method.Name == "Contains" && m.Arguments.Count == 1)
            {
                // pattern: list.Contains(x.Prop)
                var memberExpr = m.Arguments[0];
                var listObject = Evaluate(m.Object) as System.Collections.IEnumerable;
                if (listObject == null)
                    throw new NotSupportedException("Unsupported Contains signature or non-constant collection.");

                var memberSql = ParseMember(memberExpr);
                var items = new List<string>();
                foreach (var item in listObject)
                {
                    var p = GetUniqueParameterName();
                    _parameters[p] = item;
                    items.Add(p);
                }
                return memberSql + " IN (" + string.Join(",", items) + ")";
            }

            // Between(a,b,c)  => a BETWEEN b AND c  (متد کمکی کاربر)
            if (m.Method.Name == "Between" && m.Arguments.Count == 3)
            {
                var property = ParseMember(m.Arguments[0]);
                var lower = ParseValue(m.Arguments[1]);
                var upper = ParseValue(m.Arguments[2]);
                return property + " BETWEEN " + lower + " AND " + upper;
            }

            // string.IsNullOrEmpty(x.Prop)
            if (m.Method.Name == "IsNullOrEmpty" && m.Arguments.Count == 1 && m.Method.DeclaringType == typeof(string))
            {
                var prop = ParseMember(m.Arguments[0]);
                return "(" + prop + " IS NULL OR " + prop + " = '')";
            }

            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported.");
        }

        private string HandleLike(MethodCallExpression m, string format)
        {
            // x.Prop.StartsWith(value)  یا  value.StartsWith(x.Prop) پشتیبانی از اولی کافیست
            Expression propExpr;
            Expression valueExpr;
            if (m.Object != null)
            {
                propExpr = m.Object; // x.Prop
                valueExpr = m.Arguments[0];
            }
            else
            {
                // static-like not expected for string
                throw new NotSupportedException("LIKE requires instance method call on string.");
            }

            var prop = ParseMember(propExpr);
            var val = ParseValue(valueExpr, format);
            return prop + " LIKE " + val;
        }

        private string ParseMember(Expression expression)
        {
            var unary = expression as UnaryExpression; // (object)x.Prop
            if (unary != null)
                return ParseMember(unary.Operand);

            var member = expression as MemberExpression;
            if (member != null)
            {
                // اگر x.Prop باشد
                if (member.Expression != null)
                {
                    var alias = GetAliasForType(member.Expression.Type);
                    return alias + "." + GetColumnName(member.Member as PropertyInfo);
                }
                // مقدار ثابت/فیلد است
                return GetColumnName(member.Member as PropertyInfo);
            }

            var parameter = expression as ParameterExpression;
            if (parameter != null)
            {
                var alias = GetAliasForType(parameter.Type);
                return alias;
            }

            throw new NotSupportedException("Unsupported expression: " + expression);
        }

        private string ParseValue(Expression expression, string format = null)
        {
            var constant = expression as ConstantExpression;
            if (constant != null)
            {
                var p = GetUniqueParameterName();
                var v = constant.Value;
                if (format != null && v is string)
                    v = string.Format(format, v);
                _parameters[p] = v;
                return p;
            }

            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Expression != null && member.Expression.NodeType == ExpressionType.Constant)
                {
                    var value = GetValue(member);
                    var p = GetUniqueParameterName();
                    _parameters[p] = value;
                    return p;
                }
                return ParseMember(member);
            }

            var unary = expression as UnaryExpression;
            if (unary != null)
                return ParseValue(unary.Operand, format);

            var binary = expression as BinaryExpression;
            if (binary != null)
            {
                var left = ParseValue(binary.Left);
                var right = ParseValue(binary.Right);
                return left + " " + GetOperator(binary.NodeType) + " " + right;
            }

            // تلاش برای ارزیابی مستقیم (closure)
            var eval = Evaluate(expression);
            var p2 = GetUniqueParameterName();
            _parameters[p2] = eval;
            return p2;
        }

        private static object Evaluate(Expression expr)
        {
            // روش ساده و سازگار با C# 7.3
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

        private string GetOperator(ExpressionType nodeType)
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
                return "[" + (name ?? string.Empty).Replace("]", "]]") + "]";
            });
        }

        private string GenerateAlias(string tableName)
        {
            lock (_lockObject)
            {
                var shortName = tableName.Split('.').Last().Trim('[', ']');
                if (shortName.Length > 10) shortName = shortName.Substring(0, 10);
                return $"{shortName}_A{_aliasCounter++}";
            }
        }

        private string GetAliasForTable(string tableName)
        {
            string alias;
            if (_tableAliasMappings.TryGetValue(tableName, out alias))
                return alias;
            alias = GenerateAlias(tableName);
            _tableAliasMappings[tableName] = alias;
            return alias;
        }

        private string GetAliasForType(Type type)
        {
            string alias;
            if (_typeAliasMappings.TryGetValue(type, out alias))
                return alias;
            var table = GetTableName(type);
            return GetAliasForTable(table);
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
            // توجه: مدیریت کانکشن با سازنده نیست؛ Dispose عمداً کانکشن را Dispose نمی‌کند.
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

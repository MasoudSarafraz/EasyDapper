using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace EasyDapper
{
    /// <summary>
    /// Concrete implementation of <see cref="IQueryBuilder{T}"/>. Each instance owns its own
    /// <see cref="AliasManager"/>, <see cref="ParameterBuilder"/> and <see cref="ExpressionParser"/>
    /// so that builders can be composed (e.g. via UNION/INTERSECT) without alias or parameter
    /// name collisions.
    /// </summary>
    /// <typeparam name="T">The entity type that the FROM clause targets.</typeparam>
    internal sealed class QueryBuilder<T> : IQueryBuilder<T>, IDisposable
    {
        private readonly QueryBuilderCore<T> _core;
        private readonly AliasManager _aliasManager;
        private readonly ParameterBuilder _parameterBuilder;
        private readonly ExpressionParser _expressionParser;
        private bool _disposed = false;

        /// <summary>
        /// Creates a new builder bound to the supplied connection. Suitable for callers that
        /// construct a QueryBuilder directly (i.e. without going through DapperService). The
        /// resulting builder will NOT participate in any transaction.
        /// </summary>
        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection", "Connection cannot be null.");
            _aliasManager = new AliasManager();
            _parameterBuilder = new ParameterBuilder();
            _expressionParser = new ExpressionParser(_aliasManager, _parameterBuilder);
            _core = new QueryBuilderCore<T>(connection, _aliasManager, _parameterBuilder, _expressionParser,
                ownsConnection: false);
        }

        /// <summary>
        /// Creates a new builder bound to the supplied connection manager. The builder will
        /// participate in any transaction started on the manager and will use the manager's
        /// configured command timeout.
        /// </summary>
        internal QueryBuilder(ConnectionManager connectionManager, QueryCache queryCache)
        {
            if (connectionManager == null) throw new ArgumentNullException("connectionManager");
            _aliasManager = new AliasManager();
            _parameterBuilder = new ParameterBuilder();
            _expressionParser = new ExpressionParser(_aliasManager, _parameterBuilder);
            // The QueryBuilderCore's constructor will call GetOpenConnection() lazily via the
            // supplied ConnectionManager when Execute/ExecuteAsync is invoked, ensuring that
            // the connection is opened in the same critical section as the rest of the service.
            _core = new QueryBuilderCore<T>(connectionManager.GetOpenConnection(), _aliasManager,
                _parameterBuilder, _expressionParser, ownsConnection: false,
                connectionManager: connectionManager);
        }

        public IQueryBuilder<T> WithTableAlias(string tableName, string customAlias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(customAlias))
                throw new ArgumentNullException("Table name and alias cannot be null or empty.");
            _aliasManager.SetTableAlias(tableName, customAlias);
            return this;
        }

        public IQueryBuilder<T> WithTableAlias(Type tableType, string customAlias)
        {
            if (tableType == null) throw new ArgumentNullException("tableType");
            if (string.IsNullOrEmpty(customAlias)) throw new ArgumentNullException("customAlias");
            var tableName = QueryBuilderCache.GetTableName(tableType);
            return WithTableAlias(tableName, customAlias);
        }

        public IQueryBuilder<T> WithTableAlias<TTable>(string customAlias) => WithTableAlias(typeof(TTable), customAlias);

        public IQueryBuilder<T> Where(Expression<Func<T, bool>> filter) { _core.Where(filter); return this; }
        public IQueryBuilder<T> Select(params Expression<Func<T, object>>[] columns) { _core.Select(columns); return this; }
        public IQueryBuilder<T> Select<TSource>(params Expression<Func<TSource, object>>[] columns) { _core.Select<TSource>(columns); return this; }
        public IQueryBuilder<T> Select(Expression<Func<T, object>> columns) { _core.Select(columns); return this; }
        public IQueryBuilder<T> Select<TSource>(Expression<Func<TSource, object>> columns) { _core.Select(columns); return this; }

        public IQueryBuilder<T> Distinct() { _core.Distinct(); return this; }
        public IQueryBuilder<T> Top(int count) { _core.Top(count); return this; }
        public IQueryBuilder<T> Count() { _core.Count(); return this; }

        public IQueryBuilder<T> OrderBy(string orderByClause) { _core.OrderBy(orderByClause); return this; }
        public IQueryBuilder<T> OrderByAscending(Expression<Func<T, object>> keySelector) { _core.OrderByAscending(keySelector); return this; }
        public IQueryBuilder<T> OrderByDescending(Expression<Func<T, object>> keySelector) { _core.OrderByDescending(keySelector); return this; }
        public IQueryBuilder<T> Paging(int pageSize, int pageNumber = 1) { _core.Paging(pageSize, pageNumber); return this; }

        public IQueryBuilder<T> Sum(Expression<Func<T, object>> column, string alias = null) { _core.Sum(column, alias); return this; }
        public IQueryBuilder<T> Avg(Expression<Func<T, object>> column, string alias = null) { _core.Avg(column, alias); return this; }
        public IQueryBuilder<T> Min(Expression<Func<T, object>> column, string alias = null) { _core.Min(column, alias); return this; }
        public IQueryBuilder<T> Max(Expression<Func<T, object>> column, string alias = null) { _core.Max(column, alias); return this; }
        public IQueryBuilder<T> Count(Expression<Func<T, object>> column, string alias = null) { _core.Count(column, alias); return this; }

        public IQueryBuilder<T> GroupBy(params Expression<Func<T, object>>[] groupByColumns) { _core.GroupBy(groupByColumns); return this; }
        public IQueryBuilder<T> Having(Expression<Func<T, bool>> havingCondition) { _core.Having(havingCondition); return this; }
        public IQueryBuilder<T> Row_Number(Expression<Func<T, object>> partitionBy, Expression<Func<T, object>> orderBy, string alias = "RowNumber") { _core.Row_Number(partitionBy, orderBy, alias); return this; }

        public IQueryBuilder<T> InnerJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) { _core.InnerJoin<TLeft, TRight>(onCondition); return this; }
        public IQueryBuilder<T> LeftJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) { _core.LeftJoin<TLeft, TRight>(onCondition); return this; }
        public IQueryBuilder<T> RightJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) { _core.RightJoin<TLeft, TRight>(onCondition); return this; }
        public IQueryBuilder<T> FullJoin<TLeft, TRight>(Expression<Func<TLeft, TRight, bool>> onCondition) { _core.FullJoin<TLeft, TRight>(onCondition); return this; }
        public IQueryBuilder<T> CustomJoin<TLeft, TRight>(string join, Expression<Func<TLeft, TRight, bool>> onCondition) { _core.CustomJoin<TLeft, TRight>(join, onCondition); return this; }

        public IQueryBuilder<T> CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder) { _core.CrossApply<TSubQuery>(onCondition, subBuilder); return this; }
        public IQueryBuilder<T> OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder) { _core.OuterApply<TSubQuery>(onCondition, subBuilder); return this; }

        public IQueryBuilder<T> Union(IQueryBuilder<T> other) { _core.Union(other); return this; }
        public IQueryBuilder<T> UnionAll(IQueryBuilder<T> other) { _core.UnionAll(other); return this; }
        public IQueryBuilder<T> Intersect(IQueryBuilder<T> other) { _core.Intersect(other); return this; }
        public IQueryBuilder<T> Except(IQueryBuilder<T> other) { _core.Except(other); return this; }

        public IEnumerable<T> Execute() => _core.Execute();
        public IEnumerable<TResult> Execute<TResult>() => _core.Execute<TResult>();
        public Task<IEnumerable<T>> ExecuteAsync() => _core.ExecuteAsync();
        public Task<IEnumerable<TResult>> ExecuteAsync<TResult>() => _core.ExecuteAsync<TResult>();

        public string GetRawSql() => _core.GetRawSql();
        public string BuildQuery() => _core.BuildQuery();

        internal AliasManager GetAliasManager() => _aliasManager;
        internal ParameterBuilder GetParameterBuilder() => _parameterBuilder;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _core.Dispose();
        }
    }
}

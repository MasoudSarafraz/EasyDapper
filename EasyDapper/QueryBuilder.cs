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
    internal class LruCache<TKey, TValue>
    {
        private readonly int _capacity;
        private readonly ConcurrentDictionary<TKey, LinkedListNode<LruCacheItem>> _cacheMap;
        private readonly LinkedList<LruCacheItem> _lruList;
        public LruCache(int capacity)
        {
            if (capacity <= 0) throw new ArgumentOutOfRangeException("capacity");
            _capacity = capacity;
            _cacheMap = new ConcurrentDictionary<TKey, LinkedListNode<LruCacheItem>>();
            _lruList = new LinkedList<LruCacheItem>();
        }
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (valueFactory == null) throw new ArgumentNullException("valueFactory");
            while (true)
            {
                LinkedListNode<LruCacheItem> node;
                if (_cacheMap.TryGetValue(key, out node))
                {
                    lock (_lruList)
                    {
                        if (node.List == _lruList)
                        {
                            _lruList.Remove(node);
                            _lruList.AddFirst(node);
                            return node.Value.Value;
                        }
                    }
                }
                var value = valueFactory(key);
                var item = new LruCacheItem(key, value);
                var newNode = new LinkedListNode<LruCacheItem>(item);
                lock (_lruList)
                {
                    if (_cacheMap.TryAdd(key, newNode))
                    {
                        _lruList.AddFirst(newNode);
                        if (_lruList.Count > _capacity)
                        {
                            var last = _lruList.Last;
                            _lruList.RemoveLast();
                            LinkedListNode<LruCacheItem> removedNode;
                            _cacheMap.TryRemove(last.Value.Key, out removedNode);
                        }
                        return value;
                    }
                    else
                    {
                        if (_cacheMap.TryGetValue(key, out node))
                        {
                            _lruList.Remove(node);
                            _lruList.AddFirst(node);
                            return node.Value.Value;
                        }
                    }
                }
            }
        }
        private class LruCacheItem
        {
            public TKey Key;
            public TValue Value;
            public LruCacheItem(TKey k, TValue v)
            {
                Key = k;
                Value = v;
            }
        }
    }
    internal class ExpressionParser
    {
        private static readonly LruCache<Type, string> TableNameCache = new LruCache<Type, string>(500);
        private static readonly LruCache<MemberInfo, string> ColumnNameCache = new LruCache<MemberInfo, string>(3000);
        private readonly AliasManager _aliasManager;
        private readonly ParameterBuilder _parameterBuilder;
        private static readonly char[] InvalidIdentifierChars = new[] { ';', '-', '-', '/', '*', '\'', '"', '[', ']' };
        public ExpressionParser(AliasManager aliasManager, ParameterBuilder parameterBuilder)
        {
            if (aliasManager == null) throw new ArgumentNullException("aliasManager");
            if (parameterBuilder == null) throw new ArgumentNullException("parameterBuilder");
            _aliasManager = aliasManager;
            _parameterBuilder = parameterBuilder;
        }
        public string ParseExpression(Expression expression) => ParseExpressionInternal(expression, useBrackets: false, parameterAliases: null);
        public string ParseExpressionWithBrackets(Expression expression) => ParseExpressionInternal(expression, useBrackets: true, parameterAliases: null);
        public string ParseExpressionWithParameterMapping(Expression expression, Dictionary<ParameterExpression, string> parameterAliases) => ParseExpressionInternal(expression, useBrackets: false, parameterAliases: parameterAliases);
        public string ParseExpressionWithParameterMappingAndBrackets(Expression expression, Dictionary<ParameterExpression, string> parameterAliases) => ParseExpressionInternal(expression, useBrackets: true, parameterAliases: parameterAliases);
        private string ParseExpressionInternal(Expression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            switch (expression.NodeType)
            {
                case ExpressionType.Equal: return useBrackets ? HandleEqualWithBrackets((BinaryExpression)expression, parameterAliases) : HandleEqual((BinaryExpression)expression, parameterAliases);
                case ExpressionType.NotEqual: return useBrackets ? HandleNotEqualWithBrackets((BinaryExpression)expression, parameterAliases) : HandleNotEqual((BinaryExpression)expression, parameterAliases);
                case ExpressionType.GreaterThan:
                case ExpressionType.LessThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThanOrEqual:
                    var op = GetOperator(expression.NodeType);
                    return useBrackets ? HandleBinaryWithBrackets((BinaryExpression)expression, op, parameterAliases) : HandleBinary((BinaryExpression)expression, op, parameterAliases);
                case ExpressionType.AndAlso: return "(" + ParseExpressionInternal(((BinaryExpression)expression).Left, useBrackets, parameterAliases) + " AND " + ParseExpressionInternal(((BinaryExpression)expression).Right, useBrackets, parameterAliases) + ")";
                case ExpressionType.OrElse: return "(" + ParseExpressionInternal(((BinaryExpression)expression).Left, useBrackets, parameterAliases) + " OR " + ParseExpressionInternal(((BinaryExpression)expression).Right, useBrackets, parameterAliases) + ")";
                case ExpressionType.Not:
                    var operand = ((UnaryExpression)expression).Operand;
                    if (operand.NodeType == ExpressionType.Call)
                    {
                        var methodCall = (MethodCallExpression)operand;
                        if (methodCall.Method.Name == "IsNullOrEmpty" && methodCall.Method.DeclaringType == typeof(string))
                        {
                            var prop = useBrackets ? ParseMemberWithBrackets(methodCall.Arguments[0], parameterAliases) : ParseMember(methodCall.Arguments[0], parameterAliases);
                            return "(" + prop + " IS NOT NULL AND " + prop + " <> '')";
                        }
                    }
                    if (operand.NodeType == ExpressionType.MemberAccess)
                    {
                        var member = (MemberExpression)operand;
                        if (member.Type == typeof(bool) || member.Type == typeof(bool?))
                        {
                            var column = useBrackets ? ParseMemberWithBrackets(member, parameterAliases) : ParseMember(member, parameterAliases);
                            return member.Type == typeof(bool?) ? "(ISNULL(" + column + ", 0) = 0 OR " + column + " IS NULL)" : column + " = 0";
                        }
                    }
                    return "NOT (" + ParseExpressionInternal(operand, useBrackets, parameterAliases) + ")";
                case ExpressionType.Call: return useBrackets ? HandleMethodCallWithBrackets((MethodCallExpression)expression, parameterAliases) : HandleMethodCall((MethodCallExpression)expression, parameterAliases);
                case ExpressionType.MemberAccess:
                    var memberExpr = (MemberExpression)expression;
                    if (memberExpr.Member.Name == "HasValue" && memberExpr.Expression != null && memberExpr.Expression.Type.IsGenericType && memberExpr.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        var column = useBrackets ? ParseMemberWithBrackets(memberExpr.Expression, parameterAliases) : ParseMember(memberExpr.Expression, parameterAliases);
                        return column + " IS NOT NULL";
                    }
                    if (useBrackets && (memberExpr.Type == typeof(bool) || memberExpr.Type == typeof(bool?)))
                    {
                        var column = useBrackets ? ParseMemberWithBrackets(memberExpr, parameterAliases) : ParseMember(memberExpr, parameterAliases);
                        return memberExpr.Type == typeof(bool?) ? "ISNULL(" + column + ", 0) = 1" : column + " = 1";
                    }
                    return useBrackets ? ParseMemberWithBrackets(memberExpr, parameterAliases) : ParseMember(memberExpr, parameterAliases);
                case ExpressionType.Constant: return HandleConstant((ConstantExpression)expression);
                case ExpressionType.Convert: return ParseExpressionInternal(((UnaryExpression)expression).Operand, useBrackets, parameterAliases);
                case ExpressionType.Parameter:
                    var paramExpr = (ParameterExpression)expression;
                    if (parameterAliases != null && parameterAliases.TryGetValue(paramExpr, out var alias)) return alias;
                    return _aliasManager.GetAliasForType(paramExpr.Type);
                default: throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported");
            }
        }
        public string ParseSelectMember(Expression expression)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var unary = expression as UnaryExpression;
            if (unary != null) return ParseSelectMember(unary.Operand);
            var member = expression as MemberExpression;
            if (member != null)
            {
                var tableAlias = GetTableAliasForMember(member);
                var columnName = member.Member.Name;
                if (member.Member.Name == "Value" && member.Expression != null && member.Expression.Type.IsGenericType && member.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    return ParseSelectMember(member.Expression);
                }
                if (_aliasManager.IsSubqueryAlias(tableAlias)) return tableAlias + "." + columnName + " AS " + columnName;
                return tableAlias + ".[" + GetColumnName(member.Member as PropertyInfo) + "] AS " + columnName;
            }
            var newExpr = expression as NewExpression;
            if (newExpr != null)
            {
                var columns = new List<string>(newExpr.Arguments.Count);
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    var memberArg = arg as MemberExpression;
                    if (memberArg != null)
                    {
                        var tableAlias = GetTableAliasForMember(memberArg);
                        var columnName = memberArg.Member.Name;
                        if (memberArg.Member.Name == "Value" && memberArg.Expression != null && memberArg.Expression.Type.IsGenericType && memberArg.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            columns.Add(ParseSelectMember(memberArg.Expression));
                            continue;
                        }
                        if (_aliasManager.IsSubqueryAlias(tableAlias)) columns.Add(tableAlias + "." + columnName + " AS " + newExpr.Members[i].Name);
                        else columns.Add(tableAlias + ".[" + GetColumnName(memberArg.Member as PropertyInfo) + "] AS " + newExpr.Members[i].Name);
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
        public List<string> ExtractColumnList(Expression expr)
        {
            if (expr == null) throw new ArgumentNullException("expr");
            var list = new List<string>();
            var unary = expr as UnaryExpression;
            if (unary != null) expr = unary.Operand;
            var newExpr = expr as NewExpression;
            if (newExpr != null) foreach (var a in newExpr.Arguments) list.Add(ParseMember(a));
            else list.Add(ParseMember(expr));
            return list;
        }
        public List<string> ExtractColumnListWithBrackets(Expression expr)
        {
            if (expr == null) throw new ArgumentNullException("expr");
            var list = new List<string>();
            var unary = expr as UnaryExpression;
            if (unary != null) expr = unary.Operand;
            var newExpr = expr as NewExpression;
            if (newExpr != null) foreach (var a in newExpr.Arguments) list.Add(ParseMemberWithBrackets(a));
            else list.Add(ParseMemberWithBrackets(expr));
            return list;
        }
        public string ParseMemberWithBrackets(Expression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var unary = expression as UnaryExpression;
            if (unary != null) return ParseMemberWithBrackets(unary.Operand, parameterAliases);
            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Member.Name == "Value" && member.Expression != null && member.Expression.Type.IsGenericType && member.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    return ParseMemberWithBrackets(member.Expression, parameterAliases);
                }
                if (member.Expression != null)
                {
                    var tableAlias = GetTableAliasForMember(member, parameterAliases);
                    var columnName = member.Member.Name;
                    if (_aliasManager.IsSubqueryAlias(tableAlias)) return tableAlias + "." + columnName;
                    return tableAlias + ".[" + GetColumnName(member.Member as PropertyInfo) + "]";
                }
                return "[" + GetColumnName(member.Member as PropertyInfo) + "]";
            }
            var parameter = expression as ParameterExpression;
            if (parameter != null)
            {
                if (parameterAliases != null && parameterAliases.TryGetValue(parameter, out var alias)) return alias;
                if (_aliasManager.TryGetSubQueryAlias(parameter.Type, out var subQueryAlias)) return subQueryAlias;
                return _aliasManager.GetAliasForType(parameter.Type);
            }
            throw new NotSupportedException("Unsupported expression: " + expression);
        }
        private string HandleEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            if (expression.Left.NodeType == ExpressionType.MemberAccess)
            {
                var leftMember = (MemberExpression)expression.Left;
                if (leftMember.Type == typeof(bool) || leftMember.Type == typeof(bool?))
                {
                    var column = ParseMember(leftMember, parameterAliases);
                    if (expression.Right.NodeType == ExpressionType.Constant)
                    {
                        var rightConst = (ConstantExpression)expression.Right;
                        if (rightConst.Value is bool boolValue)
                        {
                            return leftMember.Type == typeof(bool?) ? "ISNULL(" + column + ", 0) = " + (boolValue ? 1 : 0) : column + " = " + (boolValue ? 1 : 0);
                        }
                    }
                }
            }
            if (IsNullConstant(expression.Right)) return ParseMember(expression.Left, parameterAliases) + " IS NULL";
            return HandleBinary(expression, "=", parameterAliases);
        }
        private string HandleEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            if (expression.Left.NodeType == ExpressionType.MemberAccess)
            {
                var leftMember = (MemberExpression)expression.Left;
                if (leftMember.Type == typeof(bool) || leftMember.Type == typeof(bool?))
                {
                    var column = ParseMemberWithBrackets(leftMember, parameterAliases);
                    if (expression.Right.NodeType == ExpressionType.Constant)
                    {
                        var rightConst = (ConstantExpression)expression.Right;
                        if (rightConst.Value is bool boolValue)
                        {
                            return leftMember.Type == typeof(bool?) ? "ISNULL(" + column + ", 0) = " + (boolValue ? 1 : 0) : column + " = " + (boolValue ? 1 : 0);
                        }
                    }
                }
            }
            if (IsNullConstant(expression.Right)) return ParseMemberWithBrackets(expression.Left, parameterAliases) + " IS NULL";
            return HandleBinaryWithBrackets(expression, "=", parameterAliases);
        }
        private string HandleNotEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            if (expression.Left.NodeType == ExpressionType.MemberAccess)
            {
                var leftMember = (MemberExpression)expression.Left;
                if (leftMember.Type == typeof(bool) || leftMember.Type == typeof(bool?))
                {
                    var column = ParseMember(leftMember, parameterAliases);
                    if (expression.Right.NodeType == ExpressionType.Constant)
                    {
                        var rightConst = (ConstantExpression)expression.Right;
                        if (rightConst.Value is bool boolValue)
                        {
                            return leftMember.Type == typeof(bool?) ? "ISNULL(" + column + ", 0) <> " + (boolValue ? 1 : 0) : column + " <> " + (boolValue ? 1 : 0);
                        }
                    }
                }
            }
            if (IsNullConstant(expression.Right)) return ParseMember(expression.Left, parameterAliases) + " IS NOT NULL";
            return HandleBinary(expression, "<>", parameterAliases);
        }
        private string HandleNotEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            if (expression.Left.NodeType == ExpressionType.MemberAccess)
            {
                var leftMember = (MemberExpression)expression.Left;
                if (leftMember.Type == typeof(bool) || leftMember.Type == typeof(bool?))
                {
                    var column = ParseMemberWithBrackets(leftMember, parameterAliases);
                    if (expression.Right.NodeType == ExpressionType.Constant)
                    {
                        var rightConst = (ConstantExpression)expression.Right;
                        if (rightConst.Value is bool boolValue)
                        {
                            return leftMember.Type == typeof(bool?) ? "ISNULL(" + column + ", 0) <> " + (boolValue ? 1 : 0) : column + " <> " + (boolValue ? 1 : 0);
                        }
                    }
                }
            }
            if (IsNullConstant(expression.Right)) return ParseMemberWithBrackets(expression.Left, parameterAliases) + " IS NOT NULL";
            return HandleBinaryWithBrackets(expression, "<>", parameterAliases);
        }
        private string HandleBinary(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var left = ParseMember(expression.Left, parameterAliases);
            var right = ParseValue(expression.Right, parameterAliases: parameterAliases);
            return left + " " + op + " " + right;
        }
        private string HandleBinaryWithBrackets(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var left = ParseMemberWithBrackets(expression.Left, parameterAliases);
            var right = ParseValue(expression.Right, parameterAliases: parameterAliases);
            return left + " " + op + " " + right;
        }
        private string HandleConstant(ConstantExpression expression)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var p = _parameterBuilder.GetUniqueParameterName();
            _parameterBuilder.AddParameter(p, expression.Value);
            return p;
        }
        private string HandleMethodCall(MethodCallExpression m, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (m == null) throw new ArgumentNullException("m");
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
                if (listObject == null) throw new NotSupportedException("Unsupported Contains signature or non-constant collection");
                var memberSql = ParseMember(memberExpr, parameterAliases);
                var items = new List<string>();
                foreach (var item in listObject)
                {
                    var p = _parameterBuilder.GetUniqueParameterName();
                    _parameterBuilder.AddParameter(p, item);
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
            if (m == null) throw new ArgumentNullException("m");
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
                if (listObject == null) throw new NotSupportedException("Unsupported Contains signature or non-constant collection");
                var memberSql = ParseMemberWithBrackets(memberExpr, parameterAliases);
                var items = new List<string>();
                foreach (var item in listObject)
                {
                    var p = _parameterBuilder.GetUniqueParameterName();
                    _parameterBuilder.AddParameter(p, item);
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
            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported");
        }
        private string HandleLike(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (m == null) throw new ArgumentNullException("m");
            if (m.Object == null) throw new NotSupportedException("LIKE requires instance method call on string");
            var prop = ParseMember(m.Object, parameterAliases);
            var val = ParseValue(m.Arguments[0], format, parameterAliases);
            return prop + " LIKE " + val;
        }
        private string HandleLikeWithBrackets(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (m == null) throw new ArgumentNullException("m");
            if (m.Object == null) throw new NotSupportedException("LIKE requires instance method call on string");
            var prop = ParseMemberWithBrackets(m.Object, parameterAliases);
            var val = ParseValue(m.Arguments[0], format, parameterAliases);
            return prop + " LIKE " + val;
        }
        private string ParseMember(Expression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var unary = expression as UnaryExpression;
            if (unary != null) return ParseMember(unary.Operand, parameterAliases);
            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Member.Name == "Value" && member.Expression != null && member.Expression.Type.IsGenericType && member.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    return ParseMember(member.Expression, parameterAliases);
                }
                if (member.Expression != null)
                {
                    var tableAlias = GetTableAliasForMember(member, parameterAliases);
                    var columnName = member.Member.Name;
                    if (_aliasManager.IsSubqueryAlias(tableAlias)) return tableAlias + "." + columnName;
                    return tableAlias + ".[" + GetColumnName(member.Member as PropertyInfo) + "]";
                }
                return "[" + GetColumnName(member.Member as PropertyInfo) + "]";
            }
            var parameter = expression as ParameterExpression;
            if (parameter != null)
            {
                if (parameterAliases != null && parameterAliases.TryGetValue(parameter, out var alias)) return alias;
                return _aliasManager.GetAliasForType(parameter.Type);
            }
            throw new NotSupportedException("Unsupported expression: " + expression);
        }
        private string GetTableAliasForMember(MemberExpression member, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (member == null) throw new ArgumentNullException("member");
            if (member.Expression is ParameterExpression paramExpr)
            {
                if (parameterAliases != null && parameterAliases.TryGetValue(paramExpr, out var alias)) return alias;
                if (_aliasManager.TryGetSubQueryAlias(paramExpr.Type, out var subQueryAlias)) return subQueryAlias;
                var memberType = member.Member.DeclaringType;
                if (_aliasManager.TryGetTypeAlias(memberType, out var mainAlias)) return mainAlias;
                var tableName = GetTableName(memberType);
                foreach (var mapping in _aliasManager.GetAllTableAliases())
                {
                    if (mapping.Key.StartsWith(tableName + "_")) return mapping.Value;
                }
                return _aliasManager.GetAliasForType(memberType);
            }
            if (member.Expression is MemberExpression nestedMember) return GetTableAliasForMember(nestedMember, parameterAliases);
            if (member.Expression != null) return ParseMemberWithBrackets(member.Expression, parameterAliases);
            return null;
        }
        private string ParseValue(Expression expression, string format = null, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var constant = expression as ConstantExpression;
            if (constant != null)
            {
                var p = _parameterBuilder.GetUniqueParameterName();
                var v = constant.Value;
                if (format != null && v is string) v = string.Format(format, v);
                _parameterBuilder.AddParameter(p, v);
                return p;
            }
            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Expression != null && member.Expression.NodeType == ExpressionType.Constant)
                {
                    var value = GetValue(member);
                    var p = _parameterBuilder.GetUniqueParameterName();
                    _parameterBuilder.AddParameter(p, value);
                    return p;
                }
                return ParseMember(member, parameterAliases);
            }
            var unary = expression as UnaryExpression;
            if (unary != null) return ParseValue(unary.Operand, format, parameterAliases);
            var binary = expression as BinaryExpression;
            if (binary != null)
            {
                var left = ParseValue(binary.Left, format, parameterAliases);
                var right = ParseValue(binary.Right, format, parameterAliases);
                return left + " " + GetOperator(binary.NodeType) + " " + right;
            }
            var eval = Evaluate(expression);
            var p2 = _parameterBuilder.GetUniqueParameterName();
            _parameterBuilder.AddParameter(p2, eval);
            return p2;
        }
        private static object Evaluate(Expression expr)
        {
            if (expr == null) return null;
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
            if (member == null) throw new ArgumentNullException("member");
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
            if (expression == null) return false;
            var c = expression as ConstantExpression;
            return c != null && c.Value == null;
        }
        private string GetTableName(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            return TableNameCache.GetOrAdd(type, t =>
            {
                var tableAttr = t.GetCustomAttribute<TableAttribute>();
                var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema) ? SanitizeIdentifier(tableAttr.Schema) : "dbo";
                var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName) ? SanitizeIdentifier(tableAttr.TableName) : SanitizeIdentifier(t.Name);
                return "[" + schema + "].[" + name + "]";
            });
        }
        private string GetColumnName(PropertyInfo property)
        {
            if (property == null) throw new ArgumentNullException("property", "PropertyInfo cannot be null");
            return ColumnNameCache.GetOrAdd(property, p =>
            {
                var column = p.GetCustomAttribute<ColumnAttribute>();
                return column != null && !string.IsNullOrWhiteSpace(column.ColumnName) ? SanitizeIdentifier(column.ColumnName) : SanitizeIdentifier(p.Name);
            });
        }
        private string SanitizeIdentifier(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier)) throw new ArgumentException("Identifier cannot be null or empty.", "identifier");
            if (identifier.IndexOfAny(InvalidIdentifierChars) >= 0) throw new ArgumentException("Identifier contains invalid characters.", "identifier");
            return identifier;
        }
    }
    internal class AliasManager
    {
        private readonly ConcurrentDictionary<string, string> _tableAliasMappings = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<Type, string> _typeAliasMappings = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<Type, string> _subQueryTypeAliases = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<string, string> _aliasSources = new ConcurrentDictionary<string, string>();
        private int _aliasCounter = 1;
        private int _subQueryCounter = 1;
        private readonly object _aliasLock = new object();
        public string GenerateAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            lock (_aliasLock)
            {
                var shortName = tableName.Split('.').Last().Trim('[', ']');
                if (shortName.Length > 10) shortName = shortName.Substring(0, 10);
                string alias;
                int attempts = 0;
                do
                {
                    var counter = Interlocked.Increment(ref _aliasCounter);
                    alias = string.Format("{0}_A{1}", shortName, counter);
                    attempts++;
                    if (attempts > 100)
                    {
                        alias = "T_" + Guid.NewGuid().ToString("N").Substring(0, 8);
                        break;
                    }
                }
                while (_aliasSources.ContainsKey(alias));
                _aliasSources.TryAdd(alias, "Generated for " + tableName);
                return alias;
            }
        }
        public string GenerateSubQueryAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            lock (_aliasLock)
            {
                var shortName = tableName.Split('.').Last().Trim('[', ']');
                if (shortName.Length > 8) shortName = shortName.Substring(0, 8);
                string alias;
                int attempts = 0;
                do
                {
                    var counter = Interlocked.Increment(ref _subQueryCounter);
                    alias = string.Format("{0}_SQ{1}", shortName, counter);
                    attempts++;
                    if (attempts > 100)
                    {
                        alias = "SQ_" + Guid.NewGuid().ToString("N").Substring(0, 8);
                        break;
                    }
                }
                while (_aliasSources.ContainsKey(alias));
                _aliasSources.TryAdd(alias, "Generated for subquery " + tableName);
                return alias;
            }
        }
        public void SetTableAlias(string tableName, string alias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(alias)) throw new ArgumentException("Table name and alias cannot be null or empty.");
            lock (_aliasLock)
            {
                if (_tableAliasMappings.TryGetValue(tableName, out var existingAlias) && existingAlias == alias) return;
                if (_tableAliasMappings.Any(x => x.Value == alias))
                {
                    var existingTable = _tableAliasMappings.First(x => x.Value == alias).Key;
                    if (existingTable != tableName) throw new InvalidOperationException("Alias '" + alias + "' is already used by table '" + existingTable + "'.");
                }
                if (_typeAliasMappings.Any(x => x.Value == alias))
                {
                    var existingType = _typeAliasMappings.First(x => x.Value == alias).Key;
                    var existingTypeTable = GetTableName(existingType);
                    if (existingTypeTable == tableName)
                    {
                        _aliasSources.AddOrUpdate(alias, "Table " + tableName, (k, v) => "Table " + tableName);
                        _tableAliasMappings.AddOrUpdate(tableName, alias, (k, v) => alias);
                        return;
                    }
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by type '" + existingType.Name + "'.");
                }
                if (_subQueryTypeAliases.Any(x => x.Value == alias))
                {
                    var existingType = _subQueryTypeAliases.First(x => x.Value == alias).Key;
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by subquery type '" + existingType.Name + "'.");
                }
                if (_aliasSources.TryGetValue(alias, out var source) && source == "Generated for " + tableName)
                {
                    _tableAliasMappings.AddOrUpdate(tableName, alias, (k, v) => alias);
                    _aliasSources.AddOrUpdate(alias, "Table " + tableName, (k, v) => "Table " + tableName);
                    return;
                }
                if (_aliasSources.ContainsKey(alias))
                {
                    var source1 = _aliasSources.TryGetValue(alias, out var src) ? src : "unknown source";
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by " + source1 + ".");
                }
                _tableAliasMappings.AddOrUpdate(tableName, alias, (k, v) => alias);
                _aliasSources.AddOrUpdate(alias, "Table " + tableName, (k, v) => "Table " + tableName);
            }
        }
        public void SetTypeAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias)) throw new ArgumentException("Type and alias cannot be null or empty.");
            lock (_aliasLock)
            {
                if (_typeAliasMappings.TryGetValue(type, out var existingAlias) && existingAlias == alias) return;
                var typeTableName = GetTableName(type);
                if (_tableAliasMappings.Any(x => x.Value == alias))
                {
                    var existingTable = _tableAliasMappings.First(x => x.Value == alias).Key;
                    if (existingTable == typeTableName)
                    {
                        _aliasSources.AddOrUpdate(alias, "Type " + type.Name, (k, v) => "Type " + type.Name);
                        _typeAliasMappings.AddOrUpdate(type, alias, (k, v) => alias);
                        return;
                    }
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by table '" + existingTable + "'.");
                }
                if (_typeAliasMappings.Any(x => x.Value == alias))
                {
                    var existingType = _typeAliasMappings.First(x => x.Value == alias).Key;
                    if (existingType != type) throw new InvalidOperationException("Alias '" + alias + "' is already used by type '" + existingType.Name + "'.");
                }
                if (_subQueryTypeAliases.Any(x => x.Value == alias))
                {
                    var existingType = _subQueryTypeAliases.First(x => x.Value == alias).Key;
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by subquery type '" + existingType.Name + "'.");
                }
                if (_aliasSources.TryGetValue(alias, out var source) && source == "Generated for " + typeTableName)
                {
                    _typeAliasMappings.AddOrUpdate(type, alias, (k, v) => alias);
                    _aliasSources.AddOrUpdate(alias, "Type " + type.Name, (k, v) => "Type " + type.Name);
                    return;
                }
                if (_aliasSources.ContainsKey(alias))
                {
                    var source1 = _aliasSources.TryGetValue(alias, out var src) ? src : "unknown source";
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by " + source1 + ".");
                }
                _typeAliasMappings.AddOrUpdate(type, alias, (k, v) => alias);
                _aliasSources.AddOrUpdate(alias, "Type " + type.Name, (k, v) => "Type " + type.Name);
            }
        }
        public void SetSubQueryAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias)) throw new ArgumentException("Type and alias cannot be null or empty.");
            lock (_aliasLock)
            {
                if (_subQueryTypeAliases.TryGetValue(type, out var existingAlias) && existingAlias == alias) return;
                var typeTableName = GetTableName(type);
                if (_tableAliasMappings.Any(x => x.Value == alias))
                {
                    var existingTable = _tableAliasMappings.First(x => x.Value == alias).Key;
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by table '" + existingTable + "'.");
                }
                if (_typeAliasMappings.Any(x => x.Value == alias))
                {
                    var existingType = _typeAliasMappings.First(x => x.Value == alias).Key;
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by type '" + existingType.Name + "'.");
                }
                if (_subQueryTypeAliases.Any(x => x.Value == alias))
                {
                    var existingType = _subQueryTypeAliases.First(x => x.Value == alias).Key;
                    if (existingType != type) throw new InvalidOperationException("Alias '" + alias + "' is already used by subquery type '" + existingType.Name + "'.");
                }
                if (_aliasSources.TryGetValue(alias, out var source) && source == "Generated for subquery " + typeTableName)
                {
                    _subQueryTypeAliases.AddOrUpdate(type, alias, (k, v) => alias);
                    _aliasSources.AddOrUpdate(alias, "SubQuery type " + type.Name, (k, v) => "SubQuery type " + type.Name);
                    return;
                }
                if (_aliasSources.ContainsKey(alias))
                {
                    var source1 = _aliasSources.TryGetValue(alias, out var src) ? src : "unknown source";
                    throw new InvalidOperationException("Alias '" + alias + "' is already used by " + source1 + ".");
                }
                _subQueryTypeAliases.AddOrUpdate(type, alias, (k, v) => alias);
                _aliasSources.AddOrUpdate(alias, "SubQuery type " + type.Name, (k, v) => "SubQuery type " + type.Name);
            }
        }
        public string GetAliasForTable(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            if (_tableAliasMappings.TryGetValue(tableName, out var alias)) return alias;
            var newAlias = GenerateAlias(tableName);
            SetTableAlias(tableName, newAlias);
            return newAlias;
        }
        public string GetAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            if (_typeAliasMappings.TryGetValue(type, out var alias)) return alias;
            var table = GetTableName(type);
            return GetAliasForTable(table);
        }
        public string GetUniqueAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var tableName = GetTableName(type);
            var newAlias = GenerateAlias(tableName);
            _aliasSources.AddOrUpdate(newAlias, "Type " + type.Name + " (duplicate)", (k, v) => "Type " + type.Name + " (duplicate)");
            return newAlias;
        }
        public bool TryGetTypeAlias(Type type, out string alias)
        {
            if (type == null) { alias = null; return false; }
            return _typeAliasMappings.TryGetValue(type, out alias);
        }
        public bool TryGetSubQueryAlias(Type type, out string alias)
        {
            if (type == null) { alias = null; return false; }
            return _subQueryTypeAliases.TryGetValue(type, out alias);
        }
        public bool IsSubqueryAlias(string alias)
        {
            if (string.IsNullOrEmpty(alias)) return false;
            return _subQueryTypeAliases.Values.Contains(alias);
        }
        public IEnumerable<KeyValuePair<string, string>> GetAllTableAliases()
        {
            return _tableAliasMappings.AsEnumerable();
        }
        public void ValidateAliases()
        {
            var allAliases = new HashSet<string>();
            foreach (var kvp in _tableAliasMappings) if (!allAliases.Add(kvp.Value)) throw new InvalidOperationException("Duplicate table alias detected: " + kvp.Value + " for table " + kvp.Key);
            foreach (var kvp in _typeAliasMappings) if (!allAliases.Add(kvp.Value)) throw new InvalidOperationException("Duplicate type alias detected: " + kvp.Value + " for type " + kvp.Key.Name);
            foreach (var kvp in _subQueryTypeAliases) if (!allAliases.Add(kvp.Value)) throw new InvalidOperationException("Duplicate subquery alias detected: " + kvp.Value + " for type " + kvp.Key.Name);
        }
        public void ClearAliases()
        {
            lock (_aliasLock)
            {
                _tableAliasMappings.Clear();
                _typeAliasMappings.Clear();
                _subQueryTypeAliases.Clear();
                _aliasSources.Clear();
                _aliasCounter = 1;
                _subQueryCounter = 1;
            }
        }
        private string GetTableName(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var tableAttr = type.GetCustomAttribute<TableAttribute>();
            var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema) ? "[" + tableAttr.Schema + "]" : "[dbo]";
            var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName) ? "[" + tableAttr.TableName + "]" : "[" + type.Name + "]";
            return schema + "." + name;
        }
    }
    internal class ParameterBuilder
    {
        private readonly ConcurrentDictionary<string, object> _parameters = new ConcurrentDictionary<string, object>();
        private int _paramCounter = 0;
        public string GetUniqueParameterName()
        {
            var id = Interlocked.Increment(ref _paramCounter);
            return "@p" + id.ToString();
        }
        public void AddParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException("name");
            _parameters.TryAdd(name, value);
        }
        public Dictionary<string, string> MergeParameters(ConcurrentDictionary<string, object> sourceParameters)
        {
            var renamings = new Dictionary<string, string>();
            if (sourceParameters == null) return renamings;
            foreach (var kv in sourceParameters)
            {
                string newName = kv.Key;
                if (_parameters.ContainsKey(kv.Key))
                {
                    newName = GetUniqueParameterName();
                    renamings[kv.Key] = newName;
                }
                _parameters.TryAdd(newName, kv.Value);
            }
            return renamings;
        }
        public ConcurrentDictionary<string, object> GetParameters()
        {
            return _parameters;
        }
    }
    internal class QueryBuilderCore<T> : IDisposable
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
        private static readonly SemaphoreSlim _connectionSemaphore = new SemaphoreSlim(Environment.ProcessorCount * 2, Environment.ProcessorCount * 2);
        public QueryBuilderCore(IDbConnection connection, AliasManager aliasManager, ParameterBuilder parameterBuilder, ExpressionParser expressionParser)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            _lazyConnection = new Lazy<IDbConnection>(() => connection);
            _aliasManager = aliasManager ?? throw new ArgumentNullException("aliasManager");
            _parameterBuilder = parameterBuilder ?? throw new ArgumentNullException("parameterBuilder");
            _expressionParser = expressionParser ?? throw new ArgumentNullException("expressionParser");
            _timeOut = _lazyConnection.Value.ConnectionTimeout;
            var mainTableName = GetTableName(typeof(T));
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
            foreach (var col in _expressionParser.ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " ASC")) _orderByQueue.Enqueue(col);
        }
        public void OrderByDescending(Expression<Func<T, object>> keySelector)
        {
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            foreach (var col in _expressionParser.ExtractColumnListWithBrackets(keySelector.Body).Select(c => c + " DESC")) _orderByQueue.Enqueue(col);
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
            var parsed = _expressionParser.ParseMemberWithBrackets(expr);
            var agg = string.IsNullOrEmpty(alias) ? fn + "(" + parsed + ")" : fn + "(" + parsed + ") AS " + alias;
            _aggregateColumns.Enqueue(agg);
        }
        public void GroupBy(params Expression<Func<T, object>>[] groupByColumns)
        {
            if (groupByColumns == null) return;
            foreach (var column in groupByColumns) if (column != null) _groupByColumns.Enqueue(_expressionParser.ParseMemberWithBrackets(column.Body));
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
            var parts = _expressionParser.ExtractColumnListWithBrackets(partitionBy.Body);
            var orders = _expressionParser.ExtractColumnListWithBrackets(orderBy.Body);
            _rowNumberClause = "ROW_NUMBER() OVER (PARTITION BY " + string.Join(", ", parts) + " ORDER BY " + string.Join(", ", orders) + ") AS " + alias;
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
            var leftTableName = GetTableName(typeof(TLeft));
            var rightTableName = GetTableName(typeof(TRight));
            var leftAlias = _aliasManager.GetAliasForType(typeof(TLeft));
            string rightAlias;
            bool isSelfJoin = typeof(TLeft) == typeof(TRight);
            int joinCount = _joins.Count(j => j.TableName == rightTableName);
            bool isRepeatedJoin = joinCount > 0;
            if (isSelfJoin || isRepeatedJoin) rightAlias = _aliasManager.GetUniqueAliasForType(typeof(TRight));
            else
            {
                if (!_aliasManager.TryGetTypeAlias(typeof(TRight), out rightAlias))
                {
                    rightAlias = _aliasManager.GenerateAlias(rightTableName);
                    _aliasManager.SetTypeAlias(typeof(TRight), rightAlias);
                    _aliasManager.SetTableAlias(rightTableName, rightAlias);
                }
            }
            var parameterAliases = new Dictionary<ParameterExpression, string> { { onCondition.Parameters[0], leftAlias }, { onCondition.Parameters[1], rightAlias } };
            var parsed = _expressionParser.ParseExpressionWithParameterMappingAndBrackets(onCondition.Body, parameterAliases);
            _joins.Enqueue(new JoinInfo { JoinType = joinType, TableName = rightTableName, Alias = rightAlias, OnCondition = parsed });
        }
        public void CrossApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder) => AddApply("CROSS APPLY", onCondition, subBuilder);
        public void OuterApply<TSubQuery>(Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder) => AddApply("OUTER APPLY", onCondition, subBuilder);
        private void AddApply<TSubQuery>(string applyType, Expression<Func<T, TSubQuery, bool>> onCondition, Func<IQueryBuilder<TSubQuery>, IQueryBuilder<TSubQuery>> subBuilder)
        {
            if (string.IsNullOrWhiteSpace(applyType)) throw new ArgumentNullException("applyType");
            if (onCondition == null) throw new ArgumentNullException("onCondition");
            if (subBuilder == null) throw new ArgumentNullException("subBuilder");
            var sub = new QueryBuilder<TSubQuery>(_lazyConnection.Value);
            var qb = subBuilder(sub);
            if (qb == null) throw new InvalidOperationException("Sub-query builder returned null");
            var subQb = (QueryBuilder<TSubQuery>)qb;
            var subAliasManager = subQb.GetAliasManager();
            var subParameterBuilder = subQb.GetParameterBuilder();
            string subSql = subQb.BuildQuery();
            string subMainAlias = subAliasManager.GetAliasForType(typeof(TSubQuery));
            var parameterAliases = new Dictionary<ParameterExpression, string> { { onCondition.Parameters[0], _aliasManager.GetAliasForType(typeof(T)) }, { onCondition.Parameters[1], subMainAlias } };
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
            foreach (var renaming in renamings) subSql = subSql.Replace(renaming.Key, renaming.Value);
            var applyAlias = _aliasManager.GenerateSubQueryAlias(GetTableName(typeof(TSubQuery)));
            _aliasManager.SetSubQueryAlias(typeof(TSubQuery), applyAlias);
            _applies.Enqueue(new ApplyInfo { ApplyType = applyType, SubQuery = "(" + subSql + ") AS " + applyAlias, SubQueryAlias = applyAlias });
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
            foreach (var renaming in renamings) sql = sql.Replace(renaming.Key, renaming.Value);
            var clause = op + " (" + sql + ")";
            if (op.StartsWith("UNION")) _unionClauses.Enqueue(clause);
            else if (op == "INTERSECT") _intersectClauses.Enqueue(clause);
            else if (op == "EXCEPT") _exceptClauses.Enqueue(clause);
        }
        public IEnumerable<T> Execute() => GetOpenConnection().Query<T>(BuildQuery(), _parameterBuilder.GetParameters(), commandTimeout: _timeOut);
        public IEnumerable<TResult> Execute<TResult>() => GetOpenConnection().Query<TResult>(BuildQuery(), _parameterBuilder.GetParameters(), commandTimeout: _timeOut);
        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = BuildQuery();
            await _connectionSemaphore.WaitAsync();
            try { return await GetOpenConnection().QueryAsync<T>(query, _parameterBuilder.GetParameters(), commandTimeout: _timeOut); }
            finally { _connectionSemaphore.Release(); }
        }
        public async Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            var query = BuildQuery();
            await _connectionSemaphore.WaitAsync();
            try { return await GetOpenConnection().QueryAsync<TResult>(query, _parameterBuilder.GetParameters(), commandTimeout: _timeOut); }
            finally { _connectionSemaphore.Release(); }
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
            if (_aggregateColumns.Any() && !_groupByColumns.Any() && _selectedColumnsQueue.Any()) throw new InvalidOperationException("When using aggregate functions, either use GROUP BY or avoid selecting non-aggregate columns.");
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
            else columns = string.Join(", ", typeof(T).GetProperties().Select(p => _aliasManager.GetAliasForType(typeof(T)) + ".[" + GetColumnName(p) + "] AS " + p.Name));
            if (!string.IsNullOrEmpty(_rowNumberClause)) columns = _rowNumberClause + ", " + columns;
            var result = new StringBuilder("SELECT");
            if (!string.IsNullOrEmpty(_distinctClause)) result.Append(' ').Append(_distinctClause);
            if (!string.IsNullOrEmpty(_topClause)) result.Append(' ').Append(_topClause);
            result.Append(' ').Append(columns);
            return result.ToString();
        }
        private string BuildFromClause()
        {
            var tableName = GetTableName(typeof(T));
            var alias = _aliasManager.GetAliasForTable(tableName);
            return " FROM " + tableName + " AS " + alias;
        }
        private string BuildJoinClauses()
        {
            var sb = new StringBuilder();
            foreach (var join in _joins) sb.Append(' ').Append(join.JoinType).Append(' ').Append(join.TableName + " AS " + join.Alias).Append(" ON ").Append(join.OnCondition);
            return sb.ToString();
        }
        private string BuildApplyClauses()
        {
            var sb = new StringBuilder();
            foreach (var apply in _applies) sb.Append(' ').Append(apply.ApplyType).Append(' ').Append(apply.SubQuery);
            return sb.ToString();
        }
        private string BuildWhereClause() => _filters.Any() ? "WHERE " + string.Join(" AND ", _filters.ToArray()) : string.Empty;
        private string BuildGroupByClause() => _groupByColumns.Any() ? "GROUP BY " + string.Join(", ", _groupByColumns.ToArray()) : string.Empty;
        private string BuildHavingClause() => !string.IsNullOrEmpty(_havingClause) ? "HAVING " + _havingClause : string.Empty;
        private string BuildOrderByClause() => _orderByQueue.Any() ? "ORDER BY " + string.Join(", ", _orderByQueue.ToArray()) : string.Empty;
        private string BuildPaginationClause(string orderByClause)
        {
            int? limit, offset;
            lock (_pagingLock) { limit = _limit; offset = _offset; }
            if (limit.HasValue)
            {
                if (string.IsNullOrEmpty(orderByClause)) { _orderByQueue.Enqueue("(SELECT 1)"); orderByClause = BuildOrderByClause(); }
                return "OFFSET " + offset + " ROWS FETCH NEXT " + limit + " ROWS ONLY";
            }
            return string.Empty;
        }
        private int FindWhereInsertPosition(string sql)
        {
            const string FROM = "FROM ";
            const string WHERE = " WHERE ";
            const string GROUP_BY = " GROUP BY ";
            const string ORDER_BY = " ORDER BY ";
            const string HAVING = " HAVING ";
            const string JOIN = " JOIN ";
            const string INNER_JOIN = " INNER JOIN ";
            const string LEFT_JOIN = " LEFT JOIN ";
            const string RIGHT_JOIN = " RIGHT JOIN ";
            const string FULL_JOIN = " FULL JOIN ";
            const string CROSS_JOIN = " CROSS JOIN ";
            const string ON = "ON ";
            int fromIndex = sql.IndexOf(FROM, StringComparison.OrdinalIgnoreCase);
            if (fromIndex == -1) return 0;
            int currentPos = fromIndex;
            while (true)
            {
                int nextWhere = sql.IndexOf(WHERE, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextGroupBy = sql.IndexOf(GROUP_BY, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextOrderBy = sql.IndexOf(ORDER_BY, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextHaving = sql.IndexOf(HAVING, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextJoin = sql.IndexOf(JOIN, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextInnerJoin = sql.IndexOf(INNER_JOIN, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextLeftJoin = sql.IndexOf(LEFT_JOIN, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextRightJoin = sql.IndexOf(RIGHT_JOIN, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextFullJoin = sql.IndexOf(FULL_JOIN, currentPos, StringComparison.OrdinalIgnoreCase);
                int nextCrossJoin = sql.IndexOf(CROSS_JOIN, currentPos, StringComparison.OrdinalIgnoreCase);
                int[] positions = { nextWhere, nextGroupBy, nextOrderBy, nextHaving, nextJoin, nextInnerJoin, nextLeftJoin, nextRightJoin, nextFullJoin, nextCrossJoin };
                int minPos = positions.Where(p => p != -1).DefaultIfEmpty(sql.Length).Min();
                if (minPos == sql.Length) return sql.Length;
                if (minPos == nextJoin || minPos == nextInnerJoin || minPos == nextLeftJoin || minPos == nextRightJoin || minPos == nextFullJoin || minPos == nextCrossJoin)
                {
                    int onIndex = sql.IndexOf(ON, minPos, StringComparison.OrdinalIgnoreCase);
                    if (onIndex != -1)
                    {
                        int onEnd = FindNextClausePosition(sql, onIndex + 3);
                        if (onEnd == -1) currentPos = sql.Length;
                        else currentPos = onEnd;
                        continue;
                    }
                    else { currentPos = minPos + 4; continue; }
                }
                else { return minPos; }
            }
        }
        private int FindNextClausePosition(string sql, int startIndex)
        {
            const string GROUP_BY = "GROUP BY ";
            const string ORDER_BY = "ORDER BY ";
            const string HAVING = "HAVING ";
            int[] positions = { sql.IndexOf(GROUP_BY, startIndex, StringComparison.OrdinalIgnoreCase), sql.IndexOf(ORDER_BY, startIndex, StringComparison.OrdinalIgnoreCase), sql.IndexOf(HAVING, startIndex, StringComparison.OrdinalIgnoreCase) };
            return positions.Where(p => p >= startIndex).DefaultIfEmpty(-1).Min();
        }
        private void SetClause(ref string clauseField, string value) { clauseField = value; }
        private void SetFlag(ref bool flagField, bool value) { flagField = value; }
        private string GetTableName(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var tableAttr = type.GetCustomAttribute<TableAttribute>();
            var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema) ? "[" + tableAttr.Schema + "]" : "[dbo]";
            var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName) ? "[" + tableAttr.TableName + "]" : "[" + type.Name + "]";
            return schema + "." + name;
        }
        private string GetColumnName(PropertyInfo property)
        {
            if (property == null) throw new ArgumentNullException("property", "PropertyInfo cannot be null");
            var column = property.GetCustomAttribute<ColumnAttribute>();
            return column != null && !string.IsNullOrWhiteSpace(column.ColumnName) ? column.ColumnName : property.Name;
        }
        private IDbConnection GetOpenConnection()
        {
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open) connection.Open();
            return connection;
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                if (_lazyConnection.IsValueCreated && _lazyConnection.Value.State != ConnectionState.Closed) _lazyConnection.Value.Close();
            }
            _disposed = true;
        }
        ~QueryBuilderCore() { Dispose(false); }
        private class JoinInfo { public string JoinType { get; set; } public string TableName { get; set; } public string Alias { get; set; } public string OnCondition { get; set; } }
        private class ApplyInfo { public string ApplyType { get; set; } public string SubQuery { get; set; } public string SubQueryAlias { get; set; } }
    }
    internal sealed class QueryBuilder<T> : IQueryBuilder<T>, IDisposable
    {
        private readonly QueryBuilderCore<T> _core;
        private readonly AliasManager _aliasManager;
        private readonly ParameterBuilder _parameterBuilder;
        private readonly ExpressionParser _expressionParser;
        private bool _disposed = false;
        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection", "Connection cannot be null.");
            _aliasManager = new AliasManager();
            _parameterBuilder = new ParameterBuilder();
            _expressionParser = new ExpressionParser(_aliasManager, _parameterBuilder);
            _core = new QueryBuilderCore<T>(connection, _aliasManager, _parameterBuilder, _expressionParser);
        }
        public IQueryBuilder<T> WithTableAlias(string tableName, string customAlias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(customAlias)) throw new ArgumentNullException("Table name and alias cannot be null or empty.");
            _aliasManager.SetTableAlias(tableName, customAlias);
            return this;
        }
        public IQueryBuilder<T> WithTableAlias(Type tableType, string customAlias)
        {
            if (tableType == null) throw new ArgumentNullException("tableType");
            if (string.IsNullOrEmpty(customAlias)) throw new ArgumentNullException("customAlias");
            var tableName = GetTableName(tableType);
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
        private string GetTableName(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var tableAttr = type.GetCustomAttribute<TableAttribute>();
            var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema) ? "[" + tableAttr.Schema + "]" : "[dbo]";
            var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName) ? "[" + tableAttr.TableName + "]" : "[" + type.Name + "]";
            return schema + "." + name;
        }
    }
}
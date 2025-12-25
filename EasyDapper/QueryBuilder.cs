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
using System.Runtime.CompilerServices;

namespace EasyDapper
{
    internal static class QueryBuilderCache
    {
        private static readonly ConcurrentDictionary<Type, string> _tableNameCache = new ConcurrentDictionary<Type, string>();
        private static readonly ConcurrentDictionary<MemberInfo, string> _columnNameCache = new ConcurrentDictionary<MemberInfo, string>();
        private static readonly char[] InvalidIdentifierChars = new[] { ';', '-', '-', '/', '*', '\'', '"', '[', ']', '(', ')', '&', '|', '^', '%', '~', '`', '$', '{', '}', '<', '>', '?', '!', '=', '+', ',', ':', '\\', ' ', '\t', '\n', '\r' };

        public static string GetTableName(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            return _tableNameCache.GetOrAdd(type, t =>
            {
                var tableAttr = t.GetCustomAttribute<TableAttribute>();
                var schema = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.Schema) ? "[" + Sanitize(tableAttr.Schema) + "]" : "[dbo]";
                var name = tableAttr != null && !string.IsNullOrWhiteSpace(tableAttr.TableName) ? "[" + Sanitize(tableAttr.TableName) + "]" : "[" + t.Name + "]";
                return schema + "." + name;
            });
        }
        public static string GetColumnName(MemberInfo member)
        {
            if (member == null) throw new ArgumentNullException("member", "MemberInfo cannot be null");
            return _columnNameCache.GetOrAdd(member, m =>
            {
                var column = m.GetCustomAttribute<ColumnAttribute>();
                return column != null && !string.IsNullOrWhiteSpace(column.ColumnName) ? Sanitize(column.ColumnName) : Sanitize(m.Name);
            });
        }
        private static string Sanitize(string identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier)) throw new ArgumentException("Identifier cannot be null or empty.", "identifier");
            if (identifier.IndexOfAny(InvalidIdentifierChars) >= 0) throw new ArgumentException("Identifier contains invalid characters: " + identifier, "identifier");
            return identifier;
        }
    }

    internal class SqlTemplate
    {
        public string Sql { get; set; }
        public List<string> LocalParameterNames { get; set; }
    }

    internal sealed class ExpressionParser
    {
        private readonly AliasManager _aliasManager;
        private readonly ParameterBuilder _parameterBuilder;
        private static readonly ConcurrentDictionary<int, SqlTemplate> _expressionTemplateCache = new ConcurrentDictionary<int, SqlTemplate>();

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
            if (parameterAliases == null)
            {
                var templateHash = CalculateTemplateHash(expression, useBrackets, parameterAliases);
                var template = _expressionTemplateCache.GetOrAdd(templateHash, k => ParseToTemplate(expression, useBrackets, parameterAliases));
                var constants = ExtractConstants(expression);
                var sql = template.Sql;
                if (constants.Count == template.LocalParameterNames.Count)
                {
                    for (int i = 0; i < constants.Count; i++)
                    {
                        var localName = template.LocalParameterNames[i];
                        var value = constants[i];
                        var globalName = _parameterBuilder.GetUniqueParameterName();
                        _parameterBuilder.AddParameter(globalName, value);
                        sql = SafeReplaceParameter(sql, localName, globalName);
                    }
                    return sql;
                }
                else
                {
                    return ParseMainLogic(expression, useBrackets, parameterAliases, _parameterBuilder);
                }
            }
            else
            {
                return ParseMainLogic(expression, useBrackets, parameterAliases, _parameterBuilder);
            }
        }

        private List<object> ExtractConstants(Expression expression)
        {
            var extractor = new ConstantExtractor();
            extractor.Visit(expression);
            return extractor.Constants;
        }

        private SqlTemplate ParseToTemplate(Expression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases)
        {
            var tempBuilder = new ParameterBuilder();
            var sql = ParseMainLogic(expression, useBrackets, parameterAliases, tempBuilder);
            var paramNames = tempBuilder.GetOrderedParameterNames();
            return new SqlTemplate { Sql = sql, LocalParameterNames = paramNames };
        }

        private string ParseMainLogic(Expression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal: return useBrackets ? HandleEqualWithBrackets((BinaryExpression)expression, parameterAliases, paramBuilder) : HandleEqual((BinaryExpression)expression, parameterAliases, paramBuilder);
                case ExpressionType.NotEqual: return useBrackets ? HandleNotEqualWithBrackets((BinaryExpression)expression, parameterAliases, paramBuilder) : HandleNotEqual((BinaryExpression)expression, parameterAliases, paramBuilder);
                case ExpressionType.GreaterThan:
                case ExpressionType.LessThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThanOrEqual:
                    var op = GetOperator(expression.NodeType);
                    return useBrackets ? HandleBinaryWithBrackets((BinaryExpression)expression, op, parameterAliases, paramBuilder) : HandleBinary((BinaryExpression)expression, op, parameterAliases, paramBuilder);
                case ExpressionType.AndAlso: return "(" + ParseMainLogic(((BinaryExpression)expression).Left, useBrackets, parameterAliases, paramBuilder) + " AND " + ParseMainLogic(((BinaryExpression)expression).Right, useBrackets, parameterAliases, paramBuilder) + ")";
                case ExpressionType.OrElse: return "(" + ParseMainLogic(((BinaryExpression)expression).Left, useBrackets, parameterAliases, paramBuilder) + " OR " + ParseMainLogic(((BinaryExpression)expression).Right, useBrackets, parameterAliases, paramBuilder) + ")";
                case ExpressionType.Not: return HandleNotExpression((UnaryExpression)expression, useBrackets, parameterAliases, paramBuilder);
                case ExpressionType.Call: return useBrackets ? HandleMethodCallWithBrackets((MethodCallExpression)expression, parameterAliases, paramBuilder) : HandleMethodCall((MethodCallExpression)expression, parameterAliases, paramBuilder);
                case ExpressionType.MemberAccess: return HandleMemberAccess((MemberExpression)expression, useBrackets, parameterAliases);
                case ExpressionType.Constant: return HandleConstant((ConstantExpression)expression, paramBuilder);
                case ExpressionType.Convert: return ParseMainLogic(((UnaryExpression)expression).Operand, useBrackets, parameterAliases, paramBuilder);
                case ExpressionType.Parameter: return HandleParameter((ParameterExpression)expression, parameterAliases);
                default: throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported");
            }
        }

        private string HandleEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
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
            return HandleBinary(expression, "=", parameterAliases, paramBuilder);
        }
        private string HandleEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
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
            return HandleBinaryWithBrackets(expression, "=", parameterAliases, paramBuilder);
        }
        private string HandleNotEqual(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
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
            return HandleBinary(expression, "<>", parameterAliases, paramBuilder);
        }
        private string HandleNotEqualWithBrackets(BinaryExpression expression, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
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
            return HandleBinaryWithBrackets(expression, "<>", parameterAliases, paramBuilder);
        }

        private string HandleBinary(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var left = ParseMember(expression.Left, parameterAliases);
            var right = ParseValue(expression.Right, paramBuilder, parameterAliases: parameterAliases);
            return left + " " + op + " " + right;
        }
        private string HandleBinaryWithBrackets(BinaryExpression expression, string op, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var left = ParseMemberWithBrackets(expression.Left, parameterAliases);
            var right = ParseValue(expression.Right, paramBuilder, parameterAliases: parameterAliases);
            return left + " " + op + " " + right;
        }
        private string HandleNotExpression(UnaryExpression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            var operand = expression.Operand;
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
            return "NOT (" + ParseMainLogic(operand, useBrackets, parameterAliases, paramBuilder) + ")";
        }
        private string HandleMethodCall(MethodCallExpression m, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            if (m == null) throw new ArgumentNullException("m");
            if (m.Method.DeclaringType == typeof(string))
            {
                if (m.Method.Name == "StartsWith") return HandleLike(m, "{0}%", parameterAliases, paramBuilder);
                if (m.Method.Name == "EndsWith") return HandleLike(m, "%{0}", parameterAliases, paramBuilder);
                if (m.Method.Name == "Contains") return HandleLike(m, "%{0}%", parameterAliases, paramBuilder);
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
                    var p = paramBuilder.GetUniqueParameterName();
                    paramBuilder.AddParameter(p, item);
                    items.Add(p);
                }
                return memberSql + " IN (" + string.Join(",", items) + ")";
            }
            if (m.Method.Name == "Between" && m.Arguments.Count == 3)
            {
                var property = ParseMember(m.Arguments[0], parameterAliases);
                var lower = ParseValue(m.Arguments[1], paramBuilder, parameterAliases: parameterAliases);
                var upper = ParseValue(m.Arguments[2], paramBuilder, parameterAliases: parameterAliases);
                return property + " BETWEEN " + lower + " AND " + upper;
            }
            if (m.Method.Name == "IsNullOrEmpty" && m.Arguments.Count == 1 && m.Method.DeclaringType == typeof(string))
            {
                var prop = ParseMember(m.Arguments[0], parameterAliases);
                return "(" + prop + " IS NULL OR " + prop + " = '')";
            }
            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported.");
        }
        private string HandleMethodCallWithBrackets(MethodCallExpression m, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            if (m == null) throw new ArgumentNullException("m");
            if (m.Method.DeclaringType == typeof(string))
            {
                if (m.Method.Name == "StartsWith") return HandleLikeWithBrackets(m, "{0}%", parameterAliases, paramBuilder);
                if (m.Method.Name == "EndsWith") return HandleLikeWithBrackets(m, "%{0}", parameterAliases, paramBuilder);
                if (m.Method.Name == "Contains") return HandleLikeWithBrackets(m, "%{0}%", parameterAliases, paramBuilder);
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
                    var p = paramBuilder.GetUniqueParameterName();
                    paramBuilder.AddParameter(p, item);
                    items.Add(p);
                }
                return memberSql + " IN (" + string.Join(",", items) + ")";
            }
            if (m.Method.Name == "Between" && m.Arguments.Count == 3)
            {
                var property = ParseMemberWithBrackets(m.Arguments[0], parameterAliases);
                var lower = ParseValue(m.Arguments[1], paramBuilder, parameterAliases: parameterAliases);
                var upper = ParseValue(m.Arguments[2], paramBuilder, parameterAliases: parameterAliases);
                return property + " BETWEEN " + lower + " AND " + upper;
            }
            if (m.Method.Name == "IsNullOrEmpty" && m.Arguments.Count == 1 && m.Method.DeclaringType == typeof(string))
            {
                var prop = ParseMemberWithBrackets(m.Arguments[0], parameterAliases);
                return "(" + prop + " IS NULL OR " + prop + " = '')";
            }
            throw new NotSupportedException("Method '" + m.Method.Name + "' is not supported");
        }
        private string HandleLike(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            if (m == null) throw new ArgumentNullException("m");
            if (m.Object == null) throw new NotSupportedException("LIKE requires instance method call on string");
            var prop = ParseMember(m.Object, parameterAliases);
            var val = ParseValue(m.Arguments[0], paramBuilder, format, parameterAliases);
            return prop + " LIKE " + val;
        }
        private string HandleLikeWithBrackets(MethodCallExpression m, string format, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            if (m == null) throw new ArgumentNullException("m");
            if (m.Object == null) throw new NotSupportedException("LIKE requires instance method call on string");
            var prop = ParseMemberWithBrackets(m.Object, parameterAliases);
            var val = ParseValue(m.Arguments[0], paramBuilder, format, parameterAliases);
            return prop + " LIKE " + val;
        }
        private string HandleConstant(ConstantExpression expression, ParameterBuilder paramBuilder)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var p = paramBuilder.GetUniqueParameterName();
            paramBuilder.AddParameter(p, expression.Value);
            return p;
        }
        private string HandleParameter(ParameterExpression expression, Dictionary<ParameterExpression, string> parameterAliases)
        {
            var paramExpr = expression;
            if (parameterAliases != null && parameterAliases.TryGetValue(paramExpr, out var alias)) return alias;
            return _aliasManager.GetAliasForType(paramExpr.Type);
        }

        private string ParseValue(Expression expression, ParameterBuilder paramBuilder, string format = null, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var constant = expression as ConstantExpression;
            if (constant != null)
            {
                var p = paramBuilder.GetUniqueParameterName();
                var v = constant.Value;
                if (format != null && v is string) v = string.Format(format, v);
                paramBuilder.AddParameter(p, v);
                return p;
            }
            var member = expression as MemberExpression;
            if (member != null)
            {
                var root = GetRootExpression(member);
                if (root.NodeType == ExpressionType.Parameter)
                {
                    return ParseMember(expression, parameterAliases);
                }
                if (root.NodeType == ExpressionType.Constant)
                {
                    var value = Evaluate(expression);
                    if (format != null && value is string) value = string.Format(format, value);

                    var p = paramBuilder.GetUniqueParameterName();
                    paramBuilder.AddParameter(p, value);
                    return p;
                }
                var eval = Evaluate(expression);
                var p2 = paramBuilder.GetUniqueParameterName();
                if (format != null && eval is string) eval = string.Format(format, eval);
                paramBuilder.AddParameter(p2, eval);
                return p2;
            }

            var unary = expression as UnaryExpression;
            if (unary != null) return ParseValue(unary.Operand, paramBuilder, format, parameterAliases);

            var binary = expression as BinaryExpression;
            if (binary != null)
            {
                var left = ParseValue(binary.Left, paramBuilder, format, parameterAliases);
                var right = ParseValue(binary.Right, paramBuilder, format, parameterAliases);
                return left + " " + GetOperator(binary.NodeType) + " " + right;
            }
            var evalFallback = Evaluate(expression);
            var p3 = paramBuilder.GetUniqueParameterName();
            paramBuilder.AddParameter(p3, evalFallback);
            return p3;
        }

        private Expression GetRootExpression(Expression exp)
        {
            if (exp == null) return null;
            while (exp is MemberExpression member && member.Expression != null)
            {
                exp = member.Expression;
            }
            return exp;
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

        private string SafeReplaceParameter(string sql, string oldName, string newName)
        {
            return Regex.Replace(sql, @"\b" + Regex.Escape(oldName) + @"\b", newName, RegexOptions.IgnoreCase);
        }

        private int CalculateTemplateHash(Expression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases)
        {
            if (expression == null) return 0;
            unchecked
            {
                int hash = expression.NodeType.GetHashCode();
                hash = (hash * 397) ^ useBrackets.GetHashCode();
                hash = (hash * 397) ^ expression.Type.GetHashCode();

                if (expression is MemberExpression m)
                {
                    hash = (hash * 397) ^ m.Member.Name.GetHashCode();
                    if (m.Expression != null) hash = (hash * 397) ^ CalculateTemplateHash(m.Expression, false, null);
                }
                else if (expression is MethodCallExpression mc)
                {
                    hash = (hash * 397) ^ mc.Method.Name.GetHashCode();
                    if (mc.Object != null) hash = (hash * 397) ^ CalculateTemplateHash(mc.Object, false, null);
                    foreach (var arg in mc.Arguments) hash = (hash * 397) ^ CalculateTemplateHash(arg, false, null);
                }
                else if (expression is BinaryExpression b)
                {
                    hash = (hash * 397) ^ CalculateTemplateHash(b.Left, false, null);
                    hash = (hash * 397) ^ CalculateTemplateHash(b.Right, false, null);
                }
                else if (expression is UnaryExpression u)
                {
                    hash = (hash * 397) ^ CalculateTemplateHash(u.Operand, false, null);
                }
                else if (expression is ParameterExpression p)
                {
                    if (parameterAliases != null && parameterAliases.TryGetValue(p, out var alias)) hash = (hash * 397) ^ alias.GetHashCode();
                    else hash = (hash * 397) ^ p.Type.GetHashCode();
                }

                return hash;
            }
        }

        private class ConstantExtractor : ExpressionVisitor
        {
            public readonly List<object> Constants = new List<object>();
            private bool IsSimpleType(Type type)
            {
                return type.IsValueType || type.IsEnum || type == typeof(string) || type == typeof(decimal) || type == typeof(DateTime) || type == typeof(Guid) || type == typeof(byte[]);
            }

            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value == null || IsSimpleType(node.Value.GetType()))
                {
                    Constants.Add(node.Value);
                }
                return base.VisitConstant(node);
            }

            protected override Expression VisitMember(MemberExpression node)
            {
                if (node.Expression != null && node.Expression.NodeType == ExpressionType.Constant)
                {
                    var obj = ((ConstantExpression)node.Expression).Value;
                    if (obj != null)
                    {
                        var fi = node.Member as FieldInfo;
                        var pi = node.Member as PropertyInfo;

                        object val = null;
                        if (fi != null) val = fi.GetValue(obj);
                        else if (pi != null) val = pi.GetValue(obj, null);
                        if (val != null && !IsSimpleType(val.GetType()))
                        {
                            return node;
                        }

                        Constants.Add(val);
                        return node;
                    }
                }
                return base.VisitMember(node);
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
                return tableAlias + ".[" + QueryBuilderCache.GetColumnName(member.Member) + "] AS " + columnName;
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
                        else columns.Add(tableAlias + ".[" + QueryBuilderCache.GetColumnName(memberArg.Member) + "] AS " + newExpr.Members[i].Name);
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
                    return tableAlias + ".[" + QueryBuilderCache.GetColumnName(member.Member) + "]";
                }
                return "[" + QueryBuilderCache.GetColumnName(member.Member) + "]";
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

        private string HandleMemberAccess(MemberExpression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases)
        {
            var memberExpr = expression;
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
                    return tableAlias + ".[" + QueryBuilderCache.GetColumnName(member.Member) + "]";
                }
                return "[" + QueryBuilderCache.GetColumnName(member.Member) + "]";
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
                return _aliasManager.GetAliasForType(memberType);
            }
            if (member.Expression is MemberExpression nestedMember) return GetTableAliasForMember(nestedMember, parameterAliases);
            if (member.Expression != null) return ParseMemberWithBrackets(member.Expression, parameterAliases);
            return null;
        }
    }

    internal sealed class ParameterBuilder
    {
        private readonly ConcurrentDictionary<string, object> _parameters = new ConcurrentDictionary<string, object>();
        private int _paramCounter = 0;
        private readonly List<string> _orderOfCreation = new List<string>();

        public string GetUniqueParameterName()
        {
            lock (_orderOfCreation)
            {
                var id = Interlocked.Increment(ref _paramCounter);
                var name = "@p" + id.ToString();
                _orderOfCreation.Add(name);
                return name;
            }
        }

        public void AddParameter(string name, object value)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException("name");
            _parameters[name] = value;
            if (!_orderOfCreation.Contains(name)) lock (_orderOfCreation) _orderOfCreation.Add(name);
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
                _parameters[newName] = kv.Value;
            }
            return renamings;
        }

        public ConcurrentDictionary<string, object> GetParameters()
        {
            return _parameters;
        }

        public List<string> GetOrderedParameterNames()
        {
            return _orderOfCreation.ToList();
        }
    }

    internal sealed class AliasManager
    {
        private sealed class AliasInfo { public AliasType Type { get; set; } public object Key { get; set; } }
        private enum AliasType { Table, Type, SubQuery }
        private readonly ConcurrentDictionary<string, AliasInfo> _allAliases = new ConcurrentDictionary<string, AliasInfo>();
        private readonly ConcurrentDictionary<string, string> _tableToAlias = new ConcurrentDictionary<string, string>();
        private readonly ConcurrentDictionary<Type, string> _typeToAlias = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<Type, string> _subQueryTypeToAlias = new ConcurrentDictionary<Type, string>();
        private int _aliasCounter = 1;
        private int _subQueryCounter = 1;

        public string GenerateAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 10) shortName = shortName.Substring(0, 10);
            var counter = Interlocked.Increment(ref _aliasCounter);
            return string.Format("{0}_A{1}", shortName, counter);
        }

        public string GenerateSubQueryAlias(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            var shortName = tableName.Split('.').Last().Trim('[', ']');
            if (shortName.Length > 8) shortName = shortName.Substring(0, 8);
            var counter = Interlocked.Increment(ref _subQueryCounter);
            return string.Format("{0}_SQ{1}", shortName, counter);
        }

        public void SetTableAlias(string tableName, string alias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(alias)) throw new ArgumentException("Table name and alias cannot be null or empty.");
            var newInfo = new AliasInfo { Type = AliasType.Table, Key = tableName };
            if (!_allAliases.TryAdd(alias, newInfo))
            {
                var existingInfo = _allAliases[alias];
                string existingKeyStr = (existingInfo.Key is string) ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                throw new InvalidOperationException("Alias '" + alias + "' is already used by " + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
            }
            _tableToAlias.AddOrUpdate(tableName, alias, (k, v) => alias);
        }

        public void SetTypeAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias)) throw new ArgumentException("Type and alias cannot be null or empty.");
            var targetTableName = QueryBuilderCache.GetTableName(type);
            if (_allAliases.TryGetValue(alias, out var existingInfo))
            {
                if (existingInfo.Type == AliasType.Table && existingInfo.Key is string existingTableName && existingTableName == targetTableName)
                {
                    _typeToAlias.AddOrUpdate(type, alias, (k, v) => alias);
                    return;
                }
                string existingKeyStr = (existingInfo.Key is string) ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                throw new InvalidOperationException("Alias '" + alias + "' is already used by " + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
            }
            else
            {
                SetTableAlias(targetTableName, alias);
                _typeToAlias.AddOrUpdate(type, alias, (k, v) => alias);
            }
        }

        public void SetSubQueryAlias(Type type, string alias)
        {
            if (type == null || string.IsNullOrEmpty(alias)) throw new ArgumentException("Type and alias cannot be null or empty.");
            var newInfo = new AliasInfo { Type = AliasType.SubQuery, Key = type };
            if (!_allAliases.TryAdd(alias, newInfo))
            {
                var existingInfo = _allAliases[alias];
                string existingKeyStr = (existingInfo.Key is string) ? (string)existingInfo.Key : ((Type)existingInfo.Key).Name;
                throw new InvalidOperationException("Alias '" + alias + "' is already used by " + existingInfo.Type.ToString().ToLower() + " '" + existingKeyStr + "'.");
            }
            _subQueryTypeToAlias.AddOrUpdate(type, alias, (k, v) => alias);
        }

        public string GetAliasForTable(string tableName)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException("tableName");
            return _tableToAlias.GetOrAdd(tableName, tn =>
            {
                string newAlias;
                while (true)
                {
                    newAlias = GenerateAlias(tn);
                    var newInfo = new AliasInfo { Type = AliasType.Table, Key = tn };
                    if (_allAliases.TryAdd(newAlias, newInfo))
                    {
                        return newAlias;
                    }
                }
            });
        }

        public string GetAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            return _typeToAlias.GetOrAdd(type, t =>
            {
                var tableName = QueryBuilderCache.GetTableName(t);
                return GetAliasForTable(tableName);
            });
        }

        public string GetUniqueAliasForType(Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            var tableName = QueryBuilderCache.GetTableName(type);
            string newAlias;
            while (true)
            {
                newAlias = GenerateAlias(tableName);
                var newInfo = new AliasInfo { Type = AliasType.Type, Key = type };
                if (_allAliases.TryAdd(newAlias, newInfo))
                {
                    _typeToAlias.AddOrUpdate(type, newAlias, (k, v) => newAlias);
                    return newAlias;
                }
            }
        }

        public bool TryGetTypeAlias(Type type, out string alias)
        {
            if (type == null) { alias = null; return false; }
            return _typeToAlias.TryGetValue(type, out alias);
        }

        public bool TryGetSubQueryAlias(Type type, out string alias)
        {
            if (type == null) { alias = null; return false; }
            return _subQueryTypeToAlias.TryGetValue(type, out alias);
        }

        public bool IsSubqueryAlias(string alias)
        {
            if (string.IsNullOrEmpty(alias)) return false;
            if (_allAliases.TryGetValue(alias, out var info))
            {
                return info.Type == AliasType.SubQuery;
            }
            return false;
        }

        public IEnumerable<KeyValuePair<string, string>> GetAllTableAliases()
        {
            return _allAliases.Where(kvp => kvp.Value.Type == AliasType.Table).Select(kvp => new KeyValuePair<string, string>((string)kvp.Value.Key, kvp.Key));
        }

        public void ValidateAliases() { }

        public void ClearAliases()
        {
            _allAliases.Clear();
            _tableToAlias.Clear();
            _typeToAlias.Clear();
            _subQueryTypeToAlias.Clear();
            Interlocked.Exchange(ref _aliasCounter, 1);
            Interlocked.Exchange(ref _subQueryCounter, 1);
        }
    }

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
        private static readonly char[] InvalidAliasChars = new[] { ']', ';', '-', '-', '/', '*', '\'', '"' };

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

        private readonly bool _ownsConnection;

        public QueryBuilderCore(IDbConnection connection, AliasManager aliasManager, ParameterBuilder parameterBuilder, ExpressionParser expressionParser, bool ownsConnection = false)
        {
            if (connection == null) throw new ArgumentNullException("connection");
            _lazyConnection = new Lazy<IDbConnection>(() => connection);
            _ownsConnection = ownsConnection;
            _aliasManager = aliasManager ?? throw new ArgumentNullException("aliasManager");
            _parameterBuilder = parameterBuilder ?? throw new ArgumentNullException("parameterBuilder");
            _expressionParser = expressionParser ?? throw new ArgumentNullException("expressionParser");
            _timeOut = _lazyConnection.Value.ConnectionTimeout;
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
            ValidateCustomAlias(alias);
            var parsed = _expressionParser.ParseMemberWithBrackets(expr);
            var agg = string.IsNullOrEmpty(alias) ? fn + "(" + parsed + ")" : fn + "(" + parsed + ") AS " + QuoteIdentifier(alias);
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
            ValidateCustomAlias(alias);
            var parts = _expressionParser.ExtractColumnListWithBrackets(partitionBy.Body);
            var orders = _expressionParser.ExtractColumnListWithBrackets(orderBy.Body);
            _rowNumberClause = "ROW_NUMBER() OVER (PARTITION BY " + string.Join(", ", parts) + " ORDER BY " + string.Join(", ", orders) + ") AS " + QuoteIdentifier(alias);
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
            string actualSubAlias = ExtractSubQueryTableAlias(subSql);
            if (string.IsNullOrEmpty(actualSubAlias))
            {

                var subTableName = QueryBuilderCache.GetTableName(typeof(TSubQuery));
                actualSubAlias = subAliasManager.GetAliasForTable(subTableName);
            }

            var parameterAliases = new Dictionary<ParameterExpression, string> {
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

        public IEnumerable<T> Execute() => GetOpenConnection().Query<T>(BuildQuery(), _parameterBuilder.GetParameters(), commandTimeout: _timeOut);
        public IEnumerable<TResult> Execute<TResult>() => GetOpenConnection().Query<TResult>(BuildQuery(), _parameterBuilder.GetParameters(), commandTimeout: _timeOut);

        public async Task<IEnumerable<T>> ExecuteAsync()
        {
            var query = BuildQuery();
            return await GetOpenConnection().QueryAsync<T>(query, _parameterBuilder.GetParameters(), commandTimeout: _timeOut);
        }

        public async Task<IEnumerable<TResult>> ExecuteAsync<TResult>()
        {
            var query = BuildQuery();
            return await GetOpenConnection().QueryAsync<TResult>(query, _parameterBuilder.GetParameters(), commandTimeout: _timeOut);
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
            else columns = string.Join(", ", typeof(T).GetProperties().Select(p => _aliasManager.GetAliasForType(typeof(T)) + ".[" + QueryBuilderCache.GetColumnName(p) + "] AS " + QuoteIdentifier(p.Name)));
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
            foreach (var join in _joins) sb.Append(' ').Append(join.JoinType).Append(' ').Append(join.TableName + " AS " + QuoteIdentifier(join.Alias)).Append(" ON ").Append(join.OnCondition);
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
            var connection = _lazyConnection.Value;
            if (connection.State != ConnectionState.Open) connection.Open();
            return connection;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;
            if (disposing)
            {
                if (_ownsConnection && _lazyConnection.IsValueCreated && _lazyConnection.Value.State != ConnectionState.Closed)
                    _lazyConnection.Value.Close();
            }
            _disposed = true;
        }

        ~QueryBuilderCore() { Dispose(false); }

        private sealed class JoinInfo { public string JoinType { get; set; } public string TableName { get; set; } public string Alias { get; set; } public string OnCondition { get; set; } }
        private sealed class ApplyInfo { public string ApplyType { get; set; } public string SubQuery { get; set; } public string SubQueryAlias { get; set; } }

        internal AliasManager GetAliasManager() => _aliasManager;
        internal ParameterBuilder GetParameterBuilder() => _parameterBuilder;
    }
    internal sealed class QueryBuilder<T> : IQueryBuilder<T>, IDisposable
    {
        private readonly QueryBuilderCore<T> _core;
        private readonly AliasManager _aliasManager;
        private readonly ParameterBuilder _parameterBuilder;
        private readonly ExpressionParser _expressionParser;
        private bool _disposed = false;
        private static readonly char[] InvalidAliasChars = new[] { ']', ';', '-', '-', '/', '*', '\'', '"' };

        private static void ValidateCustomAlias(string alias)
        {
            if (!string.IsNullOrEmpty(alias) && alias.IndexOfAny(InvalidAliasChars) >= 0)
            {
                throw new ArgumentException("Custom alias contains invalid characters.", "alias");
            }
        }

        internal QueryBuilder(IDbConnection connection)
        {
            if (connection == null) throw new ArgumentNullException("connection", "Connection cannot be null.");
            _aliasManager = new AliasManager();
            _parameterBuilder = new ParameterBuilder();
            _expressionParser = new ExpressionParser(_aliasManager, _parameterBuilder);
            _core = new QueryBuilderCore<T>(connection, _aliasManager, _parameterBuilder, _expressionParser, ownsConnection: false);
        }

        public IQueryBuilder<T> WithTableAlias(string tableName, string customAlias)
        {
            if (string.IsNullOrEmpty(tableName) || string.IsNullOrEmpty(customAlias)) throw new ArgumentNullException("Table name and alias cannot be null or empty.");
            ValidateCustomAlias(customAlias);
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
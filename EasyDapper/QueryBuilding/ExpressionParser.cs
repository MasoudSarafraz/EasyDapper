using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace EasyDapper
{
    internal sealed class ExpressionParser
    {
        private readonly AliasManager _aliasManager;
        private readonly ParameterBuilder _parameterBuilder;

        public ExpressionParser(AliasManager aliasManager, ParameterBuilder parameterBuilder)
        {
            if (aliasManager == null) throw new ArgumentNullException("aliasManager");
            if (parameterBuilder == null) throw new ArgumentNullException("parameterBuilder");
            _aliasManager = aliasManager;
            _parameterBuilder = parameterBuilder;
        }

        public string ParseExpression(Expression expression)
            => ParseExpressionInternal(expression, useBrackets: false, parameterAliases: null);

        public string ParseExpressionWithBrackets(Expression expression)
            => ParseExpressionInternal(expression, useBrackets: true, parameterAliases: null);

        public string ParseExpressionWithParameterMapping(Expression expression, Dictionary<ParameterExpression, string> parameterAliases)
            => ParseExpressionInternal(expression, useBrackets: false, parameterAliases: parameterAliases);

        public string ParseExpressionWithParameterMappingAndBrackets(Expression expression, Dictionary<ParameterExpression, string> parameterAliases)
            => ParseExpressionInternal(expression, useBrackets: true, parameterAliases: parameterAliases);

        private string ParseExpressionInternal(Expression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            return ParseMainLogic(expression, useBrackets, parameterAliases, _parameterBuilder);
        }

        private string ParseMainLogic(Expression expression, bool useBrackets, Dictionary<ParameterExpression, string> parameterAliases, ParameterBuilder paramBuilder)
        {
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    return useBrackets
                        ? HandleEqualWithBrackets((BinaryExpression)expression, parameterAliases, paramBuilder)
                        : HandleEqual((BinaryExpression)expression, parameterAliases, paramBuilder);
                case ExpressionType.NotEqual:
                    return useBrackets
                        ? HandleNotEqualWithBrackets((BinaryExpression)expression, parameterAliases, paramBuilder)
                        : HandleNotEqual((BinaryExpression)expression, parameterAliases, paramBuilder);
                case ExpressionType.GreaterThan:
                case ExpressionType.LessThan:
                case ExpressionType.GreaterThanOrEqual:
                case ExpressionType.LessThanOrEqual:
                    var op = GetOperator(expression.NodeType);
                    return useBrackets
                        ? HandleBinaryWithBrackets((BinaryExpression)expression, op, parameterAliases, paramBuilder)
                        : HandleBinary((BinaryExpression)expression, op, parameterAliases, paramBuilder);
                case ExpressionType.AndAlso:
                    return "(" + ParseMainLogic(((BinaryExpression)expression).Left, useBrackets, parameterAliases, paramBuilder)
                         + " AND " + ParseMainLogic(((BinaryExpression)expression).Right, useBrackets, parameterAliases, paramBuilder) + ")";
                case ExpressionType.OrElse:
                    return "(" + ParseMainLogic(((BinaryExpression)expression).Left, useBrackets, parameterAliases, paramBuilder)
                         + " OR " + ParseMainLogic(((BinaryExpression)expression).Right, useBrackets, parameterAliases, paramBuilder) + ")";
                case ExpressionType.Not:
                    return HandleNotExpression((UnaryExpression)expression, useBrackets, parameterAliases, paramBuilder);
                case ExpressionType.Call:
                    return useBrackets
                        ? HandleMethodCallWithBrackets((MethodCallExpression)expression, parameterAliases, paramBuilder)
                        : HandleMethodCall((MethodCallExpression)expression, parameterAliases, paramBuilder);
                case ExpressionType.MemberAccess:
                    return HandleMemberAccess((MemberExpression)expression, useBrackets, parameterAliases);
                case ExpressionType.Constant:
                    return HandleConstant((ConstantExpression)expression, paramBuilder);
                case ExpressionType.Convert:
                    return ParseMainLogic(((UnaryExpression)expression).Operand, useBrackets, parameterAliases, paramBuilder);
                case ExpressionType.Parameter:
                    return HandleParameter((ParameterExpression)expression, parameterAliases);
                default:
                    throw new NotSupportedException("Expression type '" + expression.NodeType + "' is not supported");
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
                            return leftMember.Type == typeof(bool?)
                                ? "ISNULL(" + column + ", 0) = " + (boolValue ? 1 : 0)
                                : column + " = " + (boolValue ? 1 : 0);
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
                            return leftMember.Type == typeof(bool?)
                                ? "ISNULL(" + column + ", 0) = " + (boolValue ? 1 : 0)
                                : column + " = " + (boolValue ? 1 : 0);
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
                            return leftMember.Type == typeof(bool?)
                                ? "ISNULL(" + column + ", 0) <> " + (boolValue ? 1 : 0)
                                : column + " <> " + (boolValue ? 1 : 0);
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
                            return leftMember.Type == typeof(bool?)
                                ? "ISNULL(" + column + ", 0) <> " + (boolValue ? 1 : 0)
                                : column + " <> " + (boolValue ? 1 : 0);
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
                    var prop = useBrackets
                        ? ParseMemberWithBrackets(methodCall.Arguments[0], parameterAliases)
                        : ParseMember(methodCall.Arguments[0], parameterAliases);
                    return "(" + prop + " IS NOT NULL AND " + prop + " <> '')";
                }
            }
            if (operand.NodeType == ExpressionType.MemberAccess)
            {
                var member = (MemberExpression)operand;
                if (member.Type == typeof(bool) || member.Type == typeof(bool?))
                {
                    var column = useBrackets
                        ? ParseMemberWithBrackets(member, parameterAliases)
                        : ParseMember(member, parameterAliases);
                    return member.Type == typeof(bool?)
                        ? "(ISNULL(" + column + ", 0) = 0 OR " + column + " IS NULL)"
                        : column + " = 0";
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
                if (listObject == null)
                    throw new NotSupportedException("Unsupported Contains signature or non-constant collection");
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
                if (listObject == null)
                    throw new NotSupportedException("Unsupported Contains signature or non-constant collection");
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
                if (member.Member.Name == "Value" && member.Expression != null
                    && member.Expression.Type.IsGenericType
                    && member.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    return ParseSelectMember(member.Expression);
                }
                if (_aliasManager.IsSubqueryAlias(tableAlias))
                    return tableAlias + "." + columnName + " AS " + columnName;
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
                        if (memberArg.Member.Name == "Value" && memberArg.Expression != null
                            && memberArg.Expression.Type.IsGenericType
                            && memberArg.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            columns.Add(ParseSelectMember(memberArg.Expression));
                            continue;
                        }
                        if (_aliasManager.IsSubqueryAlias(tableAlias))
                            columns.Add(tableAlias + "." + columnName + " AS " + newExpr.Members[i].Name);
                        else
                            columns.Add(tableAlias + ".[" + QueryBuilderCache.GetColumnName(memberArg.Member) + "] AS " + newExpr.Members[i].Name);
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
                if (member.Member.Name == "Value" && member.Expression != null
                    && member.Expression.Type.IsGenericType
                    && member.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
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
            if (memberExpr.Member.Name == "HasValue" && memberExpr.Expression != null
                && memberExpr.Expression.Type.IsGenericType
                && memberExpr.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var column = useBrackets
                    ? ParseMemberWithBrackets(memberExpr.Expression, parameterAliases)
                    : ParseMember(memberExpr.Expression, parameterAliases);
                return column + " IS NOT NULL";
            }
            if (useBrackets && (memberExpr.Type == typeof(bool) || memberExpr.Type == typeof(bool?)))
            {
                var column = useBrackets
                    ? ParseMemberWithBrackets(memberExpr, parameterAliases)
                    : ParseMember(memberExpr, parameterAliases);
                return memberExpr.Type == typeof(bool?)
                    ? "ISNULL(" + column + ", 0) = 1"
                    : column + " = 1";
            }
            return useBrackets
                ? ParseMemberWithBrackets(memberExpr, parameterAliases)
                : ParseMember(memberExpr, parameterAliases);
        }

        private string ParseMember(Expression expression, Dictionary<ParameterExpression, string> parameterAliases = null)
        {
            if (expression == null) throw new ArgumentNullException("expression");
            var unary = expression as UnaryExpression;
            if (unary != null) return ParseMember(unary.Operand, parameterAliases);
            var member = expression as MemberExpression;
            if (member != null)
            {
                if (member.Member.Name == "Value" && member.Expression != null
                    && member.Expression.Type.IsGenericType
                    && member.Expression.Type.GetGenericTypeDefinition() == typeof(Nullable<>))
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
                if (_aliasManager.TryGetTypeAlias(paramExpr.Type, out var mainAlias)) return mainAlias;
                return _aliasManager.GetAliasForType(paramExpr.Type);
            }
            if (member.Expression is MemberExpression nestedMember)
                return GetTableAliasForMember(nestedMember, parameterAliases);
            if (member.Expression != null)
                return ParseMemberWithBrackets(member.Expression, parameterAliases);
            return null;
        }
    }
}

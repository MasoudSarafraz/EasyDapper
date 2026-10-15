using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using Xunit;

namespace EasyDapper.Tests.Compatibility
{
    public class PublicApiCompatibilityTests
    {
        [Fact]
        public void ExistingPublicSignaturesDefaultsAndGenericConstraintsRemainCompatible()
        {
            var expected = new HashSet<string>(StringComparer.Ordinal);
#if NET462
            const string resourceName = "EasyDapper.Tests.Compatibility.PublicApiBaseline.net462.txt";
#else
            const string resourceName = "EasyDapper.Tests.Compatibility.PublicApiBaseline.txt";
#endif
            using (var stream = typeof(PublicApiCompatibilityTests).Assembly.GetManifestResourceStream(
                resourceName))
            using (var reader = new StreamReader(stream))
            {
                string line;
                while ((line = reader.ReadLine()) != null) expected.Add(line);
            }
            var actual = new HashSet<string>(Snapshot(typeof(DapperServiceFactory).Assembly), StringComparer.Ordinal);
            Assert.Empty(expected.Except(actual).OrderBy(x => x));

            // Adding a member to an existing interface breaks third-party implementations,
            // even when all previously existing members remain available.
            foreach (var contract in new[] { typeof(IDapperService), typeof(IQueryBuilder<>) })
            {
                var prefix = TypeName(contract) + " METHOD ";
                Assert.Equal(expected.Where(x => x.StartsWith(prefix, StringComparison.Ordinal)).OrderBy(x => x),
                    actual.Where(x => x.StartsWith(prefix, StringComparison.Ordinal)).OrderBy(x => x));
            }
        }

        private static IEnumerable<string> Snapshot(Assembly assembly)
        {
            foreach (var type in assembly.GetExportedTypes())
            {
                yield return "TYPE " + TypeName(type) + " flags=" + type.Attributes + " base=" +
                    (type.BaseType == null ? "" : TypeName(type.BaseType)) + " interfaces=" +
                    string.Join(",", type.GetInterfaces().Select(TypeName).OrderBy(x => x));
                if (type.IsGenericTypeDefinition)
                    yield return "TYPEGEN " + TypeName(type) + " " + Generics(type.GetGenericArguments());
                foreach (var constructor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                    yield return TypeName(type) + " CTOR(" + Parameters(constructor) + ")";
                foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                    yield return TypeName(type) + " METHOD " + TypeName(method.ReturnType) + " " + method.Name +
                        "(" + Parameters(method) + ") flags=" + method.Attributes +
                        (method.IsGenericMethodDefinition ? " generic=" + Generics(method.GetGenericArguments()) : "");
                foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly))
                    yield return TypeName(type) + " FIELD " + TypeName(field.FieldType) + " " + field.Name + " flags=" + field.Attributes +
                        (field.IsLiteral ? " value=" + field.GetRawConstantValue() : "");
            }
        }

        private static string Parameters(MethodBase method) => string.Join(",", method.GetParameters().Select(parameter =>
            TypeName(parameter.ParameterType) + " " + parameter.Name + " optional=" + parameter.IsOptional + " default=" +
            (parameter.HasDefaultValue ? Convert.ToString(parameter.DefaultValue, CultureInfo.InvariantCulture) : "<none>") +
            " params=" + parameter.IsDefined(typeof(ParamArrayAttribute), false)));

        private static string Generics(Type[] types) => string.Join(";", types.Select(type =>
            type.GenericParameterPosition + ":" + type.GenericParameterAttributes + ":" +
            string.Join(",", type.GetGenericParameterConstraints().Select(TypeName).OrderBy(x => x))));

        private static string TypeName(Type type)
        {
            if (type.IsGenericParameter) return "!" + type.GenericParameterPosition;
            if (type.IsByRef) return TypeName(type.GetElementType()) + "&";
            if (type.IsArray) return TypeName(type.GetElementType()) + "[" + new string(',', type.GetArrayRank() - 1) + "]";
            return type.IsGenericType ? type.GetGenericTypeDefinition().FullName + "<" +
                string.Join(",", type.GetGenericArguments().Select(TypeName)) + ">" : type.FullName;
        }
    }
}

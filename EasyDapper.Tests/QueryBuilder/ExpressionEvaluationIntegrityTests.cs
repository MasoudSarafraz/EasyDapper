using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class ExpressionEvaluationIntegrityTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ConstantReceiverField_PreservesPrivateAndPublicValues(bool privateField)
        {
            var owner = new FieldValues(37);
            var field = typeof(FieldValues).GetField(privateField ? "_privateAge" : "Age",
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            var fixture = new ParserFixture();
            fixture.Parse(FieldComparison(nameof(EvaluationRow.Age), Expression.Constant(owner), field));

            Assert.Equal(37, fixture.Parameters.GetParameters().Single().Value);
        }

        [Fact]
        public void ConstantReceiverField_RereadsValuesBetweenTranslations()
        {
            var owner = new FieldValues(7);
            var field = typeof(FieldValues).GetField(nameof(FieldValues.Age));
            var expression = FieldComparison(nameof(EvaluationRow.Age), Expression.Constant(owner), field);
            var fixture = new ParserFixture();
            fixture.Parse(expression);
            owner.Age = 9;
            fixture.Parse(expression);

            Assert.Equal(7, fixture.Parameters.GetParameters()["@p1"]);
            Assert.Equal(9, fixture.Parameters.GetParameters()["@p2"]);
        }

        [Fact]
        public void CapturedNullableField_PreservesNullPredicateAndBoxedUnderlyingValue()
        {
            int? captured = null;
            Expression<Func<EvaluationRow, bool>> expression = row => row.OptionalAge == captured;
            var fixture = new ParserFixture();
            Assert.EndsWith(".[OptionalAge] IS NULL", fixture.Parse(expression));
            Assert.Empty(fixture.Parameters.GetParameters());
            captured = 23;
            fixture.Parse(expression);

            Assert.Equal(23, Assert.IsType<int>(fixture.Parameters.GetParameters().Single().Value));
        }

        [Fact]
        public void CapturedEnumField_PreservesEnumTypeAndCurrentValue()
        {
            var captured = Status.Active;
            Expression<Func<EvaluationRow, bool>> expression = row => row.Status == captured;
            var fixture = new ParserFixture();
            fixture.Parse(expression);
            captured = Status.Disabled;
            fixture.Parse(expression);

            Assert.Equal(Status.Active, Assert.IsType<Status>(fixture.Parameters.GetParameters()["@p1"]));
            Assert.Equal(Status.Disabled, Assert.IsType<Status>(fixture.Parameters.GetParameters()["@p2"]));
        }

        [Fact]
        public void BaseDeclaredField_AcceptsCompatibleDerivedConstantReceiver()
        {
            var owner = new DerivedValues { BaseAge = 29 };
            var field = typeof(BaseValues).GetField(nameof(BaseValues.BaseAge));
            var fixture = new ParserFixture();
            fixture.Parse(FieldComparison(nameof(EvaluationRow.Age), Expression.Constant(owner), field));

            Assert.Equal(29, fixture.Parameters.GetParameters().Single().Value);
        }

        [Fact]
        public void FieldRead_UsesConsumerBoundaryAndCachesItsValueWithinOneTranslation()
        {
            var owner = new FieldValues(7);
            var expression = FieldComparison(nameof(EvaluationRow.Age), Expression.Constant(owner),
                typeof(FieldValues).GetField(nameof(FieldValues.Age)));
            var fixture = new ParserFixture();
            int callbacks = 0;
            fixture.Parser.EvaluateConsumerCode = read =>
            {
                callbacks++;
                owner.Age = 19;
                var value = read();
                owner.Age = 31;
                return value;
            };
            fixture.Parse(expression);

            Assert.Equal(1, callbacks);
            Assert.Equal(19, fixture.Parameters.GetParameters().Single().Value);
            Assert.Equal(31, owner.Age);
        }

        [Fact]
        public void PrimitiveConstantReceiver_KeepsCompiledValueAcrossConsumerBoundary()
        {
            // Primitive constants are embedded by the compiled expression before
            // the evaluation callback; reading their original box is different.
            var constant = Expression.Constant(31);
            var field = typeof(int).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                .Single(value => value.FieldType == typeof(int));
            var fixture = new ParserFixture();
            fixture.Parser.EvaluateConsumerCode = read =>
            {
                field.SetValue(constant.Value, 45);
                return read();
            };
            fixture.Parse(FieldComparison(nameof(EvaluationRow.Age), constant, field));

            Assert.Equal(45, constant.Value);
            Assert.Equal(31, fixture.Parameters.GetParameters().Single().Value);
        }

        [Fact]
        public void NullConstantReceiver_KeepsTheExistingWrappedNullReferenceFailure()
        {
            var fixture = new ParserFixture();
            var expression = FieldComparison(nameof(EvaluationRow.Age), Expression.Constant(null, typeof(FieldValues)),
                typeof(FieldValues).GetField(nameof(FieldValues.Age)));
            var error = Assert.Throws<NotSupportedException>(() => fixture.Parse(expression));

            Assert.StartsWith("Expression could not be evaluated independently: ", error.Message);
            Assert.IsType<NullReferenceException>(Assert.IsType<TargetInvocationException>(error.InnerException).InnerException);
            Assert.Empty(fixture.Parameters.GetParameters());
        }

        [Fact]
        public void ThrowingPropertyGetter_IsCalledOnceAndRetainsItsOriginalWrappedFailure()
        {
            var original = new InvalidOperationException("consumer getter failure");
            var owner = new CallbackValue(() => { throw original; });
            var fixture = new ParserFixture();
            var error = Assert.Throws<NotSupportedException>(() => fixture.Parse(row => row.Age == owner.Value));

            Assert.Same(original, Assert.IsType<TargetInvocationException>(error.InnerException).InnerException);
            Assert.Equal(1, owner.Reads);
            Assert.Empty(fixture.Parameters.GetParameters());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void PropertyOrMethodCallback_CanCommitNestedPredicateAndIsEvaluatedOnce(bool method)
        {
            var query = new global::EasyDapper.QueryBuilder<EvaluationRow>(new StubConnection());
            var owner = new CallbackValue(() => query.Where(row => row.Age > 40));
            if (method) query.Where(row => row.Age == owner.Read());
            else query.Where(row => row.Age == owner.Value);

            var parameters = query.GetParameterBuilder().GetParameters();
            Assert.Equal(1, owner.Reads);
            Assert.Equal(40, parameters["@p1"]);
            Assert.Equal(41, parameters["@p2"]);
            Assert.Contains(" > @p1", query.BuildQuery());
            Assert.Contains(" = @p2", query.BuildQuery());
        }

        [Fact]
        public void MethodReceiverField_PreservesReceiverSideEffectsAndPerTranslationReads()
        {
            var owner = new CallbackValue(() => { });
            var fixture = new ParserFixture();
            Expression<Func<EvaluationRow, bool>> expression = row => row.Age == owner.GetFields().Age;
            fixture.Parse(expression);
            owner.Fields.Age = 43;
            fixture.Parse(expression);

            Assert.Equal(2, owner.Reads);
            Assert.Equal(41, fixture.Parameters.GetParameters()["@p1"]);
            Assert.Equal(43, fixture.Parameters.GetParameters()["@p2"]);
        }

        [Fact]
        public void StaticAndVolatileFields_KeepCurrentValueAndConsumerEvaluationBoundary()
        {
            int originalStatic = FieldValues.StaticAge;
            try
            {
                var owner = new FieldValues(7);
                var fixture = new ParserFixture();
                int callbacks = 0;
                fixture.Parser.EvaluateConsumerCode = read =>
                {
                    callbacks++;
                    owner.VolatileAge = 13;
                    FieldValues.StaticAge = 17;
                    return read();
                };
                fixture.Parse(FieldComparison(nameof(EvaluationRow.Age), Expression.Constant(owner),
                    typeof(FieldValues).GetField(nameof(FieldValues.VolatileAge))));
                fixture.Parse(FieldComparison(nameof(EvaluationRow.Age), null,
                    typeof(FieldValues).GetField(nameof(FieldValues.StaticAge))));

                Assert.Equal(2, callbacks);
                Assert.Equal(13, fixture.Parameters.GetParameters()["@p1"]);
                Assert.Equal(17, fixture.Parameters.GetParameters()["@p2"]);
            }
            finally { FieldValues.StaticAge = originalStatic; }
        }

        private static Expression<Func<EvaluationRow, bool>> FieldComparison(string property, Expression receiver, FieldInfo field)
        {
            var row = Expression.Parameter(typeof(EvaluationRow), "row");
            return Expression.Lambda<Func<EvaluationRow, bool>>(
                Expression.Equal(Expression.Property(row, property), Expression.Field(receiver, field)), row);
        }

        private sealed class ParserFixture
        {
            internal readonly global::EasyDapper.AliasManager Aliases = new global::EasyDapper.AliasManager();
            internal readonly global::EasyDapper.ParameterBuilder Parameters = new global::EasyDapper.ParameterBuilder();
            internal readonly global::EasyDapper.ExpressionParser Parser;
            internal ParserFixture() { Parser = new global::EasyDapper.ExpressionParser(Aliases, Parameters); }
            internal string Parse(Expression<Func<EvaluationRow, bool>> expression)
                => Parser.ParseExpressionWithParameterMappingAndBrackets(expression.Body,
                    new Dictionary<ParameterExpression, string> { { expression.Parameters[0], Aliases.GetAliasForType(typeof(EvaluationRow)) } });
        }

        [Table("EvaluationRows")]
        public class EvaluationRow
        {
            public int Age { get; set; }
            public int? OptionalAge { get; set; }
            public Status Status { get; set; }
        }
        public enum Status { Active = 3, Disabled = 7 }
        private class FieldValues
        {
            private readonly int _privateAge;
            public int Age;
            public volatile int VolatileAge;
            public static int StaticAge;
            public FieldValues(int value) { Age = _privateAge = value; }
        }
        private class BaseValues { public int BaseAge; }
        private sealed class DerivedValues : BaseValues { }
        private sealed class CallbackValue
        {
            private readonly Action _callback;
            public CallbackValue(Action callback) { _callback = callback; }
            public int Reads;
            public FieldValues Fields = new FieldValues(41);
            public int Value { get { Reads++; _callback(); return 41; } }
            public int Read() { Reads++; _callback(); return 41; }
            public FieldValues GetFields() { Reads++; _callback(); return Fields; }
        }
    }
}

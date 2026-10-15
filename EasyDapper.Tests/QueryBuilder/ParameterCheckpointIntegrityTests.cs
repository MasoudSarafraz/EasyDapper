using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EasyDapper.Tests.QueryBuilder
{
    public class ParameterCheckpointIntegrityTests
    {
        [Fact]
        public void RollbackRepeatedWrites_RestoresOriginalReferencesAndNulls()
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            var original = new object();
            builder.AddParameter("@shared", original);
            builder.AddParameter("@nullable", null);
            var checkpoint = builder.CreateCheckpoint();
            builder.AddParameter("@shared", new object());
            builder.AddParameter("@shared", new object());
            builder.AddParameter("@nullable", "changed");
            builder.AddParameter("@temporary", "first");
            builder.AddParameter("@temporary", "last");
            var generated = builder.GetUniqueParameterName();
            builder.AddParameter(generated, 42);

            builder.RestoreCheckpoint(checkpoint);

            var parameters = builder.GetParameters();
            Assert.Equal(2, parameters.Count);
            Assert.Same(original, parameters["@shared"]);
            Assert.Null(parameters["@nullable"]);
            Assert.False(parameters.ContainsKey("@temporary"));
            Assert.False(parameters.ContainsKey(generated));
            Assert.Equal(generated, builder.GetUniqueParameterName());
        }

        [Fact]
        public void CommitRepeatedWrites_KeepsLatestValueAndIndependentSnapshots()
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            builder.AddParameter("@shared", "original");
            var committed = new object();
            var checkpoint = builder.CreateCheckpoint();
            builder.AddParameter("@shared", null);
            builder.AddParameter("@shared", committed);
            builder.CommitCheckpoint(checkpoint);
            var snapshot = builder.GetParameters();
            snapshot["@shared"] = "snapshot only";
            var failed = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "failed");
            builder.RestoreCheckpoint(failed);

            Assert.Same(committed, builder.GetParameters()["@shared"]);
            Assert.Equal("snapshot only", snapshot["@shared"]);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void InnerRollback_RestoresOuterWritesBeforeOuterCompletion(bool commitOuter)
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            builder.AddParameter("@shared", "original");
            var outer = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "outer");
            var outerName = builder.GetUniqueParameterName();
            builder.AddParameter(outerName, "outer parameter");
            var inner = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "inner first");
            builder.AddParameter("@shared", "inner last");
            var innerName = builder.GetUniqueParameterName();
            builder.AddParameter(innerName, "inner parameter");

            builder.RestoreCheckpoint(inner);

            Assert.Equal("outer", builder.GetParameters()["@shared"]);
            Assert.False(builder.GetParameters().ContainsKey(innerName));
            if (commitOuter) builder.CommitCheckpoint(outer);
            else builder.RestoreCheckpoint(outer);
            var parameters = builder.GetParameters();
            Assert.Equal(commitOuter ? "outer" : "original", parameters["@shared"]);
            Assert.Equal(commitOuter, parameters.ContainsKey(outerName));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void InnerCommit_SurvivesOuterRollbackAndLaterOuterOverwrite(bool writeAfterInner)
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            builder.AddParameter("@shared", "original");
            var outer = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "outer first");
            var inner = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "inner committed");
            var childName = builder.GetUniqueParameterName();
            builder.AddParameter(childName, "child parameter");
            builder.CommitCheckpoint(inner);
            if (writeAfterInner) builder.AddParameter("@shared", "outer last");

            builder.RestoreCheckpoint(outer);

            var parameters = builder.GetParameters();
            Assert.Equal("inner committed", parameters["@shared"]);
            Assert.Equal("child parameter", parameters[childName]);
        }

        [Fact]
        public void NestedMixedOutcomes_KeepSuccessfulDescendantAndRemoveBothFailedAncestors()
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            builder.AddParameter("@shared", "original");
            var outer = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "outer");
            builder.AddParameter("@outerOnly", 1);
            var middle = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "middle");
            builder.AddParameter("@middleOnly", 2);
            var inner = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "inner committed");
            builder.AddParameter("@innerOnly", 3);
            builder.CommitCheckpoint(inner);
            builder.AddParameter("@shared", "middle after child");
            builder.RestoreCheckpoint(middle);
            Assert.Equal("inner committed", builder.GetParameters()["@shared"]);
            builder.AddParameter("@shared", "outer after child");
            builder.RestoreCheckpoint(outer);

            var parameters = builder.GetParameters();
            Assert.Equal("inner committed", parameters["@shared"]);
            Assert.Equal(3, parameters["@innerOnly"]);
            Assert.False(parameters.ContainsKey("@outerOnly"));
            Assert.False(parameters.ContainsKey("@middleOnly"));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ParallelCommit_SurvivesOuterRollbackAndLaterOuterOverwrite(bool writeAfterChild)
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            builder.AddParameter("@shared", "original");
            var outer = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "outer first");
            var outerName = builder.GetUniqueParameterName();
            builder.AddParameter(outerName, "failed parameter");
            var child = Task.Run(() =>
            {
                var checkpoint = builder.CreateCheckpoint();
                builder.AddParameter("@shared", "parallel committed");
                var name = builder.GetUniqueParameterName();
                builder.AddParameter(name, "parallel parameter");
                builder.CommitCheckpoint(checkpoint);
                return name;
            });
            Drain(child);
            if (writeAfterChild) builder.AddParameter("@shared", "outer last");

            builder.RestoreCheckpoint(outer);

            var parameters = builder.GetParameters();
            Assert.Equal("parallel committed", parameters["@shared"]);
            Assert.Equal("parallel parameter", parameters[child.Result]);
            Assert.False(parameters.ContainsKey(outerName));
            Assert.Equal("@p3", builder.GetUniqueParameterName());
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public void ParallelRollbacks_DoNotResurrectAnAlreadyFailedOwnersWrite(bool existingName, bool secondWrite)
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            if (existingName) builder.AddParameter("@shared", "original");
            using (var written = new ManualResetEventSlim())
            using (var finish = new ManualResetEventSlim())
            {
                var first = builder.CreateCheckpoint();
                builder.AddParameter("@shared", "first owner");
                var second = Task.Run(() =>
                {
                    var checkpoint = builder.CreateCheckpoint();
                    builder.AddParameter("@shared", "second owner");
                    written.Set();
                    Assert.True(finish.Wait(TimeSpan.FromSeconds(5)), "The first owner did not release the second owner.");
                    if (secondWrite) builder.AddParameter("@shared", "second owner again");
                    builder.RestoreCheckpoint(checkpoint);
                });
                try
                {
                    Assert.True(written.Wait(TimeSpan.FromSeconds(5)), "The second owner did not finish its first write.");
                    builder.RestoreCheckpoint(first);
                    Assert.Equal("second owner", builder.GetParameters()["@shared"]);
                }
                finally
                {
                    finish.Set();
                    Drain(second);
                }
                var parameters = builder.GetParameters();
                if (existingName) Assert.Equal("original", parameters["@shared"]);
                else Assert.False(parameters.ContainsKey("@shared"));
            }
        }

        [Fact]
        public void AWriteWithoutCheckpoint_SurvivesAnotherOwnersRollback()
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            builder.AddParameter("@shared", "original");
            var failed = builder.CreateCheckpoint();
            builder.AddParameter("@shared", "failed first");
            var independent = Task.Run(() => builder.AddParameter("@shared", "independent committed"));
            Drain(independent);
            builder.AddParameter("@shared", "failed last");
            builder.RestoreCheckpoint(failed);

            Assert.Equal("independent committed", builder.GetParameters()["@shared"]);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void GeneratedReservation_FollowsSurvivingWritesAfterItsOriginalOwnerRollsBack(bool commitSecond)
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            using (var written = new ManualResetEventSlim())
            using (var finish = new ManualResetEventSlim())
            {
                var first = builder.CreateCheckpoint();
                var name = builder.GetUniqueParameterName();
                builder.AddParameter(name, "first owner");
                var second = Task.Run(() =>
                {
                    var checkpoint = builder.CreateCheckpoint();
                    builder.AddParameter(name, "second owner");
                    written.Set();
                    Assert.True(finish.Wait(TimeSpan.FromSeconds(5)), "The reservation owner did not release the second writer.");
                    if (commitSecond) builder.CommitCheckpoint(checkpoint);
                    else builder.RestoreCheckpoint(checkpoint);
                });
                try
                {
                    Assert.True(written.Wait(TimeSpan.FromSeconds(5)), "The second writer did not finish its write.");
                    builder.RestoreCheckpoint(first);
                    Assert.Equal("second owner", builder.GetParameters()[name]);
                }
                finally
                {
                    finish.Set();
                    Drain(second);
                }
                if (commitSecond)
                {
                    Assert.Equal("second owner", builder.GetParameters()[name]);
                    Assert.NotEqual(name, builder.GetUniqueParameterName());
                }
                else
                {
                    Assert.Empty(builder.GetParameters());
                    // Explicit allocator guarantee: with no surviving writes or
                    // reservations, a completely aborted generated name is reusable.
                    Assert.Equal(name, builder.GetUniqueParameterName());
                }
            }
        }

        [Fact]
        public void FailedMerge_RestoresDestinationAndAllowsTheSameSourceToBeMergedAgain()
        {
            var builder = new global::EasyDapper.ParameterBuilder();
            builder.AddParameter("@p1", "existing");
            var source = new System.Collections.Generic.Dictionary<string, object>
            {
                { "@p1", "imported first" }, { "@p2", "imported second" }
            };
            var checkpoint = builder.CreateCheckpoint();
            var failed = builder.MergeParameters(source);
            builder.RestoreCheckpoint(checkpoint);
            var parameters = builder.GetParameters();
            Assert.Single(parameters);
            Assert.Equal("existing", parameters["@p1"]);

            var retry = builder.CreateCheckpoint();
            var successful = builder.MergeParameters(source);
            builder.CommitCheckpoint(retry);
            parameters = builder.GetParameters();
            Assert.Equal("imported first", parameters[successful["@p1"]]);
            Assert.Equal("imported second", parameters[successful["@p2"]]);
            Assert.Equal("existing", parameters["@p1"]);
            Assert.Equal(failed["@p1"], successful["@p1"]);
            Assert.Equal(failed["@p2"], successful["@p2"]);
            Assert.Equal("imported first", source["@p1"]);
            Assert.Equal("imported second", source["@p2"]);
        }

        private static void Drain(Task task)
        {
            Assert.True(task.Wait(TimeSpan.FromSeconds(5)), "A coordinated parameter writer did not finish.");
            task.GetAwaiter().GetResult();
        }
    }
}

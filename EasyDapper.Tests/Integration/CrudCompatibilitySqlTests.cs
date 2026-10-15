using System;
using System.Data.SqlClient;
using Dapper;
using EasyDapper.Attributes;
using Xunit;

namespace EasyDapper.Tests.Integration
{
    public class CrudCompatibilitySqlTests
    {
        [SqlServerFact]
        public void Rollback_PreservesPartialUpdatesWhenAnotherWriterChangesOtherColumns()
        {
            foreach (var nested in new[] { false, true })
            {
                using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
                using (var service = DapperServiceFactory.Create(connection))
                {
                    connection.Open();
                    connection.Execute("CREATE TABLE #EDRollbackPartial (Id INT PRIMARY KEY, Name NVARCHAR(100), Age INT); INSERT #EDRollbackPartial VALUES(1,N'A',10);");
                    var entity = service.GetById<PartialRow>(1);
                    service.Attach(entity);
                    service.BeginTransaction();
                    if (nested) service.BeginTransaction();
                    entity.Name = "B";
                    Assert.Equal(1, service.Update(entity));
                    service.RollbackTransaction();
                    if (nested) service.CommitTransaction();
                    connection.Execute("UPDATE #EDRollbackPartial SET Age=99 WHERE Id=1;");

                    Assert.Equal(1, service.Update(entity));
                    var stored = service.GetById<PartialRow>(1);
                    Assert.Equal("B", stored.Name);
                    Assert.Equal(99, stored.Age);
                }
            }
        }

        [SqlServerFact]
        public void Rollback_AfterDetachAndReattach_PreservesTheExplicitPartialBaseline()
        {
            using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
            using (var service = DapperServiceFactory.Create(connection))
            {
                connection.Open();
                connection.Execute("CREATE TABLE #EDRollbackPartial (Id INT PRIMARY KEY, Name NVARCHAR(100), Age INT); INSERT #EDRollbackPartial VALUES(1,N'A',10);");
                var original = service.GetById<PartialRow>(1);
                service.Attach(original);
                service.BeginTransaction();
                original.Name = "B";
                service.Update(original);
                service.Detach(original);
                var partial = new PartialRow { Id = 1, Name = "B" };
                service.Attach(partial);
                service.RollbackTransaction();
                partial.Name = "C";

                Assert.Equal(1, service.Update(partial));
                var stored = service.GetById<PartialRow>(1);
                Assert.Equal("C", stored.Name);
                Assert.Equal(10, stored.Age);
            }
        }

        [SqlServerFact]
        public void BulkIdentityInsert_PreservesSupportForTablesWithBoundRules()
        {
            using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
            {
                connection.Open();
                connection.ChangeDatabase("tempdb");
                using (var manager = new global::EasyDapper.ConnectionManager(connection))
                {
                    manager.BeginTransaction();
                    try
                    {
                        connection.Execute("CREATE TABLE dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100));", transaction: manager.CurrentTransaction);
                        connection.Execute("CREATE RULE dbo.__EDRuleValue_064764e48e264403a0e5a06d1a529121 AS @value IS NOT NULL;", transaction: manager.CurrentTransaction);
                        connection.Execute("EXEC sys.sp_bindrule @rulename=N'dbo.__EDRuleValue_064764e48e264403a0e5a06d1a529121', @objname=N'dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121.Name';", transaction: manager.CurrentTransaction);
                        var cache = new global::EasyDapper.QueryCache();
                        var bulk = new BulkOperations(manager, cache, new global::EasyDapper.SqlBuilder(cache));
                        var rows = new[] { new RuleRow { Name = "A" }, new RuleRow { Name = "B" } };
                        Assert.Equal(2, bulk.InsertList(rows, generateIdentities: true));
                        Assert.Equal("A", connection.QueryFirst<string>("SELECT Name FROM dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 WHERE Id=@Id", new { rows[0].Id }, transaction: manager.CurrentTransaction));
                        Assert.Equal("B", connection.QueryFirst<string>("SELECT Name FROM dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 WHERE Id=@Id", new { rows[1].Id }, transaction: manager.CurrentTransaction));
                        var asyncRows = new[] { new RuleRow { Name = "C" }, new RuleRow { Name = "D" } };
                        Assert.Equal(2, bulk.InsertListAsync(asyncRows, generateIdentities: true).GetAwaiter().GetResult());
                        Assert.Equal("C", connection.QueryFirst<string>("SELECT Name FROM dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 WHERE Id=@Id", new { asyncRows[0].Id }, transaction: manager.CurrentTransaction));
                        Assert.Equal("D", connection.QueryFirst<string>("SELECT Name FROM dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 WHERE Id=@Id", new { asyncRows[1].Id }, transaction: manager.CurrentTransaction));
                    }
                    finally { manager.RollbackTransaction(); }
                }
            }
        }

        [SqlServerFact]
        public void BulkIdentityInsert_WithBoundRules_DoesNotRequireViewDefinitionPermission()
        {
            using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
            {
                connection.Open();
                connection.ChangeDatabase("tempdb");
                using (var manager = new global::EasyDapper.ConnectionManager(connection))
                {
                    manager.BeginTransaction();
                    var impersonated = false;
                    try
                    {
                        connection.Execute("CREATE TABLE dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100));", transaction: manager.CurrentTransaction);
                        connection.Execute("CREATE RULE dbo.__EDRuleValue_064764e48e264403a0e5a06d1a529121 AS @value IS NOT NULL;", transaction: manager.CurrentTransaction);
                        connection.Execute("EXEC sys.sp_bindrule @rulename=N'dbo.__EDRuleValue_064764e48e264403a0e5a06d1a529121', @objname=N'dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121.Name';", transaction: manager.CurrentTransaction);
                        connection.Execute(@"CREATE USER __EDRulePermission_064764e48e264403a0e5a06d1a529121 WITHOUT LOGIN;
                            GRANT SELECT, INSERT ON dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 TO __EDRulePermission_064764e48e264403a0e5a06d1a529121;
                            DENY VIEW DEFINITION ON OBJECT::dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 TO __EDRulePermission_064764e48e264403a0e5a06d1a529121;", transaction: manager.CurrentTransaction);
                        connection.Execute("EXECUTE AS USER=N'__EDRulePermission_064764e48e264403a0e5a06d1a529121';", transaction: manager.CurrentTransaction);
                        impersonated = true;
                        Assert.Equal(0, connection.ExecuteScalar<int>("SELECT HAS_PERMS_BY_NAME(N'dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121',N'OBJECT',N'VIEW DEFINITION')", transaction: manager.CurrentTransaction));
                        var cache = new global::EasyDapper.QueryCache();
                        var bulk = new BulkOperations(manager, cache, new global::EasyDapper.SqlBuilder(cache));
                        var rows = new[] { new RuleRow { Name = "A" }, new RuleRow { Name = "B" } };
                        Assert.Equal(2, bulk.InsertList(rows, true));
                        Assert.Equal("A", connection.QueryFirst<string>("SELECT Name FROM dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 WHERE Id=@Id", new { rows[0].Id }, transaction: manager.CurrentTransaction));
                        var asyncRows = new[] { new RuleRow { Name = "C" }, new RuleRow { Name = "D" } };
                        Assert.Equal(2, bulk.InsertListAsync(asyncRows, true).GetAwaiter().GetResult());
                        Assert.Equal("D", connection.QueryFirst<string>("SELECT Name FROM dbo.__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121 WHERE Id=@Id", new { asyncRows[1].Id }, transaction: manager.CurrentTransaction));
                    }
                    finally
                    {
                        try
                        {
                            if (impersonated) connection.Execute("REVERT;", transaction: manager.CurrentTransaction);
                        }
                        finally { manager.RollbackTransaction(); }
                    }
                }
            }
        }

        [SqlServerFact]
        public void BulkIdentityInsert_DoesNotRetryARulesErrorRaisedInsideATrigger()
        {
            using (var connection = new SqlConnection(Environment.GetEnvironmentVariable("EASYDAPPER_SQL_TEST_CONNECTION")))
            {
                connection.Open();
                connection.ChangeDatabase("tempdb");
                using (var manager = new global::EasyDapper.ConnectionManager(connection))
                {
                    manager.BeginTransaction();
                    try
                    {
                        connection.Execute("CREATE TABLE dbo.__EDTriggerMerge_064764e48e264403a0e5a06d1a529121 (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100)); CREATE TABLE dbo.__EDTriggerRuleTarget_064764e48e264403a0e5a06d1a529121 (Id INT IDENTITY PRIMARY KEY, Name NVARCHAR(100));", transaction: manager.CurrentTransaction);
                        connection.Execute("CREATE RULE dbo.__EDTriggerRuleValue_064764e48e264403a0e5a06d1a529121 AS @value IS NOT NULL;", transaction: manager.CurrentTransaction);
                        connection.Execute("EXEC sys.sp_bindrule @rulename=N'dbo.__EDTriggerRuleValue_064764e48e264403a0e5a06d1a529121', @objname=N'dbo.__EDTriggerRuleTarget_064764e48e264403a0e5a06d1a529121.Name';", transaction: manager.CurrentTransaction);
                        connection.Execute(@"CREATE TRIGGER dbo.__EDTriggerMergeError_064764e48e264403a0e5a06d1a529121 ON dbo.__EDTriggerMerge_064764e48e264403a0e5a06d1a529121 AFTER INSERT AS
                            BEGIN SET NOCOUNT ON;
                            EXEC(N'MERGE dbo.__EDTriggerRuleTarget_064764e48e264403a0e5a06d1a529121 AS Target USING (SELECT N''x'' AS Name) AS Source ON 1=0 WHEN NOT MATCHED THEN INSERT(Name) VALUES(Source.Name);');
                            END;", transaction: manager.CurrentTransaction);
                        var cache = new global::EasyDapper.QueryCache();
                        var bulk = new BulkOperations(manager, cache, new global::EasyDapper.SqlBuilder(cache));

                        var error = Assert.Throws<SqlException>(() => bulk.InsertList(new[] { new TriggerRow { Name = "A" } }, true));
                        Assert.Equal(358, error.Number);
                        Assert.True(string.IsNullOrEmpty(error.Procedure));
                    }
                    finally
                    {
                        // SQL Server can already have rolled back the transaction
                        // while propagating the trigger error, including its DDL.
                        try { manager.RollbackTransaction(); }
                        catch (InvalidOperationException) { }
                    }
                }
            }
        }

        [Table("#EDRollbackPartial")]
        public class PartialRow
        {
            [PrimaryKey] public int Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Table("__EDRuleCompatibility_064764e48e264403a0e5a06d1a529121")]
        public class RuleRow
        {
            [PrimaryKey, Identity] public int Id { get; set; }
            public string Name { get; set; }
        }

        [Table("__EDTriggerMerge_064764e48e264403a0e5a06d1a529121")]
        public class TriggerRow
        {
            [PrimaryKey, Identity] public int Id { get; set; }
            public string Name { get; set; }
        }
    }
}

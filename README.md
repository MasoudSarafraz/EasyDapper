# SQL Easy Dapper

A high-performance, feature-rich Dapper-based data access service implementation that provides an extensive set of database operations with support for transactions, bulk operations, and entity tracking.
A powerful and flexible SQL query builder implementation that supports LINQ-style syntax for building complex SQL queries with joins, aggregations, and more.

## Features

- 🚀 High-performance database operations
- 📦 Bulk insert operations
- 🔄 Transaction management with savepoints
- 🔍 Entity tracking capabilities
- 🛠 Support for stored procedures and functions
- 💾 Cached query generation
- 🔒 Thread-safe implementation
- 🎯 Async support
- 📝 Attribute-based mapping
- 💾 Support composite primary key
- 🔍 LINQ-style query syntax
- 🔄 Support for complex JOINs (INNER, LEFT, RIGHT, FULL)
- 📊 Aggregation functions (SUM, AVG, MIN, MAX, COUNT)
- 📝 GROUP BY and HAVING clauses
- 🔄 APPLY operations (CROSS APPLY, OUTER APPLY)
- 📋 Pagination support
- 🔒 SQL injection prevention
- 🎨 Expression tree parsing
- 📝 Support Row_Number, Union, UnionAll, Except and Intersect

## Complete Method Examples

### Constructor and Basic Methods

```csharp
// Initialize with connection string
IDapperService dapperService = DapperServiceFactory.Create("Your Connection String");
or
IDapperService dapperService = DapperServiceFactory.Create("Your IDbConnection object");
```
### Entity Class Example

```csharp
[Table(tableName: "Product", schema: "dbo")]
public class Product
{
    [PrimaryKey]
    [Identity]
    public int Id { get; set; }
    
    [Column("ProductName")]
    [PrimaryKey]
    public string Name { get; set; }
    
    public decimal Price { get; set; }
    
    public DateTime CreatedAt { get; set; }
}
```
### Transaction Management

```csharp
// Begin Transaction
dapperService.BeginTransaction();

// Nested transactions with savepoints
dapperService.BeginTransaction(); // First level
dapperService.BeginTransaction(); // Creates SavePoint1
dapperService.BeginTransaction(); // Creates SavePoint2

// Commit Transaction
dapperService.CommitTransaction();

// Rollback Transaction
dapperService.RollbackTransaction();

// Check transaction count
dapperService.TransactionCount();

// Example of complete transaction flow
try
{
    dapperService.BeginTransaction();
    
    var customer = new Customer { Name = "John Doe" };
    dapperService.Insert(customer);
    
    var order = new Order { CustomerId = customer.Id };
    dapperService.Insert(order);
    
    dapperService.CommitTransaction();
}
catch
{
    dapperService.RollbackTransaction();
    throw;
}
```

### Single Entity Operations

```csharp
// Insert single entity
var product = new Product { Name = "Sample Product", Price = 99.99m };
dapperService.Insert(product); // product.Id will be populated if it's an identity column

// Async Insert
await dapperService.InsertAsync(product);

// Update single entity
product.Price = 89.99m;
dapperService.Update(product);

// Async Update
await dapperService.UpdateAsync(product);

// Delete single entity
dapperService.Delete(product);

// Async Delete
await dapperService.DeleteAsync(product);

// Get entity by ID
var retrievedProduct = dapperService.GetById<Product>(1);

// Async Get by ID
var retrievedProductAsync = await dapperService.GetByIdAsync<Product>(1);

// Get by composite primary key
var compositeEntity = new CompositeKeyEntity { Key1 = 1, Key2 = "A" };
var retrieved = dapperService.GetById(compositeEntity);

// Async Get by composite key
var retrievedAsync = await dapperService.GetByIdAsync(compositeEntity);
```

### Bulk Operations

```csharp
// Insert multiple entities
var products = new List<Product>
{
    new Product { Name = "Product 1", Price = 10.99m },
    new Product { Name = "Product 2", Price = 20.99m }
};
dapperService.InsertList(products, generateIdentities: true);

// Async Insert multiple entities with generate iodentity keys and set to identity property mapped
await dapperService.InsertListAsync(products, generateIdentities: true);

// Update multiple entities
products.ForEach(p => p.Price += 5.0m);
dapperService.UpdateList(products);

// Async Update multiple entities
await dapperService.UpdateListAsync(products);

// Delete multiple entities
dapperService.DeleteList(products);

// Async Delete multiple entities
await dapperService.DeleteListAsync(products);
```

### Entity Tracking

```csharp
// Attach entity for tracking
var entity = new Product { Id = 1, Name = "Original Name", Price = 10.99m };
dapperService.Attach(entity);

// Modify tracked entity
entity.Name = "Updated Name";
entity.Price = 15.99m;

// Update will only update changed properties
dapperService.Update(entity);

// Detach entity from tracking
dapperService.Detach(entity);
```

### Stored Procedures

```csharp
// Execute stored procedure returning single result set
var results = dapperService.ExecuteStoredProcedure<Customer>(
    "GetCustomersByRegion",
    new { RegionId = 1 }
);
// Execute stored procedure with multiple result sets
// Please note that this property utilizes a local Dapper data type named **GridReader**.
// Therefore, to use this feature, you must add the **Dapper** library to your project references.

var complexResult = dapperService.ExecuteMultiResultStoredProcedure<OrderSummary>(
    "GetOrderSummary",
    reader => {
        var orders = reader.Read<Order>().ToList();
        var details = reader.Read<OrderDetail>().ToList();
        return new OrderSummary { Orders = orders, Details = details };
    },
    new { CustomerId = 1 }
);
or
var complexResult = dapperService.ExecuteMultiResultStoredProcedure(
    "usp_GetDashboardData",
    gr => new {
        Users = gr.Read<User>(),
        Orders = gr.Read<Order>(),
        Stats = gr.Read<DashboardStats>().FirstOrDefault()
    },
    new { Year = 2023 }
);

// Async multiple result sets
var complexResultAsync = await dapperService.ExecuteMultiResultStoredProcedureAsync<OrderSummary>(
    "GetOrderSummary",
    async reader => {
        var orders = (await reader.ReadAsync<Order>()).ToList();
        var details = (await reader.ReadAsync<OrderDetail>()).ToList();
        return new OrderSummary { Orders = orders, Details = details };
    },
    new { CustomerId = 1 }
);
```

### Functions

```csharp
// Execute scalar function
var totalValue = dapperService.ExecuteScalarFunction<decimal>(
    "CalculateOrderTotal",
    new { OrderId = 1 }
);

// Execute table-valued function
var orderItems = dapperService.ExecuteTableFunction<OrderItem>(
    "GetOrderItems",
    new { OrderId = 1 }
);

```
### Basic Querying

```csharp

// Simple query with where clause
var users = dapperService.Query<User>()
    .Where(u => u.Age > 18 || u.Name.Contains("m") && u.IsActive == true)
    .Execute();

// Async execution
var usersAsync = await dapperService.Query<User>()
    .Where(u => u.Age > 18)
    .ExecuteAsync();

// Execute with different result type
var dtos = dapperService.Query<User>()
    .Where(u => u.Age > 18)
    .Execute<UserDTO>();

//You can build your expressions separately and then connect them to the query at the end
var expressions = new List<Expression<Func<Person, bool>>>();
expressions.Add(x => x.PARTY_ID == 2);
expressions.Add(x=> x.IdNumber == "450");
var Query = SQL.Query<Person>();
foreach (var expression in expressions)
{
    Query = Query.Where(expression);
}
var result = Query.Execute();

```
### Select Specific Columns

```csharp
var results = dapperService.Query<User>()
    .Select(u => u.Name, u => u.Email)
    .Execute();

```
### Pagination

```csharp
var pageSize = 10;
var pageNumber = 1;
var results = dapperService.Query<User>()
    .Paging(pageSize, pageNumber)
    .Execute();
```
### Joins

```csharp
var result = dapperService.Query<Person>()
.InnerJoin<Person, Party>((x, p) => x.PARTY_ID == p.PartyId)
.Execute();

var result = dapperService.Query<Person>()
.InnerJoin<Person, Party>((x, p) => x.PARTY_ID == p.PartyId)
.LeftJoin<Person, Party>((x, p) => p.PartyId == x.PARTY_ID)
.Select<Party>(x => x.Party_Code)
.Top(10)
.Where(x => x.PARTY_ID == 129 && x.CurrentFirstName.Contains("س"))
.Execute();

```
### APPLY Operations

```csharp
// Cross Apply
var results = queryBuilder
    .CrossApply<SubQuery>((main, sub) => main.Id == sub.MainId,
        subQuery => subQuery.Where(s => s.Value > 100))
    .Execute();
// without subquery
var results = queryBuilder
    .CrossApply<SubQuery>((main, sub) => main.Id == sub.MainId,null)
    .Execute();

// Outer Apply
var results = dapperService.Query<Person>()
.InnerJoin<Person, Party>((x, p) => x.PARTY_ID == p.PartyId)
.OuterApply<Party>(
    (x, p) => x.PARTY_ID == p.PartyId,
    subQuery => subQuery.Where(a => a.PartyId == 117))
.Where(x => x.PARTY_ID == 117 && x.CurrentFirstName.Contains("J"))
.Execute();
```
### Row Numbers

```csharp
var results = dapperService.Query<Person>()
    .Row_Number(u => u.Department, u => u.Salary)
    .Execute();
```
### Set Operations

```csharp
// Distinct
var results = dapperService.Query<Person>()
    .Select(x=> x.Name)
    .Distinct()
    .Execute();

// Top
var results = dapperService.Query<Person>()
    .Top(10)
    .Execute();

// Union
var result = dapperService.Query<Person>();
var result2 = dapperService.Query<Person>().Where(x=> x.Age > 10);
var res = result.Union(result2).Execute();
```
## Complex Query Example

```csharp
var results = dapperService.Query<Person>()
    .Select(u => u.Name, u => u.Department)
    .InnerJoin<User, Department>((u, d) => u.DepartmentId == d.Id)
    .Where(u => u.Age > 25)
    .GroupBy(u => u.Department)
    .Having(g => g.Count() > 5)
    .OrderBy("Department ASC")
    .Paging(10, 1)
    .Execute<PersonDto>();
```
## Warning
Keep in mind that your code will ultimately be translated into T-SQL commands. Therefore, logically, it must align with the structure of SQL queries.
## License

This project is licensed under the MIT License - see the LICENSE file for details.

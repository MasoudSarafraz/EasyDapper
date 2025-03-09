# DapperService

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
- 💾 Support multiple primary key
- 🔍 LINQ-style query syntax
- 🔄 Support for complex JOINs (INNER, LEFT, RIGHT, FULL)
- 📊 Aggregation functions (SUM, AVG, MIN, MAX, COUNT)
- 📝 GROUP BY and HAVING clauses
- 🔄 APPLY operations (CROSS APPLY, OUTER APPLY)
- 📋 Pagination support
- 🎯 Attribute-based mapping
- 🚀 Async/await support
- 🔒 SQL injection prevention
- 🎨 Expression tree parsing
- 📝 Support Union,UnionAll,

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
[Table("Products", Schema = "dbo")]
public class Product
{
    [PrimaryKey]
    [Identity]
    public int Id { get; set; }
    
    [Column("ProductName")]
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

// Async execution
var resultsAsync = await dapperService.ExecuteStoredProcedureAsync<Customer>(
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

// Async scalar function
var totalValueAsync = await dapperService.ExecuteScalarFunctionAsync<decimal>(
    "CalculateOrderTotal",
    new { OrderId = 1 }
);

// Execute table-valued function
var orderItems = dapperService.ExecuteTableFunction<OrderItem>(
    "GetOrderItems",
    new { OrderId = 1 }
);

// Async table-valued function
var orderItemsAsync = await dapperService.ExecuteTableFunctionAsync<OrderItem>(
    "GetOrderItems",
    new { OrderId = 1 }
);
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

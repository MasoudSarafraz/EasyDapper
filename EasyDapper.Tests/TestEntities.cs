using System;
using EasyDapper.Attributes;

namespace EasyDapper.Tests
{
    /// <summary>
    /// Sample entity used by most query-builder tests. The schema and table name are explicit
    /// so that tests do not depend on the runtime type name.
    /// </summary>
    [Table("Person", "dbo")]
    public class Person
    {
        [PrimaryKey]
        [Identity]
        public int Id { get; set; }

        [Column("CurrentFirstName")]
        public string Name { get; set; }

        public int Age { get; set; }

        public int PARTY_ID { get; set; }

        public bool IsActive { get; set; }

        public DateTime? CreatedAt { get; set; }
    }

    [Table("Party", "dbo")]
    public class Party
    {
        [PrimaryKey]
        [Identity]
        public int PartyId { get; set; }

        public string Party_Code { get; set; }

        public string Code { get; set; }
    }

    [Table("Employee", "dbo")]
    public class Employee
    {
        [PrimaryKey]
        [Identity]
        public int Id { get; set; }

        public string Name { get; set; }

        public int? ManagerId { get; set; }

        public decimal Salary { get; set; }
    }

    [Table("Customer", "dbo")]
    public class Customer
    {
        [PrimaryKey]
        [Identity]
        public int Id { get; set; }

        public string Name { get; set; }
    }

    [Table("Order", "dbo")]
    public class Order
    {
        [PrimaryKey]
        [Identity]
        public int Id { get; set; }

        public int CustomerId { get; set; }

        public int? BillingCustomerId { get; set; }

        public int? ProductId { get; set; }

        public int? SpecialItemId { get; set; }

        public DateTime OrderDate { get; set; }
    }

    [Table("Product", "dbo")]
    public class Product
    {
        [PrimaryKey]
        [Identity]
        public int Id { get; set; }

        public string Name { get; set; }

        public decimal Price { get; set; }
    }

    [Table("OrderItem", "dbo")]
    public class OrderItem
    {
        [PrimaryKey]
        [Identity]
        public int Id { get; set; }

        public int OrderId { get; set; }

        public int Quantity { get; set; }
    }

    /// <summary>
    /// Entity with a composite primary key, used to exercise composite-key code paths.
    /// </summary>
    [Table("CompositeEntity", "dbo")]
    public class CompositeEntity
    {
        [PrimaryKey]
        public int Key1 { get; set; }

        [PrimaryKey]
        public string Key2 { get; set; }

        public string Data { get; set; }
    }

    /// <summary>
    /// Entity used to verify FIX (B12): the previous code used member.DeclaringType which broke
    /// for entities that inherit from a generic base. By using paramExpr.Type we now resolve
    /// aliases using the actual lambda parameter type.
    /// </summary>
    [Table("DerivedItem", "dbo")]
    public class DerivedItem
    {
        [PrimaryKey]
        [Identity]
        public int Id { get; set; }

        public string Name { get; set; }
    }
}

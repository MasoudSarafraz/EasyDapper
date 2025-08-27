//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//using EasyDapper.Attributes;

//namespace EasyDapper.Tests
//{
//    [Table("Users", "dbo")]
//    public class User
//    {
//        [Column("Id")]
//        public int Id { get; set; }
//        [Column("Name")]
//        public string Name { get; set; }
//        [Column("Age")]
//        public int Age { get; set; }
//        public bool IsActive { get; set; }
//        public int Amount { get; internal set; }
//    }

//    [Table("Orders", "dbo")]
//    public class Order
//    {
//        [Column("OrderId")]
//        public int OrderId { get; set; }
//        [Column("UserId")]
//        public int UserId { get; set; }
//        [Column("Amount")]
//        public decimal Amount { get; set; }
//    }
//}

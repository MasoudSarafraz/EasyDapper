using EasyDapper.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsFormsApp2
{
    [Table("Users","dbo")]
    public class User
    {
        [PrimaryKey]
        [Identity]
        [Column("Id")]
        public int UserId { get; set; }
        [Column("Name2")]
        public string Name { get; set; }
        public string Family { get; set; }
        public DateTime CreateDate { get; set; }
    }
}

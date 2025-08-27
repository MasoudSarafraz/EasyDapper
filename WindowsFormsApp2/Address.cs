using EasyDapper.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsFormsApp2
{
    [Table("Address", "dbo")]
    public class Address
    {
        [PrimaryKey]
        [Identity]
        public int AddressId { get; set; }
        public string AddressDesc { get; set; }
        public string PhoneNumber { get; set; }
        public int UserId { get; set; }
    }
}

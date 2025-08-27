using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WindowsFormsApp2
{
    public class UserDto
    {
        public int UserId { get; set; }
        public string AddressDesc { get; set; }
        public string JobDesc { get; set; }
        public DateTime CreateDate { get; set; }
    }
}

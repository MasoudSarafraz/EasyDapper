using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using EasyDapper;
namespace WindowsFormsApp2
{
    public partial class Form1 : Form
    {
        IDapperService SQL;
        public Form1()
        {
            SQL = DapperServiceFactory.Create("Server=.;Database=Test;User Id=sa;Password=Masoud7921463;");
            InitializeComponent();
        }

        private async void button1_Click(object sender, EventArgs e)
        {
            var bs = SQL.GetById<User>(84300);
            bs.Family = "Testلاتغع";
            SQL.Update(bs);
            bs.UserId = 0;
            SQL.Insert(bs);
            var result = SQL.Query<User>()
                .LeftJoin<User, Address>((u, ad) => u.UserId == ad.UserId)
                .LeftJoin<User, Address>((u, a) => u.UserId == a.UserId)
                //.InnerJoin<Address, UserJob>((a, u) => a.UserId == u.UserId)
                .Select<Address>(x => x.AddressDesc, x => x.AddressId)
                //.Select<UserJob>(j => j.JobDesc, j => j.UserJobId)
                .Select<User>(x => x.CreateDate, x => x.UserId)
                .Where(x => x.UserId == 84300)
                .Top(10).Execute();
            SQL.Delete(result);
            var aaaa = GenerateUser(10000);
            SQL.InsertList(aaaa, true);
            //var a = aaaa.FirstOrDefault(x => x.UserId == 211988);
            //SQL.Attach(bs);
            //bs.PhoneNumber = "09127921463";
            //bs.AddressDesc = "جهت تست2 جوین ها";
            //var user = SQL.GetById<User>(200001);
            //SQL.Attach(user);
            //user.CreateDate = DateTime.Now;
            //await SQL.UpdateAsync(user);
            //await SQL.UpdateAsync(bs);
        }
        private static List<BulkUser> GenerateBulkUser(int count)
        {
            var products = new List<BulkUser>();
            var random = new Random();

            for (int i = 1; i <= count; i++)
            {
                products.Add(new BulkUser
                {
                    UserId = i,
                    Name = "Masoud" + random.Next(10, 10000000).ToString(),
                    CreateDate = DateTime.UtcNow
                });
            }
            
            return products;
        }
        private static List<User> GenerateUser(int count)
        {
            var products = new List<User>();
            var random = new Random();

            for (int i = 1; i <= count; i++)
            {
                products.Add(new User
                {
                    Name = "Masoud" + random.Next(10, 10000000).ToString(),
                    CreateDate = DateTime.UtcNow
                });
            }

            return products;
        }
    }
}

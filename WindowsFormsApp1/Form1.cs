using EasyDapper;
using EasyDapper.Attributes;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace WindowsFormsApp1
{
    public partial class Form1 : Form
    {
        private IDapperService SQL;
        public Form1()
        {
            SQL = DapperServiceFactory.Create("Server=localhost;Database=Test;User Id=sa;Password=Masoud7921463;Pooling=true;");
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            var result = SQL.QueryBuilder<User>().Execute();
            var items = Enumerable.Range(1, 2)
            .Select(i => new User { Name = $"Test{i.ToString()}", Create_Date = DateTime.Now })
            .ToList();
            User user = items[0];
            var sw = Stopwatch.StartNew();
            SQL.BeginTransaction();
            SQL.Insert(user);
            SQL.BeginTransaction();
            SQL.InsertList(items);
            SQL.CommitTransaction();
            SQL.CommitTransaction();
            sw.Stop();
            int j = 1;
            var aaa = sw.ElapsedMilliseconds;
            foreach (var item in items)
            {

                item.Name = "Masoud" + j.ToString();
                j++;
            }
            SQL.UpdateList(items);
        }
    }
    public class User
    {
        [Identity]
        [PrimaryKey]
        public int UserId { get; set; }
        public string Name { get; set; }
        public string Family { get; set; }
        [Column("CreateDate")]
        public DateTime Create_Date { get; set; }
    }
    public class UserDto
    {
        public int SumId { get; set; }
        public string Name { get; set; }
        public string Family { get; set; }
    }
}

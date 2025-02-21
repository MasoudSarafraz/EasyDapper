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
namespace WindowsFormsControlLibrary1
{
    public partial class Form1 : Form
    {
        private IDapperService SQL;
        public Form1()
        {
            SQL = DapperServiceFactory.Create("Provider=SQLOLEDB.1;Persist Security Info=False;User ID=sa;Initial Catalog=Test;Data Source=.");
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            var result = SQL.QueryBuilder<User>().Where(x => x.UserId == 1).Execute();
        }
    }
}

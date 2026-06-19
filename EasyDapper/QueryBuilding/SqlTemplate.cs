using System.Collections.Generic;

namespace EasyDapper
{
    internal class SqlTemplate
    {
        public string Sql { get; set; }
        public List<string> LocalParameterNames { get; set; }
    }
}

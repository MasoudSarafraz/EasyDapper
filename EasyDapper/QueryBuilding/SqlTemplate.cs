using System.Collections.Generic;

namespace EasyDapper
{
    /// <summary>
    /// Holds a parsed SQL template plus the ordered list of local parameter names that the
    /// template references. Used by the expression-template cache so that an expression with the
    /// same shape but different constant values can reuse the cached SQL template and only pay
    /// the cost of binding new parameter values.
    /// </summary>
    internal class SqlTemplate
    {
        public string Sql { get; set; }
        public List<string> LocalParameterNames { get; set; }
    }
}

using System;
using System.Text.RegularExpressions;

namespace EasyDapper
{
    /// <summary>Opt-in validation for callers accepting column ordering from external input.</summary>
    public static class QueryBuilderSafetyExtensions
    {
        private static readonly Regex ColumnOrderPattern = new Regex(
            @"\A\s*(?:[\p{L}_#][\p{L}\p{N}_@$#]*|\[(?:[^\]\r\n]|\]\])+\])(?:\s*\.\s*(?:[\p{L}_#][\p{L}\p{N}_@$#]*|\[(?:[^\]\r\n]|\]\])+\]))*(?:\s+(?:ASC|DESC))?(?:\s*,\s*(?:[\p{L}_#][\p{L}\p{N}_@$#]*|\[(?:[^\]\r\n]|\]\])+\])(?:\s*\.\s*(?:[\p{L}_#][\p{L}\p{N}_@$#]*|\[(?:[^\]\r\n]|\]\])+\]))*(?:\s+(?:ASC|DESC))?)*\s*\z",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        /// <summary>Adds only a comma-separated list of column identifiers and optional ASC/DESC.</summary>
        public static IQueryBuilder<T> OrderByColumns<T>(this IQueryBuilder<T> query, string columns)
        {
            if (query == null) throw new ArgumentNullException("query");
            if (string.IsNullOrEmpty(columns)) return query;
            if (columns.IndexOf(';') >= 0 || columns.Contains("--") || columns.Contains("/*") || columns.Contains("*/")
                || !ColumnOrderPattern.IsMatch(columns))
                throw new ArgumentException("Ordering accepts only comma-separated column identifiers with optional ASC or DESC.", "columns");
            return query.OrderBy(columns);
        }
    }
}

using EasyDapper.Attributes;

namespace EasyDapper.LoadTests;

// This GUID is generated once for the compiled consumer. Each process creates and
// drops this fixture in tempdb under an exclusive session application lock.
// Keeping the CLR model fixed permits DLL-only before/after comparisons.
[Table(Fixture.TableName, "dbo")]
public sealed class LoadRow
{
    [PrimaryKey]
    public int Id { get; set; }
    public long Value { get; set; }
    public string Payload { get; set; } = "";
}

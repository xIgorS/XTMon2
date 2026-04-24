using Microsoft.Data.SqlClient;
using Moq;
using System.Data;
using XTMon.Helpers;

namespace XTMon.Tests.Helpers;

public class SqlDataHelperTests
{
    // ─── TryReadDateOnly ────────────────────────────────────────────────────────

    [Fact]
    public void TryReadDateOnly_WhenNull_ReturnsFalse()
    {
        var reader = MakeReader(DBNull.Value);
        var result = SqlDataHelper.TryReadDateOnly(reader, 0, out var date);
        Assert.False(result);
        Assert.Equal(default, date);
    }

    [Fact]
    public void TryReadDateOnly_WhenDateTime_ReturnsDateOnly()
    {
        var dt = new DateTime(2025, 3, 15);
        var reader = MakeReader(dt);
        var result = SqlDataHelper.TryReadDateOnly(reader, 0, out var date);
        Assert.True(result);
        Assert.Equal(new DateOnly(2025, 3, 15), date);
    }

    [Fact]
    public void TryReadDateOnly_WhenDateOnly_ReturnsSame()
    {
        var expected = new DateOnly(2025, 6, 1);
        var reader = MakeReader(expected);
        var result = SqlDataHelper.TryReadDateOnly(reader, 0, out var date);
        Assert.True(result);
        Assert.Equal(expected, date);
    }

    [Fact]
    public void TryReadDateOnly_WhenValidString_ParsesDate()
    {
        var reader = MakeReader("2025-12-31");
        var result = SqlDataHelper.TryReadDateOnly(reader, 0, out var date);
        Assert.True(result);
        Assert.Equal(new DateOnly(2025, 12, 31), date);
    }

    [Fact]
    public void TryReadDateOnly_WhenInvalidString_ReturnsFalse()
    {
        var reader = MakeReader("not-a-date");
        var result = SqlDataHelper.TryReadDateOnly(reader, 0, out _);
        Assert.False(result);
    }

    // ─── ParseQuery ─────────────────────────────────────────────────────────────

    [Fact]
    public void ParseQuery_WhenNull_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, SqlDataHelper.ParseQuery(null));
    }

    [Fact]
    public void ParseQuery_WhenDbNull_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, SqlDataHelper.ParseQuery(DBNull.Value));
    }

    [Fact]
    public void ParseQuery_WhenWhitespace_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, SqlDataHelper.ParseQuery("   "));
    }

    [Fact]
    public void ParseQuery_NormalizesCarriageReturnLineFeed()
    {
        var input = "SELECT 1\r\nSELECT 2";
        var result = SqlDataHelper.ParseQuery(input);
        Assert.Equal("SELECT 1\nSELECT 2", result);
    }

    [Fact]
    public void ParseQuery_NormalizesCarriageReturn()
    {
        var input = "SELECT 1\rSELECT 2";
        var result = SqlDataHelper.ParseQuery(input);
        Assert.Equal("SELECT 1\nSELECT 2", result);
    }

    [Fact]
    public void ParseQuery_SingleLineSemicolon_SplitsIntoLines()
    {
        var input = "SELECT 1; SELECT 2";
        var result = SqlDataHelper.ParseQuery(input);
        Assert.Equal("SELECT 1;\nSELECT 2", result);
    }

    [Fact]
    public void ParseQuery_MultilineInput_DoesNotSplitOnSemicolon()
    {
        // When there are already newlines, semicolons are not split further
        var input = "SELECT 1;\nSELECT 2";
        var result = SqlDataHelper.ParseQuery(input);
        Assert.Equal("SELECT 1;\nSELECT 2", result);
    }

    [Fact]
    public void ParseQuery_TrimsLeadingAndTrailingWhitespace()
    {
        var result = SqlDataHelper.ParseQuery("  SELECT 1  ");
        Assert.Equal("SELECT 1", result);
    }

    // ─── ReadBoolean ────────────────────────────────────────────────────────────

    [Fact]
    public void ReadBoolean_WhenNull_ReturnsFalse()
    {
        var reader = MakeReader(DBNull.Value);
        Assert.False(SqlDataHelper.ReadBoolean(reader, 0));
    }

    [Fact]
    public void ReadBoolean_WhenTrue_ReturnsTrue()
    {
        var reader = MakeReader(true);
        Assert.True(SqlDataHelper.ReadBoolean(reader, 0));
    }

    [Fact]
    public void ReadBoolean_WhenFalse_ReturnsFalse()
    {
        var reader = MakeReader(false);
        Assert.False(SqlDataHelper.ReadBoolean(reader, 0));
    }

    [Fact]
    public void ReadBoolean_WhenNonZeroInt_ReturnsTrue()
    {
        var reader = MakeReader(1);
        Assert.True(SqlDataHelper.ReadBoolean(reader, 0));
    }

    [Fact]
    public void ReadBoolean_WhenZeroInt_ReturnsFalse()
    {
        var reader = MakeReader(0);
        Assert.False(SqlDataHelper.ReadBoolean(reader, 0));
    }

    // ─── ReadNullableString ──────────────────────────────────────────────────────

    [Fact]
    public void ReadNullableString_WhenOrdinalNull_ReturnsNull()
    {
        var reader = MakeReader("value");
        Assert.Null(SqlDataHelper.ReadNullableString(reader, null));
    }

    [Fact]
    public void ReadNullableString_WhenDbNull_ReturnsNull()
    {
        var reader = MakeReader(DBNull.Value);
        Assert.Null(SqlDataHelper.ReadNullableString(reader, 0));
    }

    [Fact]
    public void ReadNullableString_WhenString_ReturnsString()
    {
        var reader = MakeReader("hello");
        Assert.Equal("hello", SqlDataHelper.ReadNullableString(reader, 0));
    }

    // ─── ReadNullableDateTime ────────────────────────────────────────────────────

    [Fact]
    public void ReadNullableDateTime_WhenOrdinalNull_ReturnsNull()
    {
        var reader = MakeReader(DateTime.Now);
        Assert.Null(SqlDataHelper.ReadNullableDateTime(reader, null));
    }

    [Fact]
    public void ReadNullableDateTime_WhenDbNull_ReturnsNull()
    {
        var reader = MakeReader(DBNull.Value);
        Assert.Null(SqlDataHelper.ReadNullableDateTime(reader, 0));
    }

    [Fact]
    public void ReadNullableDateTime_WhenDateTime_ReturnsValue()
    {
        var expected = new DateTime(2025, 1, 15, 12, 30, 0);
        var reader = MakeReader(expected);
        Assert.Equal(expected, SqlDataHelper.ReadNullableDateTime(reader, 0));
    }

    // ─── FindOrdinal ────────────────────────────────────────────────────────────

    [Fact]
    public void FindOrdinal_WhenColumnExists_ReturnsIndex()
    {
        var reader = MakeMultiColumnReader("FlowId", "PnlDate", "Status");
        Assert.Equal(1, SqlDataHelper.FindOrdinal(reader, "PnlDate"));
    }

    [Fact]
    public void FindOrdinal_WhenColumnExistsCaseInsensitive_ReturnsIndex()
    {
        var reader = MakeMultiColumnReader("FlowId", "PnlDate", "Status");
        Assert.Equal(2, SqlDataHelper.FindOrdinal(reader, "status"));
    }

    [Fact]
    public void FindOrdinal_WhenColumnMissing_ReturnsNull()
    {
        var reader = MakeMultiColumnReader("FlowId", "PnlDate");
        Assert.Null(SqlDataHelper.FindOrdinal(reader, "MissingColumn"));
    }

    // ─── IsSqlTimeout ───────────────────────────────────────────────────────────

    [Fact]
    public void IsSqlTimeout_WhenNumberIsNegativeTwo_ReturnsTrue()
    {
        var ex = MakeSqlException(-2);
        Assert.True(SqlDataHelper.IsSqlTimeout(ex));
    }

    [Fact]
    public void IsSqlTimeout_WhenOtherNumber_ReturnsFalse()
    {
        var ex = MakeSqlException(53);
        Assert.False(SqlDataHelper.IsSqlTimeout(ex));
    }

    [Fact]
    public void IsSqlLockTimeout_WhenNumberIs1222_ReturnsTrue()
    {
        var ex = MakeSqlException(1222);
        Assert.True(SqlDataHelper.IsSqlLockTimeout(ex));
    }

    [Fact]
    public void IsSqlLockTimeout_WhenOtherNumber_ReturnsFalse()
    {
        var ex = MakeSqlException(-2);
        Assert.False(SqlDataHelper.IsSqlLockTimeout(ex));
    }

    // ─── IsSqlConnectionFailure ──────────────────────────────────────────────────

    [Theory]
    [InlineData(-1)]
    [InlineData(2)]
    [InlineData(20)]
    [InlineData(53)]
    [InlineData(64)]
    [InlineData(233)]
    [InlineData(4060)]
    [InlineData(10054)]
    [InlineData(10060)]
    public void IsSqlConnectionFailure_WhenKnownConnectionErrorNumber_ReturnsTrue(int number)
    {
        var ex = MakeSqlException(number);
        Assert.True(SqlDataHelper.IsSqlConnectionFailure(ex));
    }

    [Theory]
    [InlineData(-2)]   // timeout, not connection failure
    [InlineData(0)]
    [InlineData(8152)] // string truncation
    public void IsSqlConnectionFailure_WhenOtherNumber_ReturnsFalse(int number)
    {
        var ex = MakeSqlException(number);
        Assert.False(SqlDataHelper.IsSqlConnectionFailure(ex));
    }

    [Fact]
    public void IsSqlDeadlock_WhenNumberIs1205_ReturnsTrue()
    {
        var ex = MakeSqlException(1205);
        Assert.True(SqlDataHelper.IsSqlDeadlock(ex));
    }

    [Fact]
    public void IsSqlDeadlock_WhenOtherNumber_ReturnsFalse()
    {
        var ex = MakeSqlException(53);
        Assert.False(SqlDataHelper.IsSqlDeadlock(ex));
    }

    // ─── Helpers ────────────────────────────────────────────────────────────────

    private static IDataRecord MakeReader(object value)
    {
        var mock = new Mock<IDataRecord>();
        mock.Setup(r => r.IsDBNull(0)).Returns(value == DBNull.Value);
        mock.Setup(r => r.GetValue(0)).Returns(value);
        return mock.Object;
    }

    private static IDataRecord MakeMultiColumnReader(params string[] columnNames)
    {
        var mock = new Mock<IDataRecord>();
        mock.Setup(r => r.FieldCount).Returns(columnNames.Length);
        for (var i = 0; i < columnNames.Length; i++)
        {
            var index = i;
            mock.Setup(r => r.GetName(index)).Returns(columnNames[index]);
        }
        return mock.Object;
    }

    /// <summary>
    /// Creates a SqlException with the given error number via reflection (SqlException has no public ctor).
    /// Adapts to the actual internal constructor signatures of the installed SqlClient version.
    /// </summary>
    private static SqlException MakeSqlException(int number, string message = "msg")
    {
        const System.Reflection.BindingFlags nonPublicInstance =
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance;

        // Build SqlError: map each parameter type to a sensible default value
        var errorCtor = typeof(SqlError).GetConstructors(nonPublicInstance)[0];
        var errorArgs = BuildArgs(errorCtor.GetParameters(), new Dictionary<Type, object?>
        {
            [typeof(int)]    = number,
            [typeof(byte)]   = (byte)0,
            [typeof(string)] = message,
            [typeof(uint)]   = (uint)0,
            [typeof(Exception)] = null,
        });
        var error = (SqlError)errorCtor.Invoke(errorArgs);

        // Build SqlErrorCollection
        var collectionCtors = typeof(SqlErrorCollection).GetConstructors(nonPublicInstance);
        var errors = (SqlErrorCollection)(collectionCtors.Length > 0
            ? collectionCtors[0].Invoke(Array.Empty<object>())
            : Activator.CreateInstance(typeof(SqlErrorCollection), nonPublicInstance, null, null, null)!);

        var addMethod = typeof(SqlErrorCollection).GetMethod("Add", nonPublicInstance)!;
        addMethod.Invoke(errors, new object[] { error });

        // Build SqlException
        var exCtor = typeof(SqlException).GetConstructors(nonPublicInstance)[0];
        var exArgs = BuildArgs(exCtor.GetParameters(), new Dictionary<Type, object?>
        {
            [typeof(string)]           = message,
            [typeof(SqlErrorCollection)] = errors,
            [typeof(Exception)]        = null,
            [typeof(Guid)]             = Guid.NewGuid(),
        });
        return (SqlException)exCtor.Invoke(exArgs);
    }

    private static object?[] BuildArgs(
        System.Reflection.ParameterInfo[] parameters,
        Dictionary<Type, object?> overrides)
    {
        var args = new object?[parameters.Length];
        var typeUsageCount = new Dictionary<Type, int>();

        for (var i = 0; i < parameters.Length; i++)
        {
            var t = parameters[i].ParameterType;
            // Strip nullable reference types (they're the same at runtime)
            if (overrides.TryGetValue(t, out var val))
            {
                // Only use each override once if there are duplicate types; after first use keep the same
                args[i] = val;
            }
            else if (t.IsValueType)
            {
                args[i] = Activator.CreateInstance(t);
            }
            else
            {
                args[i] = null;
            }
        }

        return args;
    }
}

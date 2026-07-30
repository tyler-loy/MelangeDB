namespace MelangeDB;

/// <summary>Marks the primary key column of a table. Exactly one per table.</summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public sealed class PrimaryKeyAttribute : Attribute;

/// <summary>Marks a column as secondarily indexed for equality and range lookups.</summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public sealed class IndexAttribute : Attribute;

/// <summary>Marks a column whose value must be unique across the table. Implies an index.</summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public sealed class UniqueAttribute : Attribute;

/// <summary>
/// Marks a <see cref="long"/> or <see cref="ulong"/> column whose value is assigned from a durable
/// per-table sequence when a row is inserted with the value left at zero. The contract is
/// <b>unique, not dense</b>: gaps are normal. Ids are 64-bit but allocated within 63 bits
/// (sign bit clear, 16-bit originator, 47-bit sequence) so a value round-trips through Postgres
/// <c>bigint</c> and signed-only client languages unchanged. A value assigned by an aborted
/// transaction is never observed.
/// </summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
public sealed class AutoIncAttribute : Attribute;

# Implementation Plan: Cell Type Conversion Utilities

This plan implements `TryFrom<Cell>` conversions for extracting primitive Rust types from the `Cell` enum, as outlined in TODO.md.

## Goal

Enable downstream consumers to easily convert `Cell` values to native Rust types without manual pattern matching:

```rust
let value: i32 = cell.try_into()?;           // Extract i32 from Cell::I32
let value: Option<String> = cell.try_into()?; // Handle nullable String
let values: Vec<Option<i32>> = cell.try_into()?; // Extract array contents
```

## Current State

- `etl/src/types/cell.rs` has `Cell` and `ArrayCell` enums with 17 variants each
- Existing `TryFrom<Cell> for CellNonOptional` converts between Cell variants (not to underlying types)
- No direct extraction to primitive types like `i32`, `String`, etc.

## Implementation Steps

### Step 1: Add Dependencies to `etl/Cargo.toml`

Add the following workspace dependencies:

```toml
derive_more = { workspace = true, features = ["try_into"] }
trait-gen = { workspace = true }
```

First, verify/add these to the root `Cargo.toml` workspace dependencies:
```toml
derive_more = { version = "1", default-features = false }
trait-gen = "0.3"
```

### Step 2: Add `#[derive(TryInto)]` to Cell Enum

In `etl/src/types/cell.rs`, modify the `Cell` enum:

```rust
use derive_more::TryInto;

#[derive(Debug, Clone, PartialEq, TryInto)]
#[try_into(owned, ref, ref_mut)]
pub enum Cell {
    #[try_into(ignore)]
    Null,
    Bool(bool),
    String(String),
    I16(i16),
    I32(i32),
    U32(u32),
    I64(i64),
    F32(f32),
    F64(f64),
    Numeric(PgNumeric),
    Date(NaiveDate),
    Time(NaiveTime),
    Timestamp(NaiveDateTime),
    TimestampTz(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    #[try_into(ignore)]
    Array(ArrayCell),
}
```

This generates:
- `TryFrom<Cell> for bool`
- `TryFrom<Cell> for String`
- `TryFrom<Cell> for i16`, `i32`, `u32`, `i64`
- `TryFrom<Cell> for f32`, `f64`
- etc.

### Step 3: Add `#[derive(TryInto)]` to ArrayCell Enum

```rust
#[derive(Debug, Clone, PartialEq, TryInto)]
#[try_into(owned, ref, ref_mut)]
pub enum ArrayCell {
    Bool(Vec<Option<bool>>),
    String(Vec<Option<String>>),
    // ... all variants
}
```

This generates `TryFrom<ArrayCell> for Vec<Option<T>>` for each type.

### Step 4: Implement `TryFrom<Cell>` for `Option<T>` Variants

Use `trait_gen` macro to generate implementations for nullable extraction:

```rust
use trait_gen::trait_gen;

#[trait_gen(T -> bool, String, i16, i32, u32, i64, f32, f64, PgNumeric,
            NaiveDate, NaiveTime, NaiveDateTime, DateTime<Utc>, Uuid,
            serde_json::Value, Vec<u8>)]
impl TryFrom<Cell> for Option<T> {
    type Error = EtlError;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Null => Ok(None),
            other => Ok(Some(other.try_into()?)),
        }
    }
}
```

### Step 5: Implement `TryFrom<Cell>` for `Vec<Option<T>>` (Array Extraction)

```rust
impl TryFrom<Cell> for Vec<Option<bool>> {
    type Error = EtlError;

    fn try_from(cell: Cell) -> Result<Self, Self::Error> {
        match cell {
            Cell::Array(ArrayCell::Bool(vec)) => Ok(vec),
            _ => bail!(ErrorKind::TypeMismatch, "Expected bool array", "..."),
        }
    }
}
// Repeat for all array types, or use trait_gen if possible
```

### Step 6: Add Unit Tests

Add tests in `etl/src/types/cell.rs`:

```rust
#[test]
fn test_try_from_cell_to_primitive() {
    let cell = Cell::I32(42);
    let value: i32 = cell.try_into().unwrap();
    assert_eq!(value, 42);
}

#[test]
fn test_try_from_cell_to_option() {
    let cell = Cell::I32(42);
    let value: Option<i32> = cell.try_into().unwrap();
    assert_eq!(value, Some(42));

    let cell = Cell::Null;
    let value: Option<i32> = cell.try_into().unwrap();
    assert_eq!(value, None);
}

#[test]
fn test_try_from_cell_type_mismatch() {
    let cell = Cell::String("hello".to_string());
    let result: Result<i32, _> = cell.try_into();
    assert!(result.is_err());
}

#[test]
fn test_try_from_array_cell() {
    let cell = Cell::Array(ArrayCell::I32(vec![Some(1), Some(2), None]));
    let values: Vec<Option<i32>> = cell.try_into().unwrap();
    assert_eq!(values, vec![Some(1), Some(2), None]);
}
```

### Step 7: Verify Build and Run Tests

```bash
cargo build -p etl
cargo test -p etl
```

## Files to Modify

| File | Changes |
|------|---------|
| `Cargo.toml` (workspace root) | Add `derive_more` and `trait-gen` to workspace dependencies |
| `etl/Cargo.toml` | Add `derive_more` and `trait-gen` dependencies |
| `etl/src/types/cell.rs` | Add derives, implement TryFrom traits |

## Error Handling

The existing `EtlError` type with `ErrorKind::TypeMismatch` (or similar) should be used for type conversion failures. May need to add a new error variant if one doesn't exist:

```rust
ErrorKind::CellConversionError
```

## Verification Checklist

- [ ] Dependencies added to workspace Cargo.toml
- [ ] Dependencies added to etl/Cargo.toml
- [ ] `#[derive(TryInto)]` added to `Cell` enum
- [ ] `#[derive(TryInto)]` added to `ArrayCell` enum
- [ ] `TryFrom<Cell> for Option<T>` implemented for all types
- [ ] `TryFrom<Cell> for Vec<Option<T>>` implemented for array extraction
- [ ] Unit tests added and passing
- [ ] `cargo build -p etl` succeeds
- [ ] `cargo test -p etl` passes

## Notes

- The `Null` and `Array` variants are ignored in the derive macro since they don't map directly to a single primitive type
- `Option<T>` conversions handle `Cell::Null` gracefully by returning `None`
- Array extraction provides access to the inner `Vec<Option<T>>` for further processing

# Make the same forked changes from pg_replicate onto this repo

Review ../pg_replicate/FORK_CHANGES.diff as a reference.

## Theme 1: Cell Type Conversion Utilities

### Changes in pg_replicate
- Added `derive_more::TryInto` derive macro to `Cell` and `ArrayCell` enums
- Added `trait_gen` crate to generate `TryFrom<Cell>` implementations for all wrapped types
- Added `TryFrom<Cell>` for `Option<T>` variants (to handle nullable values)
- Added `TryFrom<Cell>` for `Vec<Option<T>>` to extract array contents
- Added optional `rust_decimal::Decimal` support with feature flag

### Status in etl
- **NOT IMPLEMENTED** - etl's `Cell` enum (in `etl/src/types/cell.rs`) does not have TryFrom implementations to extract inner values directly
- etl has `TryFrom<Cell> for CellNonOptional` and `TryFrom<ArrayCell> for ArrayCellNonOptional` but these convert between Cell variants, not to the underlying Rust types

### Recommendation
**NEEDED** - Add TryFrom implementations for extracting primitive types from Cell. This enables downstream consumers (like btreemapped) to easily convert cells to native Rust types without pattern matching.

Example usage after implementation:
```rust
let value: i32 = cell.try_into()?;  // Extract i32 from Cell::I32
let value: Option<String> = cell.try_into()?;  // Handle nullable String
```

### Implementation Approach
1. Add `derive_more` dependency to `etl` crate with `TryInto` feature
2. Add `#[derive(TryInto)]` to `Cell` and `ArrayCell` enums
3. Add `#[try_into(ignore)]` attribute to `Cell::Null` and `Cell::Array` variants
4. Use `trait_gen` macro to generate `TryFrom<Cell>` for `Option<T>` for all types
5. Generate `TryFrom<Cell>` for `Vec<Option<T>>` to handle array extraction

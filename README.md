# OxideDB - A Lightweight Database Engine in Rust

A custom database engine implementation in Rust featuring B+ Tree indexing, persistent storage, and a SQL-like interface for basic database operations.

## 🚀 Features

- **B+ Tree Indexing**: Efficient tree-based indexing for fast data retrieval
- **Persistent Storage**: Data persistence across application restarts
- **Multi-type Support**: Support for INTEGER, BIGINT, DOUBLE, FLOAT, and STRING data types
- **Table Management**: Create and manage multiple tables with schema validation
- **CRUD Operations**: Insert, select, and query operations
- **Memory Management**: LRU cache implementation for optimized memory usage
- **Universal Key System**: Generic key handling for different data types

## 📁 Project Structure

```
src/
├── main.rs                    # Entry point and demo application
├── BPlusTree.rs              # Core B+ Tree implementation
├── BTree.rs                  # Binary tree utilities
├── BTreePersistence.rs       # B+ Tree serialization/deserialization
├── MetaEnum.rs               # Data type definitions and metadata
├── TableCreationHandler.rs   # Table schema creation and validation
├── TableQueryHandler.rs      # Query execution and data manipulation
├── TableMetaHandler.rs       # Table metadata management
├── TableBTreeManager.rs      # Universal B+ Tree management system
├── UniversalBPlusTree.rs     # Generic B+ Tree implementation
├── UniversalKey.rs           # Universal key abstraction
├── FileWriter.rs             # File I/O operations
├── RowData.rs               # Row data structures and serialization
├── LruDict.rs               # LRU cache implementation
└── Comparable.rs            # Trait for comparable types
```

## 🛠️ Installation & Setup

### Prerequisites
- Rust 1.70+ (2024 edition)
- Cargo package manager

### Building the Project

```bash
# Clone the repository
git clone <your-repo-url>
cd OxideDB

# Build the project
cargo build

# Run the application
cargo run
```

### First Time Setup

On the first run, uncomment the table creation code in `main.rs`:

```rust
// Uncomment this on first run to create tables
create_tables();
```

This will create the initial database schema with sample tables.

## 📖 Usage

### Basic Operations

The database supports the following operations:

#### 1. Table Creation
```rust
let user_columns = vec![
    TableColumn::new("id".to_string(), Type::INTEGER, true),
    TableColumn::new("name".to_string(), Type::STRING(100), false),
    TableColumn::new("email".to_string(), Type::STRING(255), false),
];

handler.create_table_with_validation("users".to_string(), user_columns)?;
```

#### 2. Data Insertion
```rust
let user_data = vec![
    DataArray::INTEGER(1),
    DataArray::STRING("Test A".to_string(), 100),
    DataArray::STRING("TestA@email.com".to_string(), 255),
];

let row = query_handler.create_row("users", user_data)?;
query_handler.insert("users".to_string(), 1, row)?;
```

#### 3. Data Querying
```rust
match query_handler.select("users".to_string(), 1) {
    Ok(Some(data)) => println!("User found: {}", data),
    Ok(None) => println!("User not found"),
    Err(e) => println!("Error: {}", e),
}
```

### Sample Application

The included demo application showcases:
- Creating `users` and `products` tables
- Inserting sample data
- Querying records by primary key
- B+ Tree performance testing
- Persistence operations

## 🏗️ Architecture

### Core Components

1. **Storage Layer**: File-based persistence with page management
2. **Index Layer**: B+ Tree indexes for efficient data access
3. **Query Layer**: SQL-like operations with type validation
4. **Schema Layer**: Table metadata and schema management

### Data Types Supported

- `INTEGER` (i32)
- `BIGINT` (i64) 
- `FLOAT` (f32)
- `DOUBLE` (f64)
- `STRING(length)` (Variable length strings)

### B+ Tree Implementation

- Generic implementation supporting multiple key types
- Persistent storage with serialization
- Efficient range queries and point lookups
- Thread-safe operations with mutex protection

## 🗂️ File Organization

The database creates several files for persistence:

- `*.dat` - Table data files
- `*_btree.idx` - B+ Tree index files  
- `table_metadata.dat` - Table schema metadata
- `meta_config.db` - System configuration

## 🔧 Configuration

Key constants can be modified in `main.rs`:

```rust
const PAGE_SIZE: usize = 4096;    // Database page size
const HEADER_SIZE: usize = 64;    // Page header size
```

## 🚧 Current Status & Limitations

### Working Features
- ✅ Table creation and schema validation
- ✅ Data insertion and retrieval
- ✅ B+ Tree indexing
- ✅ Data persistence
- ✅ Multi-type support

### Known Limitations
- 🔄 No DELETE or UPDATE operations yet
- 🔄 No complex queries (JOIN, WHERE clauses)
- 🔄 No transaction support
- 🔄 Limited error recovery
- 🔄 No concurrent access control

## 🧪 Testing

Run the included demo:

```bash
cargo run
```

The application will:
1. Initialize the database system
2. Create sample tables (if uncommented)
3. Insert test data
4. Perform queries
5. Test B+ Tree functionality
6. Save state to disk

## 🤝 Contributing

This is an initial build of a custom database engine. Contributions are welcome!

### Areas for Improvement
- [ ] Implement DELETE and UPDATE operations
- [ ] Add query optimization
- [ ] Implement transaction support
- [ ] Add concurrent access control
- [ ] Improve error handling and recovery
- [ ] Add comprehensive test suite
- [ ] Implement query planner

## 📝 License

This project is available under the MIT License.

## 🔗 Dependencies

This project has minimal dependencies and uses only Rust standard library features for maximum portability and learning value.

---

**Note**: This is an educational/experimental database engine. 

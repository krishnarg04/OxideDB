mod BTree;
mod MetaEnum;
mod LruDict;
mod RowData;
mod FileWriter;
mod TableMetaHandler;
mod TableCreationHandler;
mod TableQueryHandler;
mod BPlusTree;
mod Comparable;
mod UniversalKey;
mod UniversalBPlusTree;
mod TableBTreeManager;
mod BTreePersistence;

use MetaEnum::{MetaEnum as Type, DataArray};
use TableCreationHandler::{TableCreationHandler as TCH, TableColumn};
use TableQueryHandler::TableQueryHandler as TQH;
use TableBTreeManager::{initialize_btree_manager, register_table, TableKey};
use crate::TableMetaHandler::meta_config;

const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = 64;

// Configuration flags
const CREATE_TABLES_ON_STARTUP: bool = true;
const LOAD_EXISTING_BTREES: bool = false;
const ENABLE_BTREE_TESTING: bool = true;

fn main() {
    println!("=== RustDB - Database System Demo ===\n");
    
    if let Err(e) = run_database_demo() {
        eprintln!("Database demo failed: {}", e);
        std::process::exit(1);
    }
    
    println!("\n=== Demo Complete ===");
}

fn run_database_demo() -> Result<(), String> {
    // Step 1: Initialize core systems
    initialize_systems()?;
    
    // Step 2: Setup tables (conditional)
    if CREATE_TABLES_ON_STARTUP {
        setup_demo_tables()?;
    }
    
    // Step 3: Initialize query handler
    let mut query_handler = TQH::new();
    
    // Step 4: Load existing data (conditional)
    if LOAD_EXISTING_BTREES {
        load_existing_data(&mut query_handler)?;
    }
    
    // Step 5: Run demo operations
    run_demo_operations(&mut query_handler)?;
    
    // Step 6: Cleanup and save
    cleanup_and_save(&query_handler)?;
    
    Ok(())
}

fn initialize_systems() -> Result<(), String> {
    println!("Initializing database systems...");
    
    // Initialize configuration
    initialize_config().map_err(|e| format!("Config initialization failed: {}", e))?;
    
    // Initialize B+Tree manager
    initialize_btree_manager();
    
    println!("Systems initialized successfully\n");
    Ok(())
}

fn initialize_config() -> Result<(), String> {
    let mut config_lock = meta_config.lock()
        .map_err(|_| "Failed to acquire config lock")?;
    
    if config_lock.is_none() {
        let mut new_config = TableMetaHandler::TableMetaHandler::new("meta_config.db".to_string());
        new_config.load_meta_file()
            .map_err(|e| format!("Failed to load meta file: {}", e))?;
        *config_lock = Some(new_config);
    }
    
    Ok(())
}

fn setup_demo_tables() -> Result<(), String> {
    println!("Creating demo tables...");
    
    let mut handler = TCH::new();
    
    create_users_table(&mut handler)?;
    create_products_table(&mut handler)?;
    
    println!("Demo tables created successfully\n");
    Ok(())
}

fn create_users_table(handler: &mut TCH) -> Result<(), String> {
    let user_columns = vec![
        TableColumn::new("id".to_string(), Type::INTEGER, true),
        TableColumn::new("name".to_string(), Type::STRING(100), false),
        TableColumn::new("email".to_string(), Type::STRING(255), false),
        TableColumn::new("age".to_string(), Type::INTEGER, false),
        TableColumn::new("salary".to_string(), Type::DOUBLE, false),
    ];
    
    match handler.create_table_with_validation("users".to_string(), user_columns) {
        Ok(table_id) => {
            println!("Created 'users' table (ID: {})", table_id);
            register_table(table_id, Type::INTEGER)?;
            Ok(())
        },
        Err(e) => Err(format!("Failed to create users table: {}", e))
    }
}

fn create_products_table(handler: &mut TCH) -> Result<(), String> {
    let product_columns = vec![
        TableColumn::new("product_id".to_string(), Type::INTEGER, true),
        TableColumn::new("product_name".to_string(), Type::STRING(200), false),
        TableColumn::new("price".to_string(), Type::DOUBLE, false),
        TableColumn::new("stock".to_string(), Type::INTEGER, false),
    ];
    
    match handler.create_table_with_validation("products".to_string(), product_columns) {
        Ok(table_id) => {
            println!("Created 'products' table (ID: {})", table_id);
            register_table(table_id, Type::INTEGER)?;
            Ok(())
        },
        Err(e) => Err(format!("Failed to create products table: {}", e))
    }
}

fn load_existing_data(query_handler: &mut TQH) -> Result<(), String> {
    println!("Loading existing tables and B+Trees...");
    
    // Uncomment when load_existing_btrees is implemented
    // query_handler.load_existing_btrees();
    
    let tables = query_handler.get_available_tables();
    println!("  Loaded tables: {:?}\n", tables);
    
    Ok(())
}

fn run_demo_operations(query_handler: &mut TQH) -> Result<(), String> {
    println!("Running demo operations...\n");
    
    // Insert sample data
    insert_sample_data(query_handler)?;
    
    // Query and display data
    query_sample_data(query_handler)?;
    
    // Test B+Tree functionality (if enabled)
    if ENABLE_BTREE_TESTING {
        test_btree_operations(query_handler)?;
    }
    
    Ok(())
}

fn insert_sample_data(query_handler: &mut TQH) -> Result<(), String> {
    println!("Inserting sample data...");
    
    insert_users_data(query_handler)?;
    insert_products_data(query_handler)?;
    
    println!("Sample data inserted successfully\n");
    Ok(())
}

fn insert_users_data(query_handler: &mut TQH) -> Result<(), String> {
    let users = get_sample_users();
    
    println!("Inserting users:");
    for (id, user_data) in users {
        insert_record(query_handler, "users", id, user_data)?;
    }
    
    Ok(())
}

fn insert_products_data(query_handler: &mut TQH) -> Result<(), String> {
    let products = get_sample_products();
    
    println!("Inserting products:");
    for (id, product_data) in products {
        insert_record(query_handler, "products", id, product_data)?;
    }
    
    Ok(())
}

fn insert_record(
    query_handler: &mut TQH, 
    table_name: &str, 
    id: i32, 
    data: Vec<DataArray>
) -> Result<(), String> {
    match query_handler.create_row(table_name, data) {
        Ok(row) => {
            match query_handler.insert(table_name.to_string(), id, row) {
                Ok(_) => {
                    println!("Inserted {} {}", table_name.trim_end_matches('s'), id);
                    Ok(())
                },
                Err(e) => Err(format!("Failed to insert {} {}: {}", table_name, id, e))
            }
        },
        Err(e) => Err(format!("Failed to create row for {} {}: {}", table_name, id, e))
    }
}

fn query_sample_data(query_handler: &TQH) -> Result<(), String> {
    println!("🔍 Querying sample data...");
    
    query_users_data(query_handler)?;
    query_products_data(query_handler)?;
    test_non_existent_records(query_handler)?;
    
    // Show available tables
    let tables = query_handler.get_available_tables();
    println!("  Available tables: {:?}\n", tables);
    
    Ok(())
}

fn query_users_data(query_handler: &TQH) -> Result<(), String> {
    println!("  Users:");
    for id in 1..=3 {
        query_record(query_handler, "users", id)?;
    }
    Ok(())
}

fn query_products_data(query_handler: &TQH) -> Result<(), String> {
    println!("  Products:");
    for id in [101, 102, 103] {
        query_record(query_handler, "products", id)?;
    }
    Ok(())
}

fn query_record(query_handler: &TQH, table_name: &str, id: i32) -> Result<(), String> {
    match query_handler.select(table_name.to_string(), id) {
        Ok(Some(data)) => {
            println!("    {} {}: {}", 
                table_name.trim_end_matches('s').to_uppercase(), id, data);
            Ok(())
        },
        Ok(None) => {
            println!("    {} {} not found", table_name.trim_end_matches('s'), id);
            Ok(())
        },
        Err(e) => Err(format!("Error querying {} {}: {}", table_name, id, e))
    }
}

fn test_non_existent_records(query_handler: &TQH) -> Result<(), String> {
    println!("Testing non-existent records:");
    match query_handler.select("users".to_string(), 999) {
        Ok(Some(data)) => println!("Unexpected: Found user 999: {}", data),
        Ok(None) => println!(" User 999 not found (as expected)"),
        Err(e) => return Err(format!("Error testing non-existent record: {}", e)),
    }
    Ok(())
}

fn test_btree_operations(_query_handler: &TQH) -> Result<(), String> {
    println!("Testing Universal B+Tree system:");
    
    // Test users table (ID 3)
    test_btree_search(3, TableKey::Int(1), "user 1")?;
    
    // Test products table (ID 4)
    test_btree_search(4, TableKey::Int(101), "product 101")?;
    
    println!();
    Ok(())
}

fn test_btree_search(table_id: i32, key: TableKey, description: &str) -> Result<(), String> {
    match TableBTreeManager::search_in_table(table_id, &key) {
        Ok(Some(data)) => {
            println!(" Found {} via B+Tree: page_id={}, offset={}", 
                description, data.page_id, data.offset);
            Ok(())
        },
        Ok(None) => {
            println!(" {} not found in B+Tree", description);
            Ok(())
        },
        Err(e) => {
            println!(" B+Tree search error for {}: {}", description, e);
            Ok(()) // Don't fail the demo for B+Tree errors
        }
    }
}

fn cleanup_and_save(query_handler: &TQH) -> Result<(), String> {
    println!("💾 Saving database state...");
    
    match query_handler.save_btrees() {
        Ok(_) => {
            println!("B+Trees saved successfully");
            Ok(())
        },
        Err(e) => {
            println!(" Warning: Failed to save B+Trees: {}", e);
            Ok(()) // Don't fail for save errors in demo
        }
    }
}

// Sample data generators
fn get_sample_users() -> Vec<(i32, Vec<DataArray>)> {
    vec![
        (1, vec![
            DataArray::INTEGER(1),
            DataArray::STRING("Test A".to_string(), 100),
            DataArray::STRING("testA@email.com".to_string(), 255),
            DataArray::INTEGER(28),
            DataArray::DOUBLE(75000.0),
        ]),
        (2, vec![
            DataArray::INTEGER(2),
            DataArray::STRING("Test B".to_string(), 100),
            DataArray::STRING("testB@email.com".to_string(), 255),
            DataArray::INTEGER(35),
            DataArray::DOUBLE(85000.0),
        ]),
        (3, vec![
            DataArray::INTEGER(3),
            DataArray::STRING("Test C".to_string(), 100),
            DataArray::STRING("testC@email.com".to_string(), 255),
            DataArray::INTEGER(42),
            DataArray::DOUBLE(95000.0),
        ]),
    ]
}

fn get_sample_products() -> Vec<(i32, Vec<DataArray>)> {
    vec![
        (101, vec![
            DataArray::INTEGER(101),
            DataArray::STRING("Laptop".to_string(), 200),
            DataArray::DOUBLE(1299.99),
            DataArray::INTEGER(25),
        ]),
        (102, vec![
            DataArray::INTEGER(102),
            DataArray::STRING("Mouse".to_string(), 200),
            DataArray::DOUBLE(29.99),
            DataArray::INTEGER(100),
        ]),
        (103, vec![
            DataArray::INTEGER(103),
            DataArray::STRING("Keyboard".to_string(), 200),
            DataArray::DOUBLE(79.99),
            DataArray::INTEGER(50),
        ]),
    ]
}



use rusqlite::{params, Connection, Result};
use serde_json::{json, Value};
use std::env;
use std::thread;
use std::time::Duration;

fn create_log_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS change_stream_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            action     TEXT NOT NULL,
            rowid      INTEGER,
            new_data   TEXT,
            old_data   TEXT,
            ts         DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        "#,
    )?;
    Ok(())
}

fn get_user_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master 
         WHERE type='table' 
           AND name NOT LIKE 'sqlite_%'
           AND name <> 'change_stream_log';",
    )?;
    let tables_iter = stmt.query_map([], |row| row.get(0))?;
    let mut tables = Vec::new();
    for table in tables_iter {
        tables.push(table?);
    }
    Ok(tables)
}

fn get_json_exprs(conn: &Connection, table: &str) -> Result<(String, String)> {
    let query = format!("PRAGMA table_info({})", table);
    let mut stmt = conn.prepare(&query)?;
    let mut new_expr_parts = Vec::new();
    let mut old_expr_parts = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let col: String = row.get(1)?;
        new_expr_parts.push(format!("'{}', NEW.{}", col, col));
        old_expr_parts.push(format!("'{}', OLD.{}", col, col));
    }
    let new_expr = format!("json_object({})", new_expr_parts.join(", "));
    let old_expr = format!("json_object({})", old_expr_parts.join(", "));
    Ok((new_expr, old_expr))
}

fn create_triggers_for_table(conn: &Connection, table: &str) -> Result<()> {
    let (new_expr, old_expr) = get_json_exprs(conn, table)?;

    let trigger_insert = format!(
        r#"
        CREATE TRIGGER IF NOT EXISTS change_stream_{table}_insert
        AFTER INSERT ON {table}
        BEGIN
            INSERT INTO change_stream_log(table_name, action, rowid, new_data)
            VALUES ('{table}', 'insert', NEW.rowid, {new_expr});
        END;
        "#,
        table = table,
        new_expr = new_expr
    );
    conn.execute_batch(&trigger_insert)?;

    let trigger_update = format!(
        r#"
        CREATE TRIGGER IF NOT EXISTS change_stream_{table}_update
        AFTER UPDATE ON {table}
        BEGIN
            INSERT INTO change_stream_log(table_name, action, rowid, new_data, old_data)
            VALUES ('{table}', 'update', NEW.rowid, {new_expr}, {old_expr});
        END;
        "#,
        table = table,
        new_expr = new_expr,
        old_expr = old_expr
    );
    conn.execute_batch(&trigger_update)?;

    let trigger_delete = format!(
        r#"
        CREATE TRIGGER IF NOT EXISTS change_stream_{table}_delete
        AFTER DELETE ON {table}
        BEGIN
            INSERT INTO change_stream_log(table_name, action, rowid, old_data)
            VALUES ('{table}', 'delete', OLD.rowid, {old_expr});
        END;
        "#,
        table = table,
        old_expr = old_expr
    );
    conn.execute_batch(&trigger_delete)?;

    Ok(())
}

fn compute_diff(new_data: &str, old_data: &str) -> Value {
    let new_json: Value = serde_json::from_str(new_data).unwrap_or(Value::Null);
    let old_json: Value = serde_json::from_str(old_data).unwrap_or(Value::Null);

    let mut diff_map = serde_json::Map::new();
    if let (Some(new_obj), Some(old_obj)) = (new_json.as_object(), old_json.as_object()) {
        for (key, new_val) in new_obj {
            let old_val = old_obj.get(key).unwrap_or(&Value::Null);
            if new_val != old_val {
                let diff_entry = json!({
                    "old": old_val,
                    "new": new_val
                });
                diff_map.insert(key.clone(), diff_entry);
            }
        }
    }
    Value::Object(diff_map)
}

fn poll_changes(conn: &Connection) -> Result<()> {
    let mut last_id: i64 = 0;
    let mut stmt = conn.prepare(
        "SELECT id, table_name, action, rowid, new_data, old_data, ts 
         FROM change_stream_log 
         WHERE id > ?1 
         ORDER BY id ASC;"
    )?;
    
    loop {
        let rows = stmt.query_map(params![last_id], |row| {
            let id: i64 = row.get(0)?;
            let table_name: String = row.get(1)?;
            let action: String = row.get(2)?;
            let rowid: Option<i64> = row.get(3)?; 
            let new_data: Option<String> = row.get(4)?;
            let old_data: Option<String> = row.get(5)?;
            let ts: String = row.get(6)?;
            Ok((id, table_name, action, rowid, new_data, old_data, ts))
        })?;

        for row in rows {
            let (id, table_name, action, rowid, new_data, old_data, ts) = row?;
            last_id = id;
            if action == "update" {
                if let (Some(new_str), Some(old_str)) = (new_data.as_ref(), old_data.as_ref()) {
                    let diff = compute_diff(new_str, old_str);
                    let event = json!({
                        "id": id,
                        "table": table_name,
                        "action": action,
                        "rowid": rowid,
                        "changed_fields": diff,
                        "timestamp": ts
                    });
                    println!("{}", event);
                } else {
                    let event = json!({
                        "id": id,
                        "table": table_name,
                        "action": action,
                        "rowid": rowid,
                        "new_data": new_data,
                        "old_data": old_data,
                        "timestamp": ts
                    });
                    println!("{}", event);
                }
            } else {
                let event = json!({
                    "id": id,
                    "table": table_name,
                    "action": action,
                    "rowid": rowid,
                    "new_data": new_data,
                    "old_data": old_data,
                    "timestamp": ts
                });
                println!("{}", event);
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

fn cleanup(db_path: &str) -> Result<()> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch("DROP TABLE IF EXISTS change_stream_log;")?;

    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master 
         WHERE type='trigger' AND name LIKE 'change_stream_%';",
    )?;
    let trigger_names: Result<Vec<String>> = stmt.query_map([], |row| row.get(0))?.collect();
    let trigger_names = trigger_names?;

    for trigger in trigger_names {
        let drop_sql = format!("DROP TRIGGER IF EXISTS {};", trigger);
        conn.execute_batch(&drop_sql)?;
    }
    eprintln!("Cleanup completed");
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <database_file>", args[0]);
        std::process::exit(1);
    }
    let db_path = args[1].clone();
    let conn = Connection::open(&db_path)?;

    let wal_mode: String = conn.query_row("PRAGMA journal_mode = WAL;", [], |row| row.get(0))?;
    eprintln!("Database journal mode: {}", wal_mode);

    {
        let db_path_clone = db_path.clone();
        ctrlc::set_handler(move || {
            eprintln!("\nReceived interrupt signal, cleaning up...");
            if let Err(e) = cleanup(&db_path_clone) {
                eprintln!("Cleanup failed: {:?}", e);
            }
            std::process::exit(0);
        })
        .expect("Error setting interrupt handler");
    }

    create_log_table(&conn)?;

    let tables = get_user_tables(&conn)?;
    for table in tables {
        create_triggers_for_table(&conn, &table)?;
    }

    eprintln!("Starting database monitoring: {}", db_path);

    poll_changes(&conn)
}

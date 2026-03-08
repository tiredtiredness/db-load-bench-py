use crate::csv::{csv_read, clean_identifier};
use crate::inserter::{ConnParams, Inserter};
use anyhow::{Result, anyhow};
use mysql::prelude::*;
use mysql::*;

pub struct MySQLInserter {
    conn: Conn,
}

impl MySQLInserter {
    pub fn new(p: &ConnParams) -> Result<Self> {
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            p.user, p.password, p.host, p.port, p.database
        );
        let opts = Opts::from_url(&url)?;
        let conn = Conn::new(opts)?;
        Ok(Self { conn })
    }

    fn quote(name: &str) -> String {
        let clean = clean_identifier(name);
        format!("`{}`", clean.replace('`', "``"))
    }

    fn build_cols(headers: &[String]) -> String {
        headers.iter()
            .map(|h| Self::quote(h))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Inserter for MySQLInserter {

    // ─── default_insert ───────────────────────────────────────────────────────

    fn default_insert(&mut self, csv_file: &str, table: &str) -> Result<usize> {
        let data  = csv_read(csv_file)?;
        let cols  = Self::build_cols(&data.headers);
        let phs   = vec!["?"; data.headers.len()].join(", ");
        let sql   = format!("INSERT INTO {} ({}) VALUES ({})",
                            Self::quote(table), cols, phs);

        let mut tx = self.conn.start_transaction(TxOpts::default())?;

        for row in &data.rows {
            let params: Vec<Value> = row.iter()
                .map(|v| Value::Bytes(v.as_bytes().to_vec()))
                .collect();
            tx.exec_drop(&sql, params)?;
        }

        tx.commit()?;
        Ok(data.rows.len())
    }

    // ─── bulk_insert ──────────────────────────────────────────────────────────

    fn bulk_insert(&mut self, csv_file: &str, table: &str,
                   batch_size: usize) -> Result<usize> {
        let data   = csv_read(csv_file)?;
        let cols   = Self::build_cols(&data.headers);
        let qtable = Self::quote(table);
        let ncols  = data.headers.len();
        let mut total = 0;

        for chunk in data.rows.chunks(batch_size) {
            // INSERT INTO t (cols) VALUES (?,?), (?,?) ...
            let row_phs = format!("({})", vec!["?"; ncols].join(", "));
            let all_phs = vec![row_phs.as_str(); chunk.len()].join(", ");
            let sql = format!("INSERT INTO {} ({}) VALUES {}",
                              qtable, cols, all_phs);

            let params: Vec<Value> = chunk.iter()
                .flat_map(|row| row.iter()
                    .map(|v| Value::Bytes(v.as_bytes().to_vec())))
                .collect();

            let mut tx = self.conn.start_transaction(TxOpts::default())?;
            tx.exec_drop(&sql, params)?;
            tx.commit()?;
            total += chunk.len();
        }

        Ok(total)
    }

    // ─── file_insert ──────────────────────────────────────────────────────────

    fn file_insert(&mut self, csv_file: &str, table: &str) -> Result<usize> {
        let data = csv_read(csv_file)?;
        let count = data.rows.len();

        let sql = format!(
            "LOAD DATA LOCAL INFILE '{}' \
             INTO TABLE {} \
             FIELDS TERMINATED BY ',' \
             OPTIONALLY ENCLOSED BY '\"' \
             LINES TERMINATED BY '\\n' \
             IGNORE 1 ROWS",
            csv_file,
            Self::quote(table)
        );

        self.conn.query_drop(&sql)
            .map_err(|e| anyhow!("LOAD DATA error: {}", e))?;

        Ok(count)
    }
}
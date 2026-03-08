use anyhow::Result;

pub struct ConnParams {
    pub host:     String,
    pub port:     u16,
    pub user:     String,
    pub password: String,
    pub database: String,
}

pub trait Inserter {
    fn default_insert(&mut self, csv_file: &str, table: &str) -> Result<usize>;
    fn bulk_insert(&mut self, csv_file: &str, table: &str, batch_size: usize) -> Result<usize>;
    fn file_insert(&mut self, csv_file: &str, table: &str) -> Result<usize>;
}
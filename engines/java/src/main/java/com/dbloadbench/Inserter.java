public interface Inserter extends AutoCloseable {
    int defaultInsert(String csvFile, String tableName) throws Exception;
    int bulkInsert(String csvFile, String tableName, int batchSize) throws Exception;
    int fileInsert(String csvFile, String tableName) throws Exception;
    void close();
}
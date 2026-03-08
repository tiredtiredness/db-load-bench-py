public class ConnParams {
    public final String host;
    public final int    port;
    public final String user;
    public final String password;
    public final String database;

    public ConnParams(String host, int port, String user,
                      String password, String database) {
        this.host     = host;
        this.port     = port;
        this.user     = user;
        this.password = password;
        this.database = database;
    }
}
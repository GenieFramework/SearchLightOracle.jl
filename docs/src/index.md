# SearchlightOracle

### Requirements

Oracle.jl has to be installed. For this a possibility to compile the necessary C-library must be provided. For further informations use [Oracle.jl on GitHub](https://github.com/felipenoris/Oracle.jl) 

### Pre usage work

If you have to use an Oracle RDBMS use the instant client library for the connection. Install this before you load Oracle.jl. You should also integrate the environment variable LD_LIBRARY_PATH to the installation directory of the instant client.

### Usage

The yml settings file can be used to provide the details of the connection. To establish the connection you can use the following:

```julia
conn_info_oracle = SearchLight.Configuration.load(fullpath_to_connection.yml)
```

The next thing is to establish the connection:

```julia
conn = SearchLight.connect(conn_info_oracle)
```
And... here we are. The rest of the game is to develop the necessary table_migrations. For this please study the documentation of [SearchLight](https://github.com/GenieFramework/SearchLight.jl).
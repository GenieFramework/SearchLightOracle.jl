module SearchLightOracle

import DataFrames, Logging
import Oracle

import SearchLight

import SearchLight: storableFields, fields_to_store_directly

const DEFAULT_PORT = 1521

const COLUMN_NAME_FIELD_NAME = :column_name

function SearchLight.column_field_name()
  COLUMN_NAME_FIELD_NAME
end

const DatabaseHandle = Oracle.Connection
const ResultHandle   = Oracle.Result

#
# Connection
#


"""
    connect(conn_data::Dict)::DatabaseHandle

Connects to the database and returns a handle.
"""
function SearchLight.connect(conn_data::Dict = SearchLight.config.db_config_settings) :: DatabaseHandle
  dns = String[]

  haskey(conn_data, "host")     && push!(dns, string("host=", conn_data["host"]))
  haskey(conn_data, "hostaddr") && push!(dns, string("hostaddr=", conn_data["hostaddr"]))
  haskey(conn_data, "port")     && push!(dns, string("port=", conn_data["port"]))
  haskey(conn_data, "database") && push!(dns, string("dbname=", conn_data["database"]))
  haskey(conn_data, "username") && push!(dns, string("user=", conn_data["username"]))
  haskey(conn_data, "password") && push!(dns, string("password=", conn_data["password"]))
  haskey(conn_data, "passfile") && push!(dns, string("passfile=", conn_data["passfile"]))
  haskey(conn_data, "connect_timeout") && push!(dns, string("connect_timeout=", conn_data["connect_timeout"]))
  haskey(conn_data, "client_encoding") && push!(dns, string("client_encoding=", conn_data["client_encoding"]))

  push!(CONNECTIONS, LibPQ.Connection(join(dns, " ")))[end]
end

end

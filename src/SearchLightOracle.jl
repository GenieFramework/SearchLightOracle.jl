module SearchLightOracle

import DataFrames, Logging
import SearchLight
import SearchLight: storableFields, fields_to_store_directly, connect

using Oracle

const DEFAULT_PORT = 1521

const COLUMN_NAME_FIELD_NAME = :column_name

function SearchLight.column_field_name()
  COLUMN_NAME_FIELD_NAME
end

const DatabaseHandle = Oracle.Connection

const CONNECTIONS = DatabaseHandle[]

#
# Connection
#

"""
    connect(conn_data::Dict)::DatabaseHandle

Connects to the database and returns a handle.
"""
function SearchLight.connect(conn_data::Dict = SearchLight.config.db_config_settings)::DatabaseHandle
  
  host = ""
  port = ""
  database = ""
  username = ""
  password = ""
  connectionString = ""
  
  haskey(conn_data, "host")     ? host = conn_data["host"] : throw(SearchLight.Exceptions.InvalidConnectionItem("host is not there or empty"))
  haskey(conn_data, "port")     ? port = conn_data["port"] : throw(SearchLight.Exceptions.InvalidConnectionItem("port is not there or empty"))
  haskey(conn_data, "database") ? database = conn_data["database"] : throw(SearchLight.Exceptions.InvalidConnectionItem("database is not there or empty"))
  haskey(conn_data, "username") ? username = conn_data["username"] : throw(SearchLight.Exceptions.InvalidConnectionItem("username is not there or empty"))
  haskey(conn_data, "password") ? password = conn_data["password"] : throw(SearchLight.Exceptions.InvalidConnectionItem("password is not there or empty"))

  connectionString = "//$host:$port/$database"

  push!(CONNECTIONS, Oracle.Connection(username, password, connectionString))[end]
end

function SearchLight.connection()
  isempty(CONNECTIONS) && throw(SearchLight.Exceptions.NotConnectedException())
  CONNECTIONS[end]
end

function DataFrames.DataFrame(resultSet::Oracle.ResultSet)

    ## column names
    dictColumns = resultSet.schema.column_names_index
    key_value_pairs = [(key ,value) for (key,value) in dictColumns]
    sort!(key_value_pairs, by = x->x[2])
    colNames = map(x->x[1],key_value_pairs)

    ## data 
    rowSize, colSize = size(resultSet)
    matRes = Matrix{Any}(missing,rowSize,colSize)
    rowcount = 1
    for row in resultSet.rows
        for (key,value) in dictColumns
            matRes[rowcount,value] = row.data[value]
        end
        rowcount += 1
    end

    resType = create_types_for_matrix(matRes)
    df = DataFrames.DataFrame(resType,colNames,0)

    for i in 1:rowSize
        push!(df,matRes[i,:])
    end

    result = DataFrames.DataFrame(matRes,colNames)

    return df
end


function create_types_for_matrix(matrix::Array{Any,2})::Array{Type}
    df = DataFrames.DataFrame()
    resMatrix = []
    rowsize, colsize = size(matrix)
    typeArray = Type[]

    for colNum in 1:colsize

        typeCol = unique(map(x->typeof(x),matrix[:,colNum]))

        typeDef =   if length(typeCol) == 1 && length(findall(x -> x==Missing, typeCol)) > 0
                        Union{Any,Missing}
                    else
                        filter!(x -> x != Missing ,typeCol)
                        Union{typeCol[1],Missing}
                    end
        push!(typeArray,typeDef)
    end
    return typeArray
end

end # End of Modul

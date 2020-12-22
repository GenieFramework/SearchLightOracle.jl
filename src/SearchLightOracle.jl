module SearchLightOracle

import DataFrames, Logging
import SearchLight
import SearchLight: storableFields, fields_to_store_directly, connect

using Oracle, Dates

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

function SearchLight.Migration.drop_migrations_table(table_name::String = SearchLight.config.db_migrations_table_name) :: Nothing
  
    queryString = string("""SELECT 
                                table_name 
                            FROM 
                                ALL_TABLES t 
                            WHERE 
                                t.Tablespace_name in (select user from dual) 
                                and table_name = '$table_name'""")

    if !isempty(SearchLight.query(queryString)) 

        SearchLight.query("DROP TABLE $table_name")
        @info "Droped table $table_name"
    else
        @info "Nothing to drop"
    end

    nothing
end

function SearchLight.query(sql::String, conn::DatabaseHandle = SearchLight.connection(); internal = false) :: DataFrames.DataFrame
    result = if SearchLight.config.log_queries && ! internal
        @info sql
        stmt = Oracle.Stmt(conn, sql)
        @time Oracle.execute(stmt)
        Oracle.query(stmt)
    else
        stmt = Oracle.Stmt(conn, sql)
        Oracle.execute(stmt)
        Oracle.quer(stmt)
    end

    # if LibPQ.error_message(result) != ""
    #   throw(SearchLight.Exceptions.DatabaseAdapterException("$(string(LibPQ)) error: $(LibPQ.errstring(result)) [$(LibPQ.errcode(result))]"))
    # end 
    return result |> DataFrames.DataFrame
end

### not defined yet in Oracle.jl
function DataFrames.DataFrame(resultSet::Oracle.ResultSet)
    rowSize, colSize = size(resultSet)
    queryInfos = resultSet.schema.column_query_info
    df = DataFrames.DataFrame()

    ## column names
    dictColumns = resultSet.schema.column_names_index
    key_value_pairs = sort([(key,value) for (key,value) in dictColumns], by=x->x[2])
    colNames = reshape([x[1] for x in key_value_pairs],1,colSize)

    ## data 
    matRes = Matrix{Any}(missing,rowSize,colSize)

    ## Bring the data in Matrixform 
    rowcount = 1
    for row in resultSet.rows
        for (key,value) in dictColumns
            matRes[rowcount,value] = row.data[value]
        end
        rowcount += 1
    end

    ## calclulate the Types of Columns. Datatypes from raw data are exact. If no data are retried
    ## datatypes from the queryInfos must be sufficient
    resType = rowSize != 0 ? create_types_for_matrix(matRes,queryInfos) : juliaType_fromOracleType(queryInfos) 

    ## fill the dataframe with columnNames and Types
    for pair_Name_Nr in key_value_pairs
         df[Symbol(pair_Name_Nr[1])]= resType[pair_Name_Nr[2]][]
    end

    for i in 1:rowSize
        push!(df,matRes[i,:])
    end

    return df
end


function create_types_for_matrix(matrix::Array{Any,2}, columnInfos::Vector{Oracle.OraQueryInfo})::Array{Type}
    df = DataFrames.DataFrame()
    resMatrix = []
    rowsize, colsize = size(matrix)
    typeArray = Type[]

    for colNum in 1:colsize

        typeCol = unique(map(x->typeof(x),matrix[:,colNum]))

        typeDef =   if length(typeCol) == 1 && length(findall(x -> x==Missing, typeCol)) > 0
                        ora_type = juliaType_fromOracleType(columnInfos[colNum])
                        Union{ora_type,Missing}
                    else
                        filter!(x -> x != Missing ,typeCol)
                        Union{typeCol[1],Missing}
                    end
        push!(typeArray,typeDef)
    end
    return typeArray
end

########################################################################
#                                                                      #
#           Utility funcitions                                         # 
#                                                                      # 
########################################################################

const changeDict = Dict([
    Oracle.ORA_ORACLE_TYPE_NONE          => Any                 ,
    Oracle.ORA_ORACLE_TYPE_VARCHAR       => String              ,  
    Oracle.ORA_ORACLE_TYPE_NVARCHAR      => String              ,
    Oracle.ORA_ORACLE_TYPE_CHAR          => Char                ,
    Oracle.ORA_ORACLE_TYPE_NCHAR         => String              ,
    Oracle.ORA_ORACLE_TYPE_ROWID         => String              ,
    Oracle.ORA_ORACLE_TYPE_RAW           => Any                 ,
    Oracle.ORA_ORACLE_TYPE_NATIVE_FLOAT  => Float64             ,
    Oracle.ORA_ORACLE_TYPE_NATIVE_DOUBLE => Float64             ,
    Oracle.ORA_ORACLE_TYPE_NATIVE_INT    => Int64               ,
    Oracle.ORA_ORACLE_TYPE_NUMBER        => Number              ,
    Oracle.ORA_ORACLE_TYPE_DATE          => Date                ,
    Oracle.ORA_ORACLE_TYPE_TIMESTAMP     => DateTime            ,
    Oracle.ORA_ORACLE_TYPE_TIMESTAMP_TZ  => Oracle.TimestampTZ  ,
    Oracle.ORA_ORACLE_TYPE_TIMESTAMP_LTZ => Oracle.TimestampTZ  ,
    Oracle.ORA_ORACLE_TYPE_INTERVAL_DS   => Any                 ,   
    Oracle.ORA_ORACLE_TYPE_INTERVAL_YM   => Any                 ,
    Oracle.ORA_ORACLE_TYPE_CLOB          => Oracle.Lob          ,
    Oracle.ORA_ORACLE_TYPE_NCLOB         => Oracle.Lob          ,
    Oracle.ORA_ORACLE_TYPE_BLOB          => Oracle.Lob          ,
    Oracle.ORA_ORACLE_TYPE_BFILE         => Any                 ,
    Oracle.ORA_ORACLE_TYPE_STMT          => Any                 ,  
    Oracle.ORA_ORACLE_TYPE_BOOLEAN       => Bool                ,
    Oracle.ORA_ORACLE_TYPE_OBJECT        => Any                 ,
    Oracle.ORA_ORACLE_TYPE_LONG_VARCHAR  => String              ,
    Oracle.ORA_ORACLE_TYPE_LONG_RAW      => Any                 ,
    Oracle.ORA_ORACLE_TYPE_NATIVE_UINT   => Int64               ,
    Oracle.ORA_ORACLE_TYPE_MAX           => Any             
    ])  

"""
function juliaType_fromOracleType(columnInfo::Oracle.OraQueryInfo)::Type
    
    Returns the Julia datatype for a given OraQueryInfo. This function is only meant to use
    if the no data returned form the database to show the nearly right result for building a 
    dataframe.
"""
function juliaType_fromOracleType(columnInfo::Oracle.OraQueryInfo)::Type 
        ora_datatype = columnInfo.type_info.oracle_type_num              
        result =  haskey(changeDict,ora_datatype) ? changeDict[ora_datatype] : error("The type $ora_datatype is not supported by Oracle.jl")
     return result
 end
 
 
 function juliaType_fromOracleType(columnInfos::Vector{T})::Array{Type,1} where {T<:Oracle.OraQueryInfo}
     juliaType_fromOracleType.(columnInfos)
 end

end # End of Modul

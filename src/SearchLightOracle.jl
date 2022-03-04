module SearchLightOracle

import DataFrames, Logging
import SearchLight

using Oracle, Dates


const DEFAULT_PORT = 1521

const COLUMN_NAME_FIELD_NAME = :column_name

function SearchLight.column_field_name()
  COLUMN_NAME_FIELD_NAME
end

struct DBConnection
    host::String
    port::String
    db::String
    user::String
    db_handle::Oracle.Connection
end

const DatabaseHandle = DBConnection

const CONNECTIONS = DatabaseHandle[]

const TYPE_MAPPINGS = Dict{Symbol,Symbol}( # Julia / Postgres
  :char       => :CHAR                  ,
  :string     => Symbol("VARCHAR(255)") ,
  :text       => Symbol("VARCHAR(2000)"),
  :integer    => :Number                ,
  :int        => :Number                ,
  :float      => :Number                ,
  :decimal    => :Number                ,
  :datetime   => :DATE                  ,
  :timestamp  => :TIMESTAMP             ,
  :time       => :TIMESTAMP             ,
  :date       => :DATE                  ,
  :binary     => :BLOB                  ,
  :boolean    => :SMALLINT              ,
  :bool       => :SMALLINT              ,
  :dbid       => Symbol("NUMBER(10)"))

#
# Connection
#

"""
    connect(conn_data::Dict)::DatabaseHandle

Connects to the database and returns a handle.
"""
function SearchLight.connect(conn_data::Dict{String,Any} = SearchLight.config.db_config_settings)::DatabaseHandle

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

  conn = Oracle.Connection(username, password, connectionString)
  db_con = DBConnection(host,string(port), database, username, conn)

  push!(CONNECTIONS, db_con)[end]
end

function Base.count(m::Type{T}, q::SearchLight.SQLQuery = SearchLight.SQLQuery())::Int where {T<:SearchLight.AbstractModel}
  count_column = SearchLight.SQLColumn("COUNT(*) AS __cid", raw = true)
  q = SearchLight.clone(q, :columns, push!(q.columns, count_column))

  SearchLight.finddf(m, q)[1, Symbol("__cid")]
end

function SearchLight.disconnect(connection::DBConnection)
    Oracle.close(connection.db_handle)
    nothing
end

function SearchLight.connection()
  isempty(CONNECTIONS) && throw(SearchLight.Exceptions.NotConnectedException())
  CONNECTIONS[end]
end

# function SearchLight.Migration.drop_migrations_table(table_name::String = SearchLight.config.db_migrations_table_name) :: Nothing

#   SearchLight.Migration.drop_table(table_name)

#   nothing
# end


"""
create_migrations_table -- creates the needed migration table

  Runs a SQL DB query that creates the table `table_name` with the structure needed to be used as the DB migrations table.
  The table should contain one column, `version`, unique, as a string of maximum 30 chars long.
"""
function SearchLight.Migration.create_migrations_table(table_name::String = SearchLight.config.db_migrations_table_name) :: Bool

  queryString = string("select table_name from user_tables where table_name = upper('$table_name')")
  if isempty(SearchLight.query(queryString))
    stmt = Oracle.Stmt(SearchLight.connection().db_handle,"CREATE TABLE $table_name (version varchar2(30))")
    Oracle.execute(stmt)
    @info "Created table $table_name"
  else
    @info "Migration table exists."
  end

  true
end

function matchall(r::Regex, string::Union{SubString{String},String})
  matches = collect(eachmatch(r,string))
  [match.match for match in matches]
end

function column_names_from_select(sql::String)
  result = String[]
  closures = []
  replacement_string = "XXYXX"
  # matches the statement between select .... from
  rawmatch = match(r"(?i)(?s)(?<=Select).*?(?=From)", sql).match
  if rawmatch !== nothing
    match_without_linebr = string(strip(replace(rawmatch,r"\R"=>"")))
    closure_items = reverse(matchall(r"\(.+\)", match_without_linebr))
    closure_replaced = replace(match_without_linebr, r"\(.+\)" => replacement_string)
    splitted_withoutClosures = string.(split(closure_replaced,","))
    items = String[]
    for item in splitted_withoutClosures
      tmpItem = last(matchall(r"(\w+)",string(item)))
      if occursin(replacement_string,tmpItem)
        push!(items,replace(item,replacement_string => pop!(closure_items)))
      else
        push!(items,tmpItem)
      end
    end
    result = string.(items)
  end
  result
end

function SearchLight.columns(m::Type{T})::DataFrames.DataFrame where {T<:SearchLight.AbstractModel}
  SearchLight.query(table_columns_sql(SearchLight.table(m)), internal = true)
end

"""
    table_columns_sql(table_name::String)::String

Returns the adapter specific query for SELECTing table columns information corresponding to `table_name`.
"""
function table_columns_sql(table_name::String) :: String
  "SELECT column_name FROM user_tab_cols WHERE TABLE_NAME = '$table_name'"
end

function SearchLight.delete_all(m::Type{T}; truncate::Bool = true, reset_sequence::Bool = true, cascade::Bool = false)::Nothing where {T<:SearchLight.AbstractModel}
  if truncate
    sql = "TRUNCATE $(SearchLight.table(m))"
    cascade ? sql *= " CASCADE" : ""
  else
    sql = "DELETE FROM $(SearchLight.table(m))"
  end

  SearchLight.query(sql)

  if reset_sequence
    reset_sequence(sequence_name_pk(m))
  end

  nothing
end

function SearchLight.delete(m::T)::T where {T<:SearchLight.AbstractModel}
  SearchLight.ispersisted(m) || throw(SearchLight.Exceptions.NotPersistedException(m))
  pk_field = Symbol(SearchLight.primary_key_name(typeof(m)))

  "DELETE FROM $(SearchLight.table(typeof(m))) WHERE $(SearchLight.primary_key_name(typeof(m))) = '$(values(getfield(m,pk_field)))'" |> SearchLight.query

  # reinitialize the id
  setfield!(m,pk_field, SearchLight.DbId())

  m
end

function SearchLight.query(sql::String, conn::DatabaseHandle = SearchLight.connection(); internal = false) :: DataFrames.DataFrame
    #initializing the dataframe
    df = DataFrames.DataFrame()
    #preparing the statement
    stmt = Oracle.Stmt(conn.db_handle, sql)
    #execute the query statement
    result = if SearchLight.config.log_queries && ! internal
        @info sql
        stmt.info.is_query == true ? Oracle.query(stmt) : @time Oracle.execute(stmt)
    else
        stmt.info.is_query == true ? Oracle.query(stmt) : Oracle.execute(stmt)
    end

    ## if the statement is an insert-stmt bring back the actual val of the sequence
    if isInsertStmt(sql)
       id_result, id_name = current_value_seq(conn, sql)
       if id_name != ""
          df = id_result |> DataFrames.DataFrame
          DataFrames.rename!(df,[1=>id_name])
       end
    end

    #Until SearchLight will for its own support transactions every transaction will commited
    stmt.info.is_query == false && Oracle.commit(SearchLight.connection().db_handle)

    #get back the original column_names for the dataframe
    if stmt.info.is_query
      #get the dataframe
      df = DataFrames.DataFrame(result)
      realColumn_names = column_names_from_select(sql)
      colNames_df = names(df)
      indices = findall(x->uppercase(x) in colNames_df, realColumn_names)
      DataFrames.rename!(df,[index => Symbol(realColumn_names[index]) for index in indices])
    end
     ## each statment should be closed
     Oracle.close(stmt)
     typeof(result) != Oracle.ResultSet || !isempty(df) ? df : result |> DataFrames.DataFrame
end

function SearchLight.to_find_sql(m::Type{T}, q::SearchLight.SQLQuery, joins::Union{Nothing,Vector{SearchLight.SQLJoin}} = nothing)::String where {T<:SearchLight.AbstractModel}
    sql::String = ( string("$(SearchLight.to_select_part(m, q.columns, joins)) $(SearchLight.to_from_part(m)) $(SearchLight.to_join_part(m, joins)) $(SearchLight.to_where_part(q.where)) ",
                        "$(SearchLight.to_group_part(q.group)) $(SearchLight.to_having_part(q.having)) $(SearchLight.to_order_part(m, q.order)) ",
                        "$(SearchLight.to_limit_part(q.limit)) $(SearchLight.to_offset_part(q.offset))")) |> strip
    replace(sql, r"\s+"=>" ")
end

function SearchLight.to_from_part(m::Type{T})::String where {T<:SearchLight.AbstractModel}
  "FROM " * SearchLight.escape_column_name(SearchLight.table(m), SearchLight.connection())
end

function SearchLight.to_join_part(m::Type{T}, joins::Union{Nothing,Vector{SearchLight.SQLJoin}} = nothing)::String where {T<:SearchLight.AbstractModel}
    joins === nothing && return ""

    join(map(x -> string(x), joins), " ")
end

function SearchLight.to_where_part(w::Vector{SearchLight.SQLWhereEntity})::String
  where = isempty(w) ?
          "" :
          string("WHERE ",
                (string(first(w).condition) == "AND" ? "TRUE " : "FALSE "),
                join(map(wx -> string(wx), w), " "))

  replace(where, r"WHERE TRUE AND "i => "WHERE ")
end

function SearchLight.to_group_part(g::Vector{SearchLight.SQLColumn}) :: String
  isempty(g) ?
    "" :
    string(" GROUP BY ", join(map(x -> string(x), g), ", "))
end

function SearchLight.to_having_part(h::Vector{SearchLight.SQLWhereEntity}) :: String
  having =  isempty(h) ?
            "" :
            string("HAVING ",
                  (string(first(h).condition) == "AND" ? "TRUE " : "FALSE "),
                  join(map(w -> string(w), h), " "))

  replace(having, r"HAVING TRUE AND "i => "HAVING ")
end

function SearchLight.to_order_part(m::Type{T}, o::Vector{SearchLight.SQLOrder})::String where {T<:SearchLight.AbstractModel}
  isempty(o) ?
    "" :
    string("ORDER BY ",
            join(map(x -> string((! SearchLight.is_fully_qualified(x.column.value) ?
                                    SearchLight.to_fully_qualified(m, x.column) :
                                    x.column.value), " ", x.direction),
                      o), ", "))
end

function SearchLight.to_limit_part(l::SearchLight.SQLLimit) :: String
  l.value != "ALL" ? string("LIMIT ", string(l)) : ""
end

function SearchLight.to_offset_part(o::Int) :: String
  o != 0 ? string("OFFSET ", string(o)) : ""
end

function SearchLight.update_query_part(m::T)::String where {T<:SearchLight.AbstractModel}

  uf = fields_to_store_directly(typeof(m))

  update_values = join(map(x -> "$(string(SearchLight.SQLColumn(uf[x]))) = $(string(SearchLight.to_sqlinput(m, Symbol(x), getfield(m, Symbol(x)))) )", collect(keys(uf))), ", ")

  " $update_values WHERE $(SearchLight.table(typeof(m))).$(SearchLight.primary_key_name(typeof(m))) = '$(m.id.value)'"
end

function SearchLight.column_data_to_column_name(column::SearchLight.SQLColumn, column_data::Dict{Symbol,Any}) :: String
  "$(SearchLight.to_fully_qualified(column_data[:column_name], column_data[:table_name])) AS $(isempty(column_data[:alias]) ? SearchLight.to_sql_column_name(column_data[:column_name], column_data[:table_name]) : column_data[:alias] )"
end

# ### fallback function if storableFields not defined in the module
# function storableFields(m::Type{T})::Dict{String,String} where {T<:SearchLight.AbstractModel}
#     tmpStorage = Dict{String,String}()
#     for field in SearchLight.persistable_fields(m)
#       push!(tmpStorage, field => field)
#     end
#     return tmpStorage
# end

"""
  Only direct storable fields will be returnd by this function.
  The fields with an AbstractModel-field or array will be stored temporarly
  in the saving method and saved after returning the parent struct.
"""
# function fields_to_store_directly(m::Type{T}) where {T<:SearchLight.AbstractModel}

#   storage_fields = storableFields(m)
#   fields_and_types = SearchLight.to_string_dict(m)
#   uf=Dict{String,String}()

#   for (key,value) in storage_fields
#     if !(fields_and_types[key]<:SearchLight.AbstractModel || fields_and_types[key]<:Array{<:SearchLight.AbstractModel,1})
#       push!(uf,key => value)
#     end
#   end

#   return uf
# end

function SearchLight.to_store_sql(m::T; conflict_strategy = :error)::String where {T<:SearchLight.AbstractModel}

    uf = SearchLight.persistable_fields(typeof(m))

    sql = if ! SearchLight.ispersisted(m) || (SearchLight.ispersisted(m) && conflict_strategy == :update)
    pos = findfirst(x -> x == SearchLight.primary_key_name(m), uf)
    pos > 0 && splice!(uf, pos)

    id_column = SearchLight.pk(m) != "" ?  SearchLight.pk(m) * ", " : ""
    id_value = id_column != "" ?  sequence_name_pk(m)*".nextval " : ""

    fields = SearchLight.SQLColumn(uf)
    push!(fields, SearchLight.SQLColumn(SearchLight.primary_key_name(m)))

    vals = join( map(x -> string(SearchLight.to_sqlinput(m, Symbol(x), getfield(m, Symbol(x)))), uf), ", ")
    vals = string(vals, ",$id_value")

    "INSERT INTO $(SearchLight.table(typeof(m))) ( $fields ) VALUES ( $vals )" *
        if ( conflict_strategy == :error ) ""
        elseif ( conflict_strategy == :ignore ) " ON CONFLICT DO NOTHING"
        end
  else
    prepare_update_part(m)
  end

  return sql
end

function prepare_update_part(m::T)::String where {T<:SearchLight.AbstractModel}

  result = ""
  sub_abstracts = SearchLight.array_sub_abstract_models(m)

  if !isempty(sub_abstracts)
    result = join(prepare_update_part.(sub_abstracts),";",";")
    result *= ";"
  end
  result *= "UPDATE $(SearchLight.table(typeof(m))) SET $(SearchLight.update_query_part(m))"
end

function SearchLight.Migration.create_table(f::Function, name::Union{String,Symbol}, options::Union{String,Symbol} = "") :: Nothing
  SearchLight.query(create_table_sql(f, string(name), options), internal = true)
  SearchLight.Migration.create_sequence(sequence_name_pk(name))
  SearchLight.query("select $(sequence_name_pk(name)).nextval from dual")

  nothing
end

function SearchLight.Migration.add_index(table_name::Union{String,Symbol}, column_name::Union{String,Symbol}; name::Union{String,Symbol} = "", unique::Bool = false, order::Union{String,Symbol} = :none) :: Nothing
  name = isempty(name) ? SearchLight.index_name(table_name, column_name) : name
  SearchLight.query("CREATE $(unique ? "UNIQUE" : "") INDEX $(name) ON $table_name ($column_name)", internal = true)

  nothing
end

function SearchLight.Migration.add_column(table_name::Union{String,Symbol}, name::Union{String,Symbol}, column_type::Union{String,Symbol}; default::Union{String,Symbol,Nothing} = nothing, limit::Union{Int,Nothing} = nothing, not_null::Bool = false) :: Nothing
  SearchLight.query("ALTER TABLE $table_name ADD $(SearchLight.Migration.column(name, column_type, default = default, not_null = not_null))", internal = true)

  nothing
end

function SearchLight.Migration.remove_column(table_name::Union{String,Symbol}, name::Union{String,Symbol}, options::Union{String,Symbol} = "") :: Nothing
  SearchLight.query("ALTER TABLE $table_name DROP COLUMN $name $options", internal = true)

  nothing
end

function SearchLight.Migration.remove_index(name::Union{String,Symbol}, options::Union{String,Symbol} = "") :: Nothing
  SearchLight.query("DROP INDEX $name $options", internal = true)

  nothing
end

function SearchLight.Migration.constraint(table_name::Union{String,Symbol}, column_name::Union{String,Symbol}) :: String
  string("CONSTRAINT ", SearchLight.index_name(table_name, column_name))
end

function SearchLight.Migration.nextval(table_name::Union{String,Symbol}, column_name::Union{String,Symbol}) :: String
  "NEXTVAL('$(sequence_name(table_name, column_name) )')"
end

function SearchLight.Migration.column_id_sequence(table_name::Union{String,Symbol}, column_name::Union{String,Symbol})
  throw(SearchLight.Exceptions.NotImplementedInAdapter("function column_id_sequence", "is not implemented in SearchLightOracle"))
end

function create_table_sql(f::Function, name::String, options::String = "") :: String
  "CREATE TABLE $name (" * join(f()::Vector{String}, ", ") * ") $options" |> strip
end

function SearchLight.Migration.column(name::Union{String,Symbol}, column_type::Union{String,Symbol}, options::Any = ""; default::Any = nothing, limit::Union{Int,Nothing,String} = nothing, not_null::Bool = false) :: String
  "$name $(TYPE_MAPPINGS[column_type] |> string) " *
    (default === nothing ? "" : " DEFAULT $default ") *
    (not_null ? " NOT NULL " : "") *
    string(options)
end

function sequence_name(table_name::Union{String,Symbol}, column_name::Union{String,Symbol}) :: String
  string(table_name) * "__" * "seq_" * string(column_name)
end

function SearchLight.Migration.create_sequence(name::Union{String,Symbol}) :: Nothing
  SearchLight.query("CREATE SEQUENCE $name")
  nothing
end

function SearchLight.Migration.create_sequence(table_name::Union{String,Symbol}, column_name::Union{String,Symbol}) :: Nothing
  res_column_name = column_name == "" ? string(SearchLight.primary_key_name) : column_name
  SearchLight.Migration.create_sequence(sequence_name(table_name, res_column_name))
  nothing
end

function SearchLight.Migration.remove_sequence(name::Union{String,Symbol}, options::Union{String,Symbol}) :: Nothing
  SearchLight.query("DROP SEQUENCE $name $options", internal = true)

  nothing
end

function SearchLight.Migration.remove_sequence(name::Union{String,Symbol}) :: Nothing
  SearchLight.Migration.remove_sequence(name,"")
end

function SearchLight.Migration.column_id(name::Union{String,Symbol} = "id", options::Union{String,Symbol} = ""; constraint::Union{String,Symbol} = "", nextval::Union{String,Symbol} = "") :: String
  "$name NUMBER(10) NOT NULL $options"
end

function reset_sequence(sequence_name)
  sql = "alter sequence $sequence_name restart start with 1"
  SearchLight.query(sql)
end

"""
  escape_column_name(c::String, conn::DatabaseHandle)::String

  Escapes the column name.

  # Examples
  ```julia
  julia>
  ```
"""
function SearchLight.escape_column_name(c::String, conn::DatabaseHandle = SearchLight.connection()) :: String
  join([cx for cx in split(c, '.')], '.')
end

"""
  escape_value{T}(v::T, conn::DatabaseHandle)::T

  Escapes the value `v` using native features provided by the database backend if available.

  # Examples
  ```julia
  julia>
  ```
"""
function SearchLight.escape_value(v::T, conn::DatabaseHandle = SearchLight.connection())::T where {T}
  isa(v, Number) ? v : "'$(replace(string(v), "'"=>"\\'"))'"
end

"""
current_value_seq -- value of a given sequence

  Returns the actual value of a given insert statement, theoreticaly also from a select statemnt.
  The prerequisite for this is the calling of nextval for this particulary squence in the actual session
"""
function current_value_seq(conn::SearchLightOracle.DatabaseHandle, sql::String)
  m  = match(r"(?i)\w+__SEQ_\w+_PK\.NEXTVAL",sql)
  id_name = ""
  sequenceName = m !== nothing ?  split(m.match,".")[1] : nothing
  if sequenceName !== nothing
    closures = eachmatch(r"\((.*?)\)", sql)
    matches  = [exp.match for exp in closures]
    matchStrings = [strip(w, [' ', '(', ')']) for w in matches]
    closureFields = split.(matchStrings,",")
    index_id = findfirst(x->occursin(sequenceName,x), closureFields[2])
    id_name = index_id !== nothing ? strip(closureFields[1][index_id]) : ""

    sql = "select $sequenceName.currval from dual"
    stmt = Oracle.Stmt(conn.db_handle,sql)
    result = Oracle.query(stmt)
  else
    result = nothing
  end
  return result, id_name
end



########################################################################
#                                                                      #
#           Utility funcitions                                         #
#                                                                      #
########################################################################

function connectionInfo(;db_handle::Oracle.Connection)::Dict{String,Any}

    db_conn = db_handle === nothing ? SearchLight.connection().db_handle : db_handle

    infoStringDict = Dict(
        ["host"     => "select sys_context('USERENV', 'IP_ADDRESS') ip_adress from dual",
        "username" => "select user from dual",
        "database" => "select ora_database_name from dual"])

    result = Dict([info => getEnvironmentInfo(sql,db_conn) for (info,sql) in infoStringDict])

end

function getEnvironmentInfo(sql::String, db_handle::Oracle.Connection)
    df = SearchLight.query(sql, db_handle) |> DataFrames.DataFrame
    return df[1,1]
end

"""
  function sequence_name_pk(m::T) where {T<:AbstractModel}
Oracle doesn't support the returning of a value direct within the insert or update statement.
Therefor it is nesessary to use it directly in preparing insert or update statement
"""

function sequence_name_pk(m::T) where {T<:SearchLight.AbstractModel}
  sequence_name_pk(typeof(m))
end

function sequence_name_pk(m::Type{T}) where {T<:SearchLight.AbstractModel}
  sequence_name_pk(SearchLight.Inflector.to_plural(string(m)))
end

function sequence_name_pk(table::Union{String,Symbol})
  rawTable = SearchLight.strip_module_name(table)
  default_sequence = uppercase(string(rawTable))
  default_sequence *= "__SEQ_"
  default_sequence *= uppercase(SearchLight.Inflector.to_singular(string(rawTable))) * "_PK"
end

function isInsertStmt(sql::String)::Bool
  m =  match(r"(?i)insert", sql)
  m !== nothing ? true : false
end

"""
Returns the columnnames in a select statment case sensitive

  It is meant to be a workaround that Oracle gaves back column names
  as uppercase strings. For the column names of the dataframes it is
  nesessary to bring that back to the original
"""

### Generator

function SearchLight.Generator.FileTemplates.adapter_default_config()
  """
  $(SearchLight.config.app_env):
    adapter:  Oracle
    host:     127.0.0.1
    port:     1521
    database: yourdb
    username: your_username
    password: your_password
  """
end

end # End of Modul

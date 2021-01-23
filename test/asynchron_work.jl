module Async_SearchLight

include("test_models.jl")

using SearchLight
using OrderedCollections
using .TestModels
using DataFrames
using Oracle
using SearchLight.QueryBuilder

export prepareDbConnection, initial_work, make_jobs, work_all
export Async_Work_Model

mutable struct Async_Work_Model
    jobs::Channel
    results::Channel
    fields::AbstractArray
    object_array::AbstractArray
    result_frame::Union{DataFrames.DataFrame,Nothing}
    objects_splits::AbstractArray
    frame_splits::AbstractArray
    model_type::Type{T} where {T<:Union{<:AbstractModel,<:Nothing}}
end
Async_Work_Model() = Async_Work_Model(Channel{Tuple}(1000),Channel{Tuple}(1000),[],[],nothing,[],[],Nothing)

function default_model_type()
    Author  
end

function prepareDbConnection()
  connection_file = "oracle_connection.yml"

  conn_info_oracle = SearchLight.Configuration.load(connection_file)
  conn = SearchLight.connect(conn_info_oracle)
  return conn
end

function initial_work()

  strucAsync = Async_Work_Model()
  strucAsync.model_type = default_model_type()
  strucAsync.fields = Async_SearchLight.retrieving_fields()
  splitting_interval = 1000
  chanel_size = 1000
  
  prepareDbConnection()
  
  sql = "SELECT authors.firstname AS authors_first_name, authors.id AS authors_id, authors.lastname AS authors_last_name FROM authors ORDER BY authors.id ASC"
  strucAsync.result_frame = SearchLight.query(sql)
  
  const strucAsync.jobs = Channel{Tuple}(chanel_size);
  const strucAsync.results = Channel{Tuple}(chanel_size);
  
  object_array = Array{Union{Any,Int64},2}(undef,nrow(strucAsync.result_frame),2)
  object_array[:,2] = [strucAsync.model_type() for i in 1:nrow(strucAsync.result_frame)]
  object_array[:,1] = 1:nrow(strucAsync.result_frame)
  
  object_array_part = SearchLight.interval_values(object_array,interval=splitting_interval)
  
  result_frame_part = SearchLight.interval_values(strucAsync.result_frame,interval=splitting_interval)
  
  strucAsync.object_array = object_array
  strucAsync.frame_splits = result_frame_part
  strucAsync.objects_splits = object_array_part

  strucAsync
end

function retrieving_fields()
    fields = [:authors_last_name =>:last_name,
            :authors_first_name => :first_name,
            :authors_id => :id]
end

function do_work(work_item)
  for job_id in work_item.jobs
      tmp_df = work_item.frame_splits[job_id[1]]
      tmp_rod = work_item.objects_splits[job_id[1]]
      tmp_field_db = job_id[2][1]
      tmp_field_model = job_id[2][2]
      tmp_pk = pk(typeof(tmp_rod[1,2])) == string(tmp_field_model)
      result_status = :ok
      for i in 1:nrow(tmp_df)
        try
          tmp_pk ? getfield(tmp_rod[i,2],tmp_field_model).value = tmp_df[i,tmp_field_db] :
            setfield!(tmp_rod[i,2],tmp_field_model,tmp_df[i,tmp_field_db])
        catch ex
          result_status = :error
        finally
          
        end
      end
      put!(work_item.results,(job_id[1],result_status)) 
  end
end

function make_jobs(async_item)
  for i in 1:length(async_item.frame_splits)
    for r in 1:length(async_item.fields)   
      put!(async_item.jobs, (i,async_item.fields[r]))
    end 
  end
end

function workbegin(n,work_item)
  for i in 1:n # start 4 tasks to process requests in parallel
    @async do_work(work_item)
  end
end

function exec_parallel(async_item)
  n = length(async_item.frame_splits) * length(async_item.fields)
  counter = 0
  array_error = []
  result_stand =["Standard 1","Standard 2"]
  while n > 0 # print out results
    if !isempty(async_item.results) 
      result = take!(async_item.results)
      !(result[2] == :ok) && push!(array_error,result)
      counter += 1
    end
    mod(n,100) == 0 && @info "Zeile $n ausgeführt: $counter"
    n -= 1
  end
  array_error
end

function work_all(n)
  work_horse = initial_work()
  @async make_jobs(work_horse)
  workbegin(n,work_horse)
  exec_parallel(work_horse)
end

function empty_results(results)
    while !isempty(results)
    res = take!(results)
    println(res)
    end  
end

function empty_jobs(jobs)
    while !isempty(jobs)
    ris = take!(jobs)
    println(ris)
    end
end

end #End module
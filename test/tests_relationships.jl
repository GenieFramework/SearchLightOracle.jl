using SearchLight, SearchLight.Migrations, SearchLight.Relationships
using SearchLight: connect

#To reach the db/migrations folder change the directory is necessary
cd(@__DIR__)

config_file = joinpath(@__DIR__, "oracle_connection.yml")
conn_info = SearchLight.Configuration.load(config_file)
conn = SearchLight.connect(conn_info)

try
  SearchLight.Migrations.status()
catch _
  SearchLight.Migrations.create_migrations_table()
end

isempty(SearchLight.Migrations.downed_migrations()) || SearchLight.Migrations.all_up!!()

Base.@kwdef mutable struct User <: AbstractModel
  id::DbId = DbId()
  username::String = ""
  password::String = ""
  name::String = ""
  email::String = ""
end

Base.@kwdef mutable struct Role <: AbstractModel
  id::DbId = DbId()
  name::String = ""
end

Base.@kwdef mutable struct Ability <: AbstractModel
  id::DbId = DbId()
  name::String = ""
end

u1 = findone_or_create(User, username = "a") |> save!
r1 = findone_or_create(Role, name = "abcd") |> save!
k2 = findone(Role)
if k2 === nothing
    println("Keine Role gefunden")
end

for x in 'a':'d'
  findone_or_create(Ability, name = "$x") |> save!
end

Relationships.Relationship!(u1, r1)

for a in all(Ability)
  Relationships.Relationship!(r1, a)
end

Relationships.related(u1, Role)
Relationships.related(findone(Role), Ability)
Relationships.related(u1, Ability, through = [Role])

tearDown()

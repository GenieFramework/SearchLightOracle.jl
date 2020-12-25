using Test, TestSetExtensions, SafeTestsets

include(joinpath(@__DIR__,"test_models.jl"))
module TestSetupTeardown

  using SearchLight
  using SearchLightOracle

  export prepareDbConnection, tearDown, connection_file

  connection_file = "oracle_connection.yml"

  function prepareDbConnection()
      
    conn_info_oracle = SearchLight.Configuration.load(connection_file)
    conn = SearchLight.connect(conn_info_oracle)
    return conn
  end

  function tearDown(conn)
    if conn !== nothing
        ######## Dropping used tables
        SearchLight.Migration.drop_migrations_table()

        # insert tables you use in tests here
        tables = ["Book","BookWithIntern","Callback","Author","BookWithAuthor"]

        # obtain tables exists or not, if they does drop it
        wheres = join(map(x -> uppercase(string("'", uppercase(SearchLight.Inflector.to_plural(x)), "'")), tables), " , ", " , ")
        queryString = string("SELECT table_name FROM user_tables where table_name in ($wheres)")
        result = SearchLight.query(queryString)
        for item in eachrow(result)
            try
                SearchLight.Migration.drop_table((item[1]))
            catch ex
                @show "Table $item doesn't exist"
            end 
        end 
  
        SearchLight.disconnect(conn)
        rm(SearchLight.config.db_migrations_folder, force=true, recursive=true)
    end
  end

end

@safetestset "Connection test" begin
  using SearchLight
  using SearchLightOracle
  using Main.TestSetupTeardown

  connection_file = "oracle_connection.yml"

  conn = prepareDbConnection()  
  @test conn !== nothing

  tearDown(conn)

end

@safetestset "Orcalce connection and infos" begin
    using SearchLight
    using SearchLightOracle
    using Main.TestSetupTeardown

  
    conn = prepareDbConnection()
  
    infoDB = SearchLightOracle.connectionInfo()

    conn_info_oracle = SearchLight.Configuration.load(connection_file)
    itemsToTest=["host","username","database"]

    for info in itemsToTest  
        @test uppercase(conn_info_oracle[info]) == uppercase(infoDB[info])
    end

    tearDown(conn)

end

@safetestset "Oracle query" begin
    using SearchLight
    using SearchLightOracle
    using SearchLight.Configuration
    using SearchLight.Migrations
    using Main.TestSetupTeardown

    conn = prepareDbConnection()

    queryString = string("select table_name from user_tables where table_name = upper('$(SearchLight.SEARCHLIGHT_MIGRATIONS_TABLE_NAME)')")

    @test isempty(SearchLight.query(queryString, conn)) == true
  
  # create migrations_table
    SearchLight.Migration.create_migrations_table()

    @test Array(SearchLight.query(queryString, conn))[1] == uppercase(SearchLight.SEARCHLIGHT_MIGRATIONS_TABLE_NAME)

    tearDown(conn)

end;

@safetestset "Models and tableMigration" begin
    using SearchLight
    using SearchLightOracle
    using Main.TestSetupTeardown
    using Main.TestModels

  ## establish the database-connection
    conn = prepareDbConnection()

  ## create migrations_table
    SearchLight.Migration.create_migrations_table()
  
  ## make Table "Book" 
    SearchLight.Generator.new_table_migration(Book)
    SearchLight.Migration.up()

    # testBook = Book(title="Faust", author="Goethe")

    # @test testBook.author == "Goethe"
    # @test testBook.title == "Faust"
    # @test typeof(testBook) == Book
    # @test isa(testBook, AbstractModel)

    # testBook |> SearchLight.save

    # @test testBook |> SearchLight.save == true

  ############ tearDown ##################

    tearDown(conn)

end

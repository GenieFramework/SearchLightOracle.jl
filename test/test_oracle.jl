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

@safetestset "Gesamt test" begin

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

        try
          for info in itemsToTest 
              @test uppercase(conn_info_oracle[info]) == uppercase(infoDB[info])
          end
        catch ex
            println("Fehler")
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

        testBook = Book(title="Faust", author="Goethe")

        @test testBook.author == "Goethe"
        @test testBook.title == "Faust"
        @test typeof(testBook) == Book
        @test isa(testBook, AbstractModel)

        try
          testBook |> SearchLight.save
        catch ex
          errorCode = ex
        end

        # @test (testBook |> SearchLight.save) === true

      ############ tearDown ##################
        SearchLight.Migration.down()
        tearDown(conn)

    end

    @safetestset "Model Store and Query models without intern variables" begin
        using SearchLight
        using SearchLightOracle
        using Main.TestModels

        using Main.TestSetupTeardown

      ## establish the database-connection
        conn = prepareDbConnection()

      ## create migrations_table
        SearchLight.Migration.create_migrations_table()
      
      ## make Table "Book" 
        SearchLight.Generator.new_table_migration(Book)
        SearchLight.Migration.up()

        testBooks = Book[]
      
      ## prepare the TestBooks
        for book in TestModels.seed() 
            push!(testBooks, Book(title=book[1], author=book[2]))
        end

        @test testBooks |> SearchLight.save == true

        booksReturn = SearchLight.find(Book)

        @test size(booksReturn) == (5,)

    
      ############ tearDown ##################
        SearchLight.Migration.down()
        tearDown(conn)

    end

    @safetestset "Query and Models with intern variables" begin
      using Test
      using SearchLight
      using SearchLightOracle
      using Main.TestSetupTeardown
      using Main.TestModels

      ## establish the database-connection
      conn = prepareDbConnection()

      ## make Table "BooksWithInterns" 
      SearchLight.Migration.create_migrations_table()
      SearchLight.Generator.new_table_migration(BookWithInterns)
      SearchLight.Migration.up()

      booksWithInterns = BookWithInterns[]

      ## prepare the TestBooks
      for book in TestModels.seed() 
          push!(booksWithInterns, BookWithInterns(title=book[1], author=book[2]))
      end

      testItem = BookWithInterns(author="Alexej Tolstoi", title="Krieg oder Frieden")

      savedTestItem = SearchLight.save(testItem)
      @test savedTestItem === true

      savedTestItems = booksWithInterns |> save
      @test savedTestItems === true

      idTestItem = SearchLight.save!(testItem)
      @test idTestItem.id !== nothing
      @test idTestItem.id.value  > 0

      resultBooksWithInterns = booksWithInterns |> save!

      fullTestBooks = find(BookWithInterns)
      @test isa(fullTestBooks, Array{BookWithInterns,1})
      @test length(fullTestBooks) > 0
  
      ############ tearDown ##################
      tearDown(conn)

    end

    @safetestset "Saving and Reading fields and datatbase columns are different" begin
        using SearchLight
        using SearchLightOracle
        using Main.TestSetupTeardown
        using Main.TestModels

        conn = prepareDbConnection()


        SearchLight.Migration.create_migrations_table()
        SearchLight.Generator.new_table_migration(BookWithAuthor)
        SearchLight.Migration.up()
        SearchLight.Generator.new_table_migration(Author)
        SearchLight.Migration.up()


        testAuthor = Author(firstname="Johann Wolfgang", lastname="Goethe")
        testId = testAuthor |> save! 

        @test length(find(Author)) > 0 

        ####### tearDown #########
        tearDown(conn)
    end

    @safetestset "Saving and Reading fields and datatbase columns are different" begin
      using SearchLight
      using SearchLightOracle
      using Main.TestSetupTeardown
      using Main.TestModels

      conn = prepareDbConnection()
      SearchLight.Migration.create_migrations_table()
      SearchLight.Generator.new_table_migration(BookWithAuthor)
      SearchLight.Migration.up()
      SearchLight.Generator.new_table_migration(Author)
      SearchLight.Migration.up()

      testAuthor = Author(firstname="Johann Wolfgang", lastname="Goethe")
      testId = testAuthor |> save! 

      @test length(find(Author)) > 0 

      ####### tearDown #########
      SearchLight.Migration.all_down!!(confirm=false)
      tearDown(conn)
  end;

    @safetestset "Saving and Reading Models with fields containing submodels" begin
        using SearchLight
        using SearchLightOracle
        using Main.TestSetupTeardown
        using Main.TestModels

        conn = prepareDbConnection()
        SearchLight.Migration.create_migrations_table()
        SearchLight.Generator.new_table_migration(BookWithAuthor)
        SearchLight.Migration.up()
        SearchLight.Generator.new_table_migration(Author)
        SearchLight.Migration.up()

        #create an author
        testAuthor = Author(firstname="John", lastname="Grisham")
        #create books from the author above and bring it to them 
        testAuthor.books = map(book -> BookWithAuthor(title=book), seedBook())
      
        testId = testAuthor |> save! 

        idAuthor = testAuthor.id.value
        for book in testId.books
          @test book.id_author.value == idAuthor
        end

        result = find(Author)
        @test length(result) > 0 

        @test !isempty(result[1].books)
        @test length(result[1].books) == length(seedBook())

        ####### tearDown #########
        SearchLight.Migration.all_down!!(confirm=false)
        tearDown(conn)
    end;

    @safetestset "functions findone_or_create, updateby_or_create etc" begin
      using SearchLight
      using SearchLightOracle
      using Main.TestSetupTeardown
      using Main.TestModels

      conn = prepareDbConnection()
      SearchLight.Migration.create_migrations_table()
      SearchLight.Generator.new_table_migration(BookWithAuthor)
      SearchLight.Migration.up()
      SearchLight.Generator.new_table_migration(Author)
      SearchLight.Migration.up()

      #create an author
      testAuthor = Author(firstname="John", lastname="Grisham")
      #create books from the author above and bring it to them 
      testAuthor.books = map(book -> BookWithAuthor(title=book), seedBook())

      result = findone_or_create(typeof(testAuthor))

      @test result !== nothing
      @test result.first_name == ""
      @test result.last_name == ""
      @test isempty(result.books)

      ####### tearDown #########
      SearchLight.Migration.all_down!!(confirm=false)
      tearDown(conn)
    end;
  
end
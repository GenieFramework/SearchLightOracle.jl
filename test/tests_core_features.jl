@testset "Core features" begin

  config_file = joinpath(@__DIR__, "oracle_connection.yml")

  @safetestset "Oracle configuration" begin
    using SearchLight

    config_file = joinpath(@__DIR__, "oracle_connection.yml")
    conn_info = SearchLight.Configuration.load(config_file)

    @test conn_info["adapter"] == "Oracle"
    @test conn_info["host"] == "127.0.0.1"
    @test conn_info["password"] == "searchlight"
    @test conn_info["config"]["log_level"] == ":debug"
    @test conn_info["config"]["log_queries"] == true
    @test conn_info["port"] == 51521
    @test conn_info["username"] == "searchlight_test"
    @test conn_info["database"] == "XEPDB1"

  end;

  @safetestset "Oracle connection" begin
    using SearchLight
    using SearchLightOracle

    config_file = joinpath(@__DIR__, "oracle_connection.yml")
    conn_info = SearchLight.Configuration.load(config_file)
    conn = SearchLight.connect()

    @test conn.host == "127.0.0.1"
    @test conn.port == "51521"
    @test conn.db == "XEPDB1"
    @test conn.user == "searchlight_test"

    SearchLight.disconnect(conn)
  end;

  @safetestset "Oracle query" begin
    using SearchLight
    include(joinpath(@__DIR__, "setUp_tearDown.jl"))

    config_file = joinpath(@__DIR__, "oracle_connection.yml")
    conn_info = SearchLight.Configuration.load(config_file)
    conn = SearchLight.connect()

    @test isempty(SearchLight.query("SELECT table_name FROM user_tables")) == true
    @test SearchLight.Migration.create_migrations_table() == true
    @test Array(SearchLight.query("SELECT t.table_name FROM user_tables t WHERE t.table_name = 'SCHEMA_MIGRATIONS'"))[1] == uppercase(SearchLight.config.db_migrations_table_name)

    tearDown()
  end;
end

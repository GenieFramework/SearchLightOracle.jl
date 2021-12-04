function tearDown()
    query = "select table_name from USER_TABLES"

    names = SearchLight.query(query)

    for tab_name in names[:,1]
        query_str = "drop table " * tab_name
        SearchLight.query(query_str)
    end

    query = "select SEQUENCE_NAME from USER_SEQUENCES"

    names = SearchLight.query(query)

    for seq_name in names[:,1]
        query_str = "drop sequence " * seq_name
        SearchLight.query(query_str)
    end
end

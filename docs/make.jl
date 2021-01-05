
using Documenter, SearchLightOracle, SearchLight

makedocs(
    sitename = "SearchLightOracle.jl",
    modules = [ SearchLightOracle ],
    pages = [ "Home" => "index.md",
            ],
)

deploydocs(
    repo = "github.com/FrankUrbach/SearchLightOracle.jl.git",
    target = "build",
)

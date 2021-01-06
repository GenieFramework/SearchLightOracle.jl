using Documenter, SearchLight
using SearchLightOracle

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

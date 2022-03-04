cd(@__DIR__)

using Pkg

using Test, TestSetExtensions, SafeTestsets
using SearchLight

include(joinpath(@__DIR__, "setUp_tearDown.jl"))

@testset ExtendedTestSet "SearchLight tests" begin
  @includetests ARGS
end

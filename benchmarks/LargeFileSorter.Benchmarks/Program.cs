using BenchmarkDotNet.Running;

// BenchmarkSwitcher lets the user pick which suite to run:
//
//   dotnet run -c Release                          → interactive picker
//   dotnet run -c Release -- --filter '*Strategy*' → stream vs mmf comparison
//   dotnet run -c Release -- --filter '*Sorting*'  → naive vs sequential vs optimized
//   dotnet run -c Release -- --filter '*'          → run everything
//
BenchmarkSwitcher
    .FromAssembly(typeof(Program).Assembly)
    .Run(args);

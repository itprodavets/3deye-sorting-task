using BenchmarkDotNet.Running;

// BenchmarkSwitcher lets the user pick which suite to run:
//
//   dotnet run -c Release                               → interactive picker
//   dotnet run -c Release -- --filter '*Strategy*'      → stream vs mmf vs shard (head-to-head)
//   dotnet run -c Release -- --filter '*Sorting*'       → naive vs sequential vs optimized (historical)
//   dotnet run -c Release -- --filter '*ThreadScaling*' → Threads sweep {1..16} × {stream, mmf, shard}
//   dotnet run -c Release -- --filter '*MergePhase*'    → Phase 2 only: stream-merge vs shard-merge
//   dotnet run -c Release -- --filter '*'               → run everything
//
BenchmarkSwitcher
    .FromAssembly(typeof(Program).Assembly)
    .Run(args);

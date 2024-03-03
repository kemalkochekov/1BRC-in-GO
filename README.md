go test -cpuprofile cpu.prof -memprofile mem.prof -bench .
Then, analyze the profiles using go tool pprof:

go tool pprof cpu.prof
go tool pprof mem.prof
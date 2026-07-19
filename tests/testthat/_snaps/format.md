# flat source status snapshot

    Code
      cat(format(st, color = FALSE), sep = "\n")
    Output
      pump_status
        pulled:  7
        errors:  2
        pops:    7 hits, 3 misses

# multi-stage frame snapshot (ascii, no color)

    Code
      cat(format(st, header = "pump", color = FALSE, unicode = FALSE), sep = "\n")
    Output
      <pump (20)>
      +- source   12/20
      +- stage 1 main  * bottleneck
      |    wrk [#----] 1/4   buf [##---] 2/5   done 8   err 0
      |    fn 42.0ms/it   crd 1.5ms/bt   wrk 80% stv 10% blk 10%
      +- stage 2 mirai
      |    wrk [-----] 0/4   buf [#####] 5/5   done 4   err 0
      |    fn 8.0ms/it   crd 0.3ms/bt   wrk 30% stv 60% blk 10%
      +- sink   4/20

# bottleneck marker + error chips snapshot

    Code
      cat(format(st, header = "pump", color = FALSE, unicode = TRUE), sep = "\n")
    Output
      <pump (20)>
      ┌─ source   12/20   err 3
      ├─ stage 1 main  * bottleneck
      │    wrk [##---] 2/4   buf [###--] 3/5   done 8   err 0
      │    fn 42.0ms/it   crd 1.5ms/bt   wrk 90% stv 0% blk 10%
      ├─ stage 2 mirai
      │    wrk [#----] 1/4   buf [#####] 5/5   done 4   err 2
      │    fn 8.0ms/it   crd 0.3ms/bt   wrk 20% stv 70% blk 0%
      └─ sink   4/20

# stuck-job warning snapshot

    Code
      cat(format(st, header = "pump", color = FALSE, unicode = TRUE), sep = "\n")
    Output
      <pump (10)>
      ┌─ source   5/20
      ├─ stage 1 main
      │    wrk [#----] 1/4   buf [-----] 0/5   done 4   err 0
      │    fn 8.0ms/it   crd 0.3ms/bt   wrk 100% stv 0% blk 0%
      │    oldest in flight: 5.0s (id 7)
      └─ sink   4/20

# colored render snapshot exercises the ANSI path

    Code
      cat(format(st, header = "pump", color = TRUE, unicode = TRUE), sep = "\n")
    Output
      [1m<pump (8)>[0m
      [2m┌─[0m [1msource[0m   6/10   [91merr 1[0m
      [2m├─[0m [94mstage 1[0m main
      [2m│[0m    [2mwrk[0m [[32m##[0m[2m---[0m] 1/2   [2mbuf[0m [[31m#####[0m] 2/2   [2mdone[0m 6   [91merr 1[0m
      [2m│[0m    [2mfn[0m 120ms[2m/it[0m   [2mcrd[0m 5.0ms[2m/bt[0m   [2mwrk[0m [92m50%[0m [93mstv[0m [93m25%[0m [91mblk[0m [91m25%[0m
      [2m└─[0m [1msink[0m   6/10


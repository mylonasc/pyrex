## Benchmarking

This is to store some benchmarks of different embedded KV stores. 

The B+Tree -- based `LMDB` and LSM tree -- based `Plyvel` are two common ones. 


The results for "small" data seem to favor `LMDB`

| db_type | workload | operations | time_taken_sec | ops_per_sec | notes |
|---|---|---|---|---|---|
| Plyvel | Sequential Writes | 300000 | 0.800414 | 374806.146508 | |
| Plyvel | Random Writes | 300000 | 1.353231 | 221691.627449 | |
| Plyvel | Batch Writes | 300000 | 1.237599 | 242404.861788 | |
| Plyvel | Sequential Reads | 300000 | 0.530876 | 565103.310855 | |
| Plyvel | Random Reads | 300000 | 0.899977 | 333341.712065 | |
| Plyvel | Random Deletes | 300000 | 0.817028 | 367184.674774 | |
| Plyvel | Iteration | 0 | 0.115943 | 0.000000 | |
| Plyvel | Compaction | 1 | 0.068957 | 14.501816 | Full DB compaction |
| LMDB | Sequential Writes | 300000 | 0.511226 | 586824.559849 | |
| LMDB | Random Writes | 300000 | 1.145724 | 261843.091915 | |
| LMDB | Batch Writes | 300000 | 1.222433 | 245412.292258 | |
| LMDB | Sequential Reads | 300000 | 0.475163 | 631361.934298 | |
| LMDB | Random Reads | 300000 | 0.604456 | 496313.720812 | |
| LMDB | Random Deletes | 300000 | 0.779530 | 384847.480698 | |
| LMDB | Iteration | 0 | 0.000234 | 0.000000 | |

For larger data `LMDB` crashes in random deletes and is very slow.

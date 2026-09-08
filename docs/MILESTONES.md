# Milestones

## `v1-redis-foundation` - Baseline complete

Status: **completed on 2026-09-08**

Delivered:

- 13-lesson Redis learning path
- deterministic simulations and parameter comparison tasks
- guided, namespaced real Redis command exercises
- stage assessments, review queue and learning report
- return-value validation for real command steps
- local Java RESP gateway with a command allowlist

Not included in this milestone: disposable multi-node topologies, arbitrary
production-like concurrency, public runtime hosting, or complete deep-dive
chapters for all 13 lessons. These are tracked in `BACKLOG.md`.

## `v2-jvm-foundation` - Baseline complete

Status: **completed on 2026-09-08**

Phase 1 scope:

- Class files and operand-stack bytecode execution
- class loading, linking, initialization and parent delegation
- JVM runtime data areas
- object allocation and TLAB
- GC Roots and reachability analysis
- generational collection and promotion

Phase 2 delivered:

- real `javap -c -p` output for a fixed compiled sample
- actual ClassLoader chain and Bootstrap null representation
- MemoryMXBean and MemoryPoolMXBean snapshots
- thread allocation-byte measurement for a bounded allocation probe
- WeakReference observation with explicit non-deterministic GC semantics
- GarbageCollectorMXBean names, counts and accumulated time
- fixed probe allowlist with no user-provided source, class name or process command

Phase 3 delivered:

- full deep-dive chapters for all six lessons with causal explanations, evidence tables and common misconceptions
- business transfer questions with expandable reference analysis and official Oracle/OpenJDK references
- three JVM-specific stage assessments, module-scoped review queue and topic report
- module-aware learning center shared by Redis and JVM without mixing their weak-question counts
- desktop and 390px mobile browser verification, plus successful real-probe checks for all six lessons

The next JVM milestone covers collector comparison, GC log analysis and
OOM/leak diagnosis. JIT compilation and escape analysis follow after diagnosis.

## `v3-jvm-diagnostics` - In progress

Status: **phase 1 implemented**

Phase 1 delivered:

- Serial, Parallel and G1 collector comparison lesson
- deterministic comparison of pause work, worker parallelism, concurrent work and reclaimed volume
- three required collector scenarios under the same modeled workload
- current-JVM collector identification through the existing safe GC MXBean probe
- collector-specific deep dive, business transfer question and stage assessment
- extensible diagram layout for lessons with more than four architecture nodes

Next exit criteria:

- parse and explain real GC log events without accepting arbitrary file paths or JVM commands
- correlate allocation, survival, promotion and pause evidence on one timeline
- add controlled heap, Metaspace, direct-memory and thread-resource failure diagnosis
- complete desktop and mobile verification for the full diagnostics path

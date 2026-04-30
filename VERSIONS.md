# Course versions — Apache DataFusion

Last review: 2026-04-30
Next review: 2026-07-30

## Cadence

Квартальный — DataFusion релизит мажор/минор каждые ~6 недель, Comet и LakeSail активно эволюционируют, экосистема Arrow обновляется ежеквартально.

## Pinned baseline (April 2026)

| Component | Version | Released | Course depth |
|-----------|---------|----------|--------------|
| Apache DataFusion (Rust) | 53.0.0 | 2026-04 | full |
| datafusion-python | 53.0.0 | 2026-04 | full |
| Apache Arrow (Rust crate) | 54.x | 2026-Q1 | full |
| Apache Comet | 0.14.0 | 2026-03 | full |
| LakeSail / Sail (distributed DataFusion) | OSS preview | 2026-Q1 | partial |
| Ballista | 0.12.x | 2026 | mention |
| Substrait | latest stable | 2026 | partial |
| iceberg-rust | 0.8.0 | 2026-Q1 | partial |
| object_store crate | 0.11.x | 2026 | full |
| parquet (Rust) | 54.x | 2026-Q1 | full |

## Forthcoming (next review)

- DataFusion 54+ — потенциальный async TableProvider API.
- Comet 0.15 / 1.0 GA — full-stage Arrow execution в Spark.
- LakeSail multi-tenant scheduling.
- Substrait 0.55+ — стабилизация extensions для DataFusion.
- DataFusion Spark dialect (для миграции).

## Recent updates

- 2026-04-30 — Wave 1 P0 правки (DataFusion 53, Comet 0.14, LakeSail) + Wave 2 новые уроки (distributed execution, lakehouse integration) + Wave 3 cross-refs (storage-formats, spark-course).

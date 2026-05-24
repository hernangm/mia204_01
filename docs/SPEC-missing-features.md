# Phase: Missing Features for Final Delivery — Specification

**Created:** 2026-05-19
**Ambiguity score:** 0.16 (gate: ≤ 0.20)
**Requirements:** 6 locked

## Goal

Close the rubric-critical gap in the feature-store pipeline so training and inference both consume from `featurestore.features`, refresh `docs/checklist_implementacion.md` to reflect the actual implementation state, and restructure remaining work so the final-delivery PR history visibly reflects both team members' contributions.

## Background

The partial-delivery feedback from the teaching team flagged "Feature Store para entrenamiento — Puede mejorar: No usan 'feature store'. Usan simplemente una DB." Verification against `etapa-3-ray-serve` (5 commits ahead of `main`, all three Etapas implemented) confirmed two MUST-clauses fail:

- The `features` table is defined in [db/init-featurestore.sql:41-51](../db/init-featurestore.sql) but no pipeline code writes to it. `scripts/load_featurestore.py` populates only `production` and `wells`.
- The `ml_pipeline` DAG ([airflow/dags/dag_pozos.py:163-178](../airflow/dags/dag_pozos.py)) passes preprocessed data directly via XCom dict from `preprocess` → `train_experiments`. Training never queries the feature store.
- Consequently the API `/forecast` endpoint ([api/app/repositories/postgres.py:56-90](../api/app/repositories/postgres.py)) reads `features` and returns empty for any known well — Ray Serve is never reached in practice.

Etapas 1 (monthly schedule), 2 (drift/decay with PSI + KS + RMSE), and 3 (Ray Serve, 2 replicas) are implemented and committed but never merged to `main`. The checklist `docs/checklist_implementacion.md` predates them and still marks all final-delivery items as TODO.

Recent git history (Etapas 1–3, Apr-16 polish, and new docs `docs/guia_plataforma.md`/`docs/guion_video.md`) shows `Hernan` as the sole author with Claude as co-author. The team-collaboration rubric requires commit/PR history to reflect distributed work between both members.

## Requirements

1. **Features table populated by DAG**: `ml_pipeline` writes preprocessed rows to `featurestore.features` on every run.
   - Current: `features` table exists in schema but no pipeline code writes to it; only seeded by integration test fixtures.
   - Target: A new task (e.g. `persist_features`) in `dag_pozos.py` writes the preprocessed dataframe to `featurestore.features` with columns `(id_pozo, fecha, tipoextraccion, prod_gas, prod_agua, tef, prod_pet, profundidad)` after applying the static `TIPOEXTRACCION_MAP`. Existing rows for the same `(id_pozo, fecha)` are overwritten or upserted — not duplicated.
   - Acceptance: After one full DAG run on a fresh DB volume, `SELECT COUNT(*) FROM features` returns `> 0`. After a second DAG run with the same `date_from`/`date_to`, `COUNT(*)` is unchanged (idempotent).

2. **Training consumes from feature store**: The training task reads its dataset from `featurestore.features`, not from an in-memory XCom from `preprocess`.
   - Current: [training.py:23-56](../airflow/plugins/ml_pipeline/training.py) receives `data_dict` produced from the raw CSV in `preprocess`.
   - Target: A new DAG task (e.g. `read_features_from_store`) executes `SELECT … FROM features WHERE fecha BETWEEN :date_from AND :date_to` and produces the dict consumed by `train_experiments`. `preprocess`'s output is no longer passed to training.
   - Acceptance: With the `features` table truncated, triggering `ml_pipeline` causes the training task to raise `RuntimeError` with a message containing "feature store" or "features" — training MUST NOT silently fall back to raw CSV.

3. **MLflow audit trail for feature-store consumption**: Every training run logs a tag and a parameter proving training data came from the store.
   - Current: No MLflow tag/param indicates the data source. Reviewing a run in the MLflow UI cannot distinguish CSV-trained from store-trained runs.
   - Target: Each run sets MLflow tag `data_source = "featurestore"` and params `feature_store_rows = N` (positive int) and `feature_store_date_from`/`feature_store_date_to` (ISO dates or `"all"` if unfiltered).
   - Acceptance: For any run created by the new pipeline, `mlflow.MlflowClient().get_run(run_id).data.tags["data_source"] == "featurestore"` and `int(run.data.params["feature_store_rows"]) > 0`.

4. **End-to-end inference smoke test**: After a DAG run, the API returns non-empty forecast data for a known well in the trained date range.
   - Current: `scripts/test_e2e.sh` exists ([scripts/test_e2e.sh](../scripts/test_e2e.sh)) but does not assert on a forecast result.
   - Target: `scripts/test_e2e.sh` adds a step that triggers the DAG, waits for completion, then `curl`s `/api/v1/forecast` for a known seeded `id_well` plus `date_start`/`date_end` and asserts the JSON `data` array has length `> 0`.
   - Acceptance: Running `bash scripts/test_e2e.sh` against the docker-compose stack exits 0; failing the forecast-non-empty assertion causes the script to exit non-zero and print the offending response body.

5. **`docs/checklist_implementacion.md` matches reality**: The checklist reflects what is implemented on the `etapa-3-ray-serve` branch (post-merge with these fixes) instead of pre-Etapa-1 state.
   - Current: All final-delivery items are `[ ]`; "Feature Store persistente" is `[x]` despite the table being unpopulated.
   - Target: Each line is either `[x]` with a one-line evidence pointer (`file:line` or DAG name), or `[ ]` with a one-line note pointing to the actual remaining gap. The list also includes the two MUSTs from this spec (#1 + #2).
   - Acceptance: A reviewer reading the checklist cold can locate the implementation for every `[x]` item via the cited reference within one click/grep.

6. **PR history visibly distributes work between team members**: Remaining final-delivery commits and PRs visibly originate from both `Hernan` and `Matias`, and the README explicitly attributes work.
   - Current: All five `etapa-*` commits + Apr-16 polish + new docs (`guia_plataforma.md`, `guion_video.md`) are authored by `Hernan`. Only `6dafcb7` ("Matias, corregí docker-compose.yaml…") references the second team member.
   - Target: From this point through `main` merge, at least one merged PR (or direct commit) for each remaining work area (feature-store fix, checklist refresh, README update, video recording) is authored by `Matias`. README adds a "Equipo y contribuciones" subsection listing both members and the areas each owned.
   - Acceptance: `git log etapa-3-ray-serve..HEAD --format='%an'` on the final-delivery branch shows ≥ 1 commit from each of `Hernan` and `Matias`; README contains a heading literally matching `Equipo` or `Team` with both names beneath it.

## Boundaries

**In scope:**
- New DAG tasks `persist_features` and `read_features_from_store` in `dag_pozos.py`.
- Modifying `train_experiments` to consume the store-read dict instead of the preprocess-produced dict (signature unchanged; producer changes).
- MLflow tags and params on the training run capturing feature-store provenance.
- New assertion in `scripts/test_e2e.sh` for `/forecast` returning non-empty data.
- Full rewrite of `docs/checklist_implementacion.md` to current state.
- README "Equipo y contribuciones" subsection.
- Splitting remaining final-delivery work into PRs that name both team members as authors.

**Out of scope:**
- README "Decisiones de diseño" section justifying Postgres-as-feature-store vs Feast/Tecton/Hopsworks — user explicitly deferred this; can be added later as a separate doc or README section if the evaluator pushes back on "Puede mejorar."
- Replacing PostgreSQL with a real feature-store library (Feast, Tecton, Hopsworks) — educational scope, schedule constraint (9 days to final delivery), and the partial feedback was "Puede mejorar" not "DEBE cambiar."
- Etapas 1 / 2 / 3 themselves — already implemented on `etapa-3-ray-serve`, no functional changes needed.
- Recording the 6-minute demo video — script (`docs/guion_video.md`) exists; recording is a separate deliverable that doesn't change code.
- Retroactively rewriting git authorship on already-merged commits — deceptive; only forward commits count for the team-collaboration rubric.

## Constraints

- MUST use the existing `featurestore` PostgreSQL database and the existing `features` table schema in [db/init-featurestore.sql](../db/init-featurestore.sql). Schema changes to that table require coordinated changes in the API's `get_features` (which reads `id_pozo, fecha, tipoextraccion, prod_gas, prod_agua, tef, prod_pet, profundidad`) and the drift DAG (which `SELECT *` from the same table).
- MUST work with the current docker-compose service set — no new services. Postgres connection string MUST come from `FEATURESTORE_DB_URL` env var (added in the prior commit), not hardcoded.
- The DAG's `date_from` / `date_to` params already exist on `ml_pipeline` — the new `read_features_from_store` task MUST honor them. If both are null, read all rows.
- The fail-loud guarantee (Req #2 Acceptance) means the training task MUST NOT have a fallback path to raw CSV. The whole point of the rubric clause is that the store is on the critical path.
- The feature-engineering logic (column selection, `TIPOEXTRACCION_MAP`, dropna) is currently in `preprocess` in `dag_pozos.py`. It MUST be applied before persistence so the stored rows are model-ready — the training task should be able to feed them to scikit-learn directly without extra transformation.

## Acceptance Criteria

- [ ] `SELECT COUNT(*) FROM featurestore.features` returns `> 0` after a fresh `ml_pipeline` DAG run.
- [ ] Running `ml_pipeline` twice with identical `date_from`/`date_to` does not duplicate rows for the same `(id_pozo, fecha)` pair.
- [ ] With `features` truncated, the training task raises `RuntimeError` (or equivalent failure) referencing the empty feature store; the DAG run is marked failed.
- [ ] Each successful training run has MLflow tag `data_source=featurestore` and param `feature_store_rows` with a positive integer value.
- [ ] `bash scripts/test_e2e.sh` exits 0 and the script asserts `/api/v1/forecast` returns `data` of length `> 0` for a seeded well in the trained date range.
- [ ] `docs/checklist_implementacion.md` has zero items that disagree with the codebase: every `[x]` has an inline reference (file path or DAG name) and every `[ ]` notes the specific remaining gap.
- [ ] `git log` on the final-delivery branch shows ≥ 1 commit authored by `Matias` and ≥ 1 commit authored by `Hernan` after the SPEC was committed.
- [ ] README contains a heading `Equipo` or `Team` with both team members listed and a one-line description of each member's areas of ownership.

## Ambiguity Report

| Dimension          | Score | Min  | Status | Notes                                                          |
|--------------------|-------|------|--------|----------------------------------------------------------------|
| Goal Clarity       | 0.85  | 0.75 | ✓      | Scope locked by user; train flow and acceptance defaulted to recommendation |
| Boundary Clarity   | 0.85  | 0.70 | ✓      | Explicit out-of-scope: design-decisions doc deferred, no Feast |
| Constraint Clarity | 0.80  | 0.65 | ✓      | Existing schema + FEATURESTORE_DB_URL env var must be honored  |
| Acceptance Criteria| 0.85  | 0.70 | ✓      | 8 pass/fail criteria, all checkable post-hoc                   |
| **Ambiguity**      | 0.16  | ≤0.20| ✓      |                                                                |

## Interview Log

| Round | Perspective       | Question summary                                | Decision locked                                                                 |
|-------|-------------------|-------------------------------------------------|---------------------------------------------------------------------------------|
| 0     | (pre-interview)   | Initial ambiguity assessment from prior audit   | 0.43 — Goal and Boundary below minimum; scope of "missing features" unclear     |
| 1     | Researcher + Boundary Keeper | Scope of the spec (multi-select)     | Feature-store pipeline + checklist refresh + PR-history fix; README design-decisions deferred |
| 1     | Researcher        | How should training consume the store?         | DAG persists then reads back with date_from/date_to filter; fail loud on empty   |
| 1     | Failure Analyst   | What proves the store is actually used?        | All 4 criteria adopted (row count, training-fails-on-empty, MLflow audit tag, E2E /forecast smoke) |

---

*Spec created: 2026-05-19*
*Next step: `/gsd:discuss-phase` (or direct implementation) — `discuss-phase` will read this file and skip "what/why" questions; design-time decisions remaining are upsert strategy (INSERT … ON CONFLICT vs TRUNCATE+APPEND), failure-mode wording, exact MLflow tag schema, and how to record Matias's contributions (rebase, fresh PR, or paired-commit Author/Co-Author swap).*

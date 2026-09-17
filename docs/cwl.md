# CWL workflows

The controller accepts **Common Workflow Language** documents as job DAGs. This tree parses a CWL graph, rejects cycles, and schedules steps with the same QoS, GRES, and cgroup path as a normal `veloce submit`.

This is a Veloce-shaped CWL path, not a full `cwltool` replacement (no arbitrary CWL 1.2 feature matrix).

## Submit

```bash
veloce submit --cwl ./workflow.cwl
veloce submit --json --cwl ./workflow.cwl
```

`--cwl` does not need a trailing command. The CLI reads the file, builds a DAG (`veloce-common` CWL parser), and sends `SubmitDag` over Noise. HTTPS equivalent: `POST /api/v1/jobs/cwl` (see [rest.md](rest.md)).

The dashboard can upload a CWL file on the submit page as well.

## What the engine uses

- **Steps and dependencies** — edges become controller job steps; cyclic graphs fail before enqueue.
- **Command / container** — step commands and optional Apptainer images follow the same worker launch path as a single job (`--image` / solver manifest). Pair with [apptainer.md](apptainer.md) when a step is a `.sif`.
- **User and working directory** — same rules as `veloce submit` (OS user when `--user` is omitted).

Keep workflow YAML next to the lab directory, or stage inputs through the fileserver (`--input-deck` on ordinary submits; CWL file refs must be reachable from the worker).

## Out of scope on this page

Full CWL conformance, `cwl-runner` flags, and Scatter/`when` edge cases. If a construct is not in the parser, submission fails closed rather than silently dropping a step. Inspect `veloce jobs show` / `veloce jobs events` after submit.

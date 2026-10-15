Project: single-node LSM-tree key-value store in Python 3.11+.
Environment: Windows, PowerShell. Do not use && or Unix-only commands.
Rules:
- Work ONLY on the file(s) named in the task. Do not rewrite other files.
- Keep each module under 300 lines. Standard library only, except pytest.
- Never run a server, benchmark or loop in the foreground. Run long things in the background with a timeout.
- Run tests as: pytest -x -q --timeout=30 2>&1 | Select-Object -Last 40
- After finishing, append 3 lines to PROGRESS.md: what was built, file names, what's next.
- Do not start the next task. Stop and wait.
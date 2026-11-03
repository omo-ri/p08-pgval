FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

# Build context is the repository root; copy runtime modules only.
COPY pgval/__init__.py pgval/plan.py /app/pgval/
COPY pgval/generator/__init__.py pgval/generator/__main__.py pgval/generator/main.py /app/pgval/generator/
COPY pgval/generator/generators/*.py /app/pgval/generator/generators/
COPY pgval/validator/__init__.py pgval/validator/__main__.py pgval/validator/main.py /app/pgval/validator/

USER 65532:65532
CMD ["python", "-m", "pgval.generator", "--help"]

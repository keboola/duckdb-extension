FROM python:3.12-slim

RUN pip install uv

WORKDIR /tests
COPY pyproject.toml uv.lock* ./
RUN uv sync

COPY . .

ENTRYPOINT ["uv", "run", "pytest", ".", "-m", "live", "-n", "4", "-v", "--tb=short"]

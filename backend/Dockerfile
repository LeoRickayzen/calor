# AWS Lambda container image for the houses backend (FastAPI + Mangum).
# Build from backend/: docker build -t houses-backend .
FROM public.ecr.aws/lambda/python:3.11

COPY pyproject.toml ${LAMBDA_TASK_ROOT}/
COPY app ${LAMBDA_TASK_ROOT}/app

RUN pip install --no-cache-dir -t ${LAMBDA_TASK_ROOT} .

# Use real DynamoDB (no local endpoint) when this image runs in Lambda.
ENV HOUSES_DYNAMODB_ENDPOINT_URL=""

CMD ["app.lambda_handler.handler"]

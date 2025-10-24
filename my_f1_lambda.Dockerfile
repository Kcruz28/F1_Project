# Start from the official AWS Lambda Python image
FROM public.ecr.aws/lambda/python:3.10

# Install the fastf1 library (and all its dependencies)
# This installs pandas, numpy, etc., all at once.
RUN pip install fastf1

# Copy your Lambda function code (e.g., 'app.py') into the container
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

# And change this line:
# CMD [ "app.lambda_handler" ]
CMD [ "lambda_function.lambda_handler" ]

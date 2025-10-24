# Dockerfile
# Start from the official AWS Lambda Python image
FROM public.ecr.aws/lambda/python:3.10  

# Install the required libraries
# MODIFIED LINE: Added 'supabase'
RUN pip install fastf1 pytz supabase

# Copy your Lambda function code 
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

# Tell Lambda what function to run
CMD [ "lambda_function.lambda_handler" ]
# Dockerfile
# Start from the official AWS Lambda Python image
FROM public.ecr.aws/lambda/python:3.10  

# Install the required libraries
# MODIFIED LINE: Added 'supabase'
RUN pip install fastf1 pytz supabase


# System deps needed to build scientific/plotting libs when wheels aren't available (e.g., on arm64)
# RUN yum -y update \
# 	&& yum -y install \
# 		gcc \
# 		gcc-c++ \
# 		make \
# 		libpng-devel \
# 		freetype-devel \
# 		pkgconfig \
# 	&& yum clean all \
# 	&& rm -rf /var/cache/yum

# # Upgrade pip toolchain for better wheel support and then install Python deps
# RUN pip install --upgrade pip setuptools wheel \
# 	&& pip install fastf1 pytz supabase

# Copy your Lambda function code 
COPY lambda_function.py ${LAMBDA_TASK_ROOT}/

# Tell Lambda what function to run
CMD [ "lambda_function.lambda_handler" ]
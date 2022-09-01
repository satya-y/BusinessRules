FROM apache_base

# Sets the working directory for following COPY and CMD instructions
# Notice we haven’t created a directory by this name - this
# instruction creates a directory with this name if it doesn’t exist
WORKDIR /app

# Copy rest of the files
COPY . /app/

# Run app.py when the container launches
CMD ["python", "-u", "__init__.py"]

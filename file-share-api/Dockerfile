# Use an official Rust image as a build environment
FROM rust:1.73 as builder

# Create a new directory for the application
WORKDIR /usr/src/app

# Copy the source code into the container
COPY . .

# Build the application
RUN cargo build --release

# Use a smaller base image for the final runtime
FROM debian:bullseye-slim

# Install necessary runtime dependencies
RUN apt-get update && \
    apt-get install -y libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the built application from the builder stage
COPY --from=builder /usr/src/app/target/release/file-share-api /usr/local/bin/file-share-api

# Set the environment variables
ENV DATABASE_URL=postgres://yourusername:yourpassword@postgres:5432/yourdatabase

# Run the application
CMD ["file-share-api"]

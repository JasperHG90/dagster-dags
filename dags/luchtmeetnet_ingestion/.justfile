set dotenv-load

alias i := install
alias d := dev
alias c := clean

# Install poetry dependencies
install:
    poetry install

# Run local dagster service
dev:
    poetry run invoke dagster-dev

# Remove .dagster folder
clean:
    rm -rf .dagster

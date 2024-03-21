set dotenv-load

alias i := install
alias d := dev

# Install poetry dependencies
install:
    poetry install

# Run local dagster service
dev:
    poetry run invoke dagster-dev

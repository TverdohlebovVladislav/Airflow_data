#!/bin/bash

psql -h "${DB_HOST}" -U "${POSTGRES_USER}" -p "${POSTGRES_PASSWORD}" -c "create schema raw if not exists"
psql -h "${DB_HOST}" -U "${POSTGRES_USER}" -p "${POSTGRES_PASSWORD}" -c "create schema datamart if not exists"